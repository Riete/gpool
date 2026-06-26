package gpool

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

type Mode int64

const ConcurrentMode Mode = 0
const RateLimitMode Mode = 1

var emptyFuture = new(Future)

type Future struct {
	wg          *sync.WaitGroup
	cancelFuncs []context.CancelFunc
}

func (f *Future) Wait() {
	if f.wg != nil {
		f.wg.Wait()
	}
}

func (f *Future) Cancel() {
	for _, cancel := range f.cancelFuncs {
		cancel()
	}
}

type Counter struct {
	running   *atomic.Int64
	pending   *atomic.Int64
	completed *atomic.Int64
}

func (c *Counter) Running() int64 {
	return c.running.Load()
}

func (c *Counter) Pending() int64 {
	return c.pending.Load()
}

func (c *Counter) Completed() int64 {
	return c.completed.Load()
}

func (c *Counter) Reset() {
	c.running.Store(0)
	c.pending.Store(0)
	c.completed.Store(0)
}

type Pool[T any] struct {
	mode        Mode
	limiter     *RateLimiter
	task        chan *Task[T]
	idle        *semaphore.Weighted
	counter     *Counter
	stopped     bool
	runningTask *atomic.Int64
	mu          sync.Mutex
}

func (p *Pool[T]) start() {
	p.limiter.Start()
	for task := range p.task {
		go p.dispatch(task)
	}
}

func (p *Pool[T]) dispatch(task *Task[T]) {
	p.runningTask.Add(1)
	defer p.runningTask.Add(-1)
	if task.maxConcurrency > 0 {
		p.limitedRun(task)
	} else {
		p.unlimitedRun(task)
	}
}

func (p *Pool[T]) limitedRun(task *Task[T]) {
	if p.mode == RateLimitMode {
		p.limitedRateLimitRun(task)
	} else {
		p.limitedConcurrentRun(task)
	}
}

func (p *Pool[T]) unlimitedRun(task *Task[T]) {
	if p.mode == RateLimitMode {
		p.unlimitedRateLimitRun(task)
	} else {
		p.unlimitedConcurrentRun(task)
	}
}

func (p *Pool[T]) run(ctx context.Context, f func(context.Context, T), param T, onPanic func(T, any)) {
	defer func() {
		if err := recover(); err != nil {
			if onPanic != nil {
				onPanic(param, err)
			} else {
				// default recover
				buf := make([]byte, 64<<10)
				buf = buf[:runtime.Stack(buf, false)]
				fmt.Println("panic:", err, "\n"+string(buf))
			}
		}
	}()
	p.counter.pending.Add(-1)
	p.counter.running.Add(1)
	f(ctx, param)
	p.counter.running.Add(-1)
	p.counter.completed.Add(1)
}

// limitedRateLimitRun maximum qps is min(Task.maxConcurrency, Pool.limiter.capacity)
func (p *Pool[T]) limitedRateLimitRun(task *Task[T]) {
	wg := new(sync.WaitGroup)
	taskLimiter := limiterProvider.Get().(*RateLimiter)
	taskLimiter.SetCapacity(min(task.maxConcurrency, p.limiter.Capacity()))
	taskLimiter.Start()
	defer func() {
		wg.Wait()
		task.done()
		taskLimiter.Stop()
		limiterProvider.Put(taskLimiter)
	}()

	for _, param := range task.param {
		<-taskLimiter.Wait()
		select {
		case <-task.ctx.Done():
			return
		case <-p.limiter.Wait():
			wg.Go(func() {
				p.run(task.ctx, task.taskFunc, param, task.recover)
			})
		}
	}
}

// limitedConcurrentRun maximum concurrency is min(Task.maxConcurrency, Pool.limiter.capacity)
func (p *Pool[T]) limitedConcurrentRun(task *Task[T]) {
	wg := new(sync.WaitGroup)
	idle := semaphore.NewWeighted(int64(min(task.maxConcurrency, p.limiter.Capacity())))
	defer func() {
		wg.Wait()
		task.done()
	}()

	for _, param := range task.param {
		if err := idle.Acquire(task.ctx, 1); err != nil {
			return
		}
		if err := p.idle.Acquire(task.ctx, 1); err != nil {
			return
		}
		select {
		case <-task.ctx.Done():
			p.idle.Release(1)
			return
		case <-p.limiter.Wait():
			wg.Go(func() {
				p.run(task.ctx, task.taskFunc, param, task.recover)
				p.idle.Release(1)
				idle.Release(1)
			})
		}
	}
}

// unlimitedRateLimitRun maximum qps is Pool.limiter.capacity
func (p *Pool[T]) unlimitedRateLimitRun(task *Task[T]) {
	wg := new(sync.WaitGroup)
	defer func() {
		wg.Wait()
		task.done()
	}()

	for _, param := range task.param {
		select {
		case <-task.ctx.Done():
			return
		case <-p.limiter.Wait():
			wg.Go(func() {
				p.run(task.ctx, task.taskFunc, param, task.recover)
			})
		}
	}
}

// unlimitedConcurrentRun maximum concurrency is Pool.limiter.capacity
func (p *Pool[T]) unlimitedConcurrentRun(task *Task[T]) {
	wg := new(sync.WaitGroup)
	defer func() {
		wg.Wait()
		task.done()
	}()

	for _, param := range task.param {
		if err := p.idle.Acquire(task.ctx, 1); err != nil {
			return
		}
		select {
		case <-task.ctx.Done():
			p.idle.Release(1)
			return
		case <-p.limiter.Wait():
			wg.Go(func() {
				p.run(task.ctx, task.taskFunc, param, task.recover)
				p.idle.Release(1)
			})
		}
	}
}

func (p *Pool[T]) Counter() *Counter {
	return p.counter
}

func (p *Pool[T]) Submit(tasks ...*Task[T]) *Future {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return emptyFuture
	}
	wg := new(sync.WaitGroup)
	wg.Add(len(tasks))
	cancelFuncs := make([]context.CancelFunc, 0, len(tasks))
	for _, task := range tasks {
		var cancel context.CancelFunc
		task.wg = wg
		task.ctx, cancel = context.WithCancel(task.ctx)
		cancelFuncs = append(cancelFuncs, cancel)
		p.counter.pending.Add(int64(len(task.param)))
		p.task <- task
	}
	return &Future{wg: wg, cancelFuncs: cancelFuncs}
}

// Stop no new tasks can be submitted after stop, all running tasks will wait to be completed
func (p *Pool[T]) Stop() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	close(p.task)
	p.mu.Unlock()
	for {
		if p.runningTask.Load() == 0 {
			p.limiter.Stop()
			return
		}
		time.Sleep(time.Second)
	}
}

func (p *Pool[T]) Wait(futures ...*Future) {
	for _, future := range futures {
		future.Wait()
	}
}

func (p *Pool[T]) Pause() {
	p.limiter.Pause()
}

func (p *Pool[T]) Resume() {
	p.limiter.Resume()
}

func (p *Pool[T]) IsPaused() bool {
	return p.limiter.IsPaused()
}

func (p *Pool[T]) IsStopped() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stopped
}

func NewPool[T any](capacity int, mode Mode) *Pool[T] {
	p := &Pool[T]{
		mode:    mode,
		limiter: NewRateLimiter(capacity),
		task:    make(chan *Task[T], 64),
		counter: &Counter{
			running:   new(atomic.Int64),
			pending:   new(atomic.Int64),
			completed: new(atomic.Int64),
		},
		runningTask: new(atomic.Int64),
	}
	if mode == ConcurrentMode {
		p.idle = semaphore.NewWeighted(int64(capacity))
	}
	go p.start()
	return p
}

func NewConcurrentPool[T any](capacity int) *Pool[T] {
	return NewPool[T](capacity, ConcurrentMode)
}

func NewRateLimitPool[T any](capacity int) *Pool[T] {
	return NewPool[T](capacity, RateLimitMode)
}
