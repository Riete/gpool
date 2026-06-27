package conrate

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

var ErrExecutorStopped = errors.New("executor stopped")

type ExecutorMode int64

const ConcurrencyMode ExecutorMode = 0
const RateLimitMode ExecutorMode = 1

type Future struct {
	wg          *sync.WaitGroup
	cancelFuncs []context.CancelFunc
	err         error
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

func (f *Future) Error() error {
	return f.err
}

type Counter struct {
	running   *atomic.Int64
	pending   *atomic.Int64
	completed *atomic.Int64
	canceled  *atomic.Int64
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

func (c *Counter) Canceled() int64 {
	return c.canceled.Load()
}

func (c *Counter) Reset() {
	c.running.Store(0)
	c.pending.Store(0)
	c.completed.Store(0)
	c.canceled.Store(0)
}

type Executor[T any] struct {
	mode        ExecutorMode
	limiter     *RateLimiter
	task        chan *Task[T]
	idle        *semaphore.Weighted
	counter     *Counter
	stopped     bool
	stop        chan struct{}
	runningTask *atomic.Int64
	mu          sync.Mutex
}

func (e *Executor[T]) start() {
	for task := range e.task {
		go e.dispatch(task)
	}
}

func (e *Executor[T]) dispatch(task *Task[T]) {
	e.runningTask.Add(1)
	defer e.runningTask.Add(-1)
	if task.maxConcurrency > 0 {
		e.limitedRun(task)
	} else {
		e.unlimitedRun(task)
	}
}

func (e *Executor[T]) limitedRun(task *Task[T]) {
	if e.mode == RateLimitMode {
		e.limitedRateLimitRun(task)
	} else {
		e.limitedConcurrentRun(task)
	}
}

func (e *Executor[T]) unlimitedRun(task *Task[T]) {
	if e.mode == RateLimitMode {
		e.unlimitedRateLimitRun(task)
	} else {
		e.unlimitedConcurrentRun(task)
	}
}

func (e *Executor[T]) run(ctx context.Context, f func(context.Context, T), param T, onPanic func(T, any)) {
	e.counter.pending.Add(-1)
	e.counter.running.Add(1)
	defer func() {
		e.counter.running.Add(-1)
		e.counter.completed.Add(1)
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
	f(ctx, param)
}

// limitedRateLimitRun maximum qps is min(Task.maxConcurrency, Executor.limiter.capacity)
func (e *Executor[T]) limitedRateLimitRun(task *Task[T]) {
	wg := new(sync.WaitGroup)
	taskLimiter := NewRateLimiter(min(task.maxConcurrency, e.limiter.Capacity()))
	canceled := new(atomic.Int64)
	canceled.Store(int64(len(task.param)))
	defer func() {
		e.counter.canceled.Add(canceled.Load())
		e.counter.pending.Add(-canceled.Load())
	}()
	defer taskLimiter.Stop()
	defer task.done()
	defer wg.Wait()

	for _, param := range task.param {
		select {
		case <-taskLimiter.Wait():
		case <-e.stop:
			return
		case <-task.ctx.Done():
			return
		}
		select {
		case <-e.stop:
			return
		case <-task.ctx.Done():
			return
		case <-e.limiter.Wait():
			wg.Go(func() {
				canceled.Add(-1)
				e.run(task.ctx, task.taskFunc, param, task.recover)
			})
		}
	}
}

// limitedConcurrentRun maximum concurrency is min(Task.maxConcurrency, Executor.limiter.capacity)
func (e *Executor[T]) limitedConcurrentRun(task *Task[T]) {
	wg := new(sync.WaitGroup)
	idle := semaphore.NewWeighted(int64(min(task.maxConcurrency, e.limiter.Capacity())))
	canceled := new(atomic.Int64)
	canceled.Store(int64(len(task.param)))
	defer func() {
		e.counter.canceled.Add(canceled.Load())
		e.counter.pending.Add(-canceled.Load())
	}()
	defer task.done()
	defer wg.Wait()

	for _, param := range task.param {
		if err := idle.Acquire(task.ctx, 1); err != nil {
			return
		}
		if err := e.idle.Acquire(task.ctx, 1); err != nil {
			idle.Release(1)
			return
		}
		select {
		case <-e.stop:
			idle.Release(1)
			e.idle.Release(1)
			return
		case <-task.ctx.Done():
			idle.Release(1)
			e.idle.Release(1)
			return
		case <-e.limiter.Wait():
			wg.Go(func() {
				canceled.Add(-1)
				e.run(task.ctx, task.taskFunc, param, task.recover)
				e.idle.Release(1)
				idle.Release(1)
			})
		}
	}
}

// unlimitedRateLimitRun maximum qps is Executor.limiter.capacity
func (e *Executor[T]) unlimitedRateLimitRun(task *Task[T]) {
	wg := new(sync.WaitGroup)
	canceled := new(atomic.Int64)
	canceled.Store(int64(len(task.param)))
	defer func() {
		e.counter.canceled.Add(canceled.Load())
		e.counter.pending.Add(-canceled.Load())
	}()
	defer task.done()
	defer wg.Wait()

	for _, param := range task.param {
		select {
		case <-e.stop:
			return
		case <-task.ctx.Done():
			return
		case <-e.limiter.Wait():
			wg.Go(func() {
				canceled.Add(-1)
				e.run(task.ctx, task.taskFunc, param, task.recover)
			})
		}
	}
}

// unlimitedConcurrentRun maximum concurrency is Executor.limiter.capacity
func (e *Executor[T]) unlimitedConcurrentRun(task *Task[T]) {
	wg := new(sync.WaitGroup)
	canceled := new(atomic.Int64)
	canceled.Store(int64(len(task.param)))
	defer func() {
		e.counter.canceled.Add(canceled.Load())
		e.counter.pending.Add(-canceled.Load())
	}()
	defer task.done()
	defer wg.Wait()

	for _, param := range task.param {
		if err := e.idle.Acquire(task.ctx, 1); err != nil {
			return
		}
		select {
		case <-e.stop:
			e.idle.Release(1)
			return
		case <-task.ctx.Done():
			e.idle.Release(1)
			return
		case <-e.limiter.Wait():
			wg.Go(func() {
				canceled.Add(-1)
				e.run(task.ctx, task.taskFunc, param, task.recover)
				e.idle.Release(1)
			})
		}
	}
}

func (e *Executor[T]) Counter() *Counter {
	return e.counter
}

func (e *Executor[T]) Submit(tasks ...*Task[T]) *Future {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return &Future{err: ErrExecutorStopped}
	}
	wg := new(sync.WaitGroup)
	wg.Add(len(tasks))
	cancelFuncs := make([]context.CancelFunc, 0, len(tasks))
	for _, task := range tasks {
		var cancel context.CancelFunc
		task.wg = wg
		task.ctx, cancel = context.WithCancel(task.ctx)
		cancelFuncs = append(cancelFuncs, cancel)
		e.counter.pending.Add(int64(len(task.param)))
		e.task <- task
	}
	return &Future{wg: wg, cancelFuncs: cancelFuncs}
}

// GracefulStop no new tasks can be submitted after stop, all running tasks will wait to be completed until timeout
func (e *Executor[T]) GracefulStop(timeout time.Duration) {
	e.mu.Lock()
	if e.stopped {
		e.mu.Unlock()
		return
	}
	e.stopped = true
	close(e.task)
	e.mu.Unlock()
	defer e.limiter.Stop()
	defer close(e.stop)
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) || e.runningTask.Load() == 0 {
			return
		}
		time.Sleep(time.Second)
	}
}

// Stop immediately stop
func (e *Executor[T]) Stop() {
	e.mu.Lock()
	if e.stopped {
		e.mu.Unlock()
		return
	}
	defer e.mu.Unlock()
	e.stopped = true
	close(e.task)
	close(e.stop)
	e.limiter.Stop()
}

func (e *Executor[T]) Wait(futures ...*Future) {
	for _, future := range futures {
		future.Wait()
	}
}

func (e *Executor[T]) Pause() {
	e.limiter.Pause()
}

func (e *Executor[T]) Resume() {
	e.limiter.Resume()
}

func (e *Executor[T]) IsPaused() bool {
	return e.limiter.IsPaused()
}

func (e *Executor[T]) IsStopped() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stopped
}

func NewExecutor[T any](capacity int, mode ExecutorMode) *Executor[T] {
	p := &Executor[T]{
		mode:    mode,
		limiter: NewRateLimiter(capacity),
		task:    make(chan *Task[T], 64),
		stop:    make(chan struct{}),
		counter: &Counter{
			running:   new(atomic.Int64),
			pending:   new(atomic.Int64),
			completed: new(atomic.Int64),
			canceled:  new(atomic.Int64),
		},
		runningTask: new(atomic.Int64),
	}
	if mode == ConcurrencyMode {
		p.idle = semaphore.NewWeighted(int64(capacity))
	}
	go p.start()
	return p
}

func NewConcurrentExecutor[T any](maxConcurrency int) *Executor[T] {
	return NewExecutor[T](maxConcurrency, ConcurrencyMode)
}

func NewRateLimitExecutor[T any](maxQPS int) *Executor[T] {
	return NewExecutor[T](maxQPS, RateLimitMode)
}
