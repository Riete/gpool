package gpool

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Mode int64

const ConcurrentMode Mode = 0
const RateLimitMode Mode = 1

var syncPoolProvider = &syncPool{
	chanPool: make(map[int]*sync.Pool),
	rateLimiterPool: &sync.Pool{
		New: func() any {
			return NewRateLimiter(0)
		}},
}

type syncPool struct {
	mu              sync.RWMutex
	chanPool        map[int]*sync.Pool
	rateLimiterPool *sync.Pool
}

func (s *syncPool) getChanPool(capacity int) *sync.Pool {
	s.mu.RLock()
	cp, exists := s.chanPool[capacity]
	s.mu.RUnlock()
	if exists {
		return cp
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp, exists = s.chanPool[capacity]
	if exists {
		return cp
	}
	cp = &sync.Pool{New: func() any {
		return make(chan struct{}, capacity)
	}}
	s.chanPool[capacity] = cp
	return cp
}

func (s *syncPool) getChan(capacity int) (*sync.Pool, chan struct{}) {
	cp := s.getChanPool(capacity)
	return cp, cp.Get().(chan struct{})
}

func (s *syncPool) putChan(cp *sync.Pool, ch chan struct{}) {
	cp.Put(ch)
}

func (s *syncPool) getRateLimiter(capacity int) *RateLimiter {
	limiter := s.rateLimiterPool.Get().(*RateLimiter)
	limiter.SetCapacity(capacity)
	return limiter
}
func (s *syncPool) putRateLimiter(limiter *RateLimiter) {
	s.rateLimiterPool.Put(limiter)
}

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
	stop        chan struct{}
	idle        chan struct{}
	counter     *Counter
	stopped     *atomic.Bool
	runningTask *atomic.Int64
}

func (p *Pool[T]) start() {
	p.limiter.Start()
	for {
		select {
		case <-p.stop:
			return
		case task := <-p.task:
			go p.dispatch(task)
		}
	}
}

// Stop no new tasks can be submitted after stop, all running tasks will wait to be completed
func (p *Pool[T]) Stop() {
	if p.stopped.CompareAndSwap(false, true) {
		close(p.stop)
		for {
			if p.runningTask.Load() == 0 {
				p.limiter.Stop()
				return
			}
			time.Sleep(time.Second)
		}
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

func (p *Pool[T]) run(f func(T), param T, onPanic func(T, any)) {
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
	// limit maximum concurrency in ConcurrentMode
	if p.mode == ConcurrentMode {
		<-p.idle
		defer func() {
			p.idle <- struct{}{}
		}()
	}
	p.counter.pending.Add(-1)
	p.counter.running.Add(1)
	f(param)
	p.counter.running.Add(-1)
	p.counter.completed.Add(1)
}

// limitedRun
// in ConcurrentMode Task maximum concurrency is min(Task.maxConcurrency, Pool.limiter.capacity)
// in RateLimitMode Task maximum qps is min(Task.maxConcurrency, Pool.limiter.capacity)
func (p *Pool[T]) limitedRun(task *Task[T]) {
	wg := new(sync.WaitGroup)
	capacity := min(task.maxConcurrency, p.limiter.Capacity())
	cp, idle := syncPoolProvider.getChan(capacity)
	defer func() {
		wg.Wait()
		task.done()
		for len(idle) > 0 {
			<-idle
		}
		syncPoolProvider.putChan(cp, idle)
	}()

	if p.mode == RateLimitMode {
		limiter := syncPoolProvider.getRateLimiter(capacity)
		limiter.Start()
		defer syncPoolProvider.putRateLimiter(limiter)
		defer limiter.Stop()
		go func() {
			for {
				select {
				case <-limiter.Wait():
					idle <- struct{}{}
				case <-limiter.Stopped():
					return
				}
			}
		}()
	} else {
		for range capacity {
			idle <- struct{}{}
		}
	}

	for _, param := range task.param {
		<-idle
		select {
		case <-task.ctx.Done():
			return
		case <-p.limiter.Wait():
			wg.Go(func() {
				p.run(task.taskFunc, param, task.recover)
				if p.mode == ConcurrentMode {
					idle <- struct{}{}
				}
			})
		}
	}
}

func (p *Pool[T]) Counter() *Counter {
	return p.counter
}

// unlimitedRun
// in ConcurrentMode Task maximum maxConcurrency is Pool.limiter.capacity
// in RateLimitMode Task maximum qps is Pool.limiter.capacity
func (p *Pool[T]) unlimitedRun(task *Task[T]) {
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
				p.run(task.taskFunc, param, task.recover)
			})
		}
	}
}

func (p *Pool[T]) Submit(tasks ...*Task[T]) *Future {
	if p.stopped.Load() {
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

func (p *Pool[T]) Wait(futures ...*Future) {
	for _, future := range futures {
		future.Wait()
	}
}

func NewPool[T any](capacity int, mode Mode) *Pool[T] {
	p := &Pool[T]{
		mode:    mode,
		limiter: NewRateLimiter(capacity),
		task:    make(chan *Task[T], 64),
		stop:    make(chan struct{}),
		counter: &Counter{
			running:   new(atomic.Int64),
			pending:   new(atomic.Int64),
			completed: new(atomic.Int64),
		},
		stopped:     new(atomic.Bool),
		runningTask: new(atomic.Int64),
	}
	if mode == ConcurrentMode {
		p.idle = make(chan struct{}, capacity)
		for range capacity {
			p.idle <- struct{}{}
		}
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
