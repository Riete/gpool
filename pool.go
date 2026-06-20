package gpool

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var poolProvider = &pool{idlePool: make(map[int]*sync.Pool), idleRateLimiter: make(map[int]*sync.Pool)}

type Mode int64

const ConcurrentMode Mode = 0
const RateLimiterMode Mode = 1

type pool struct {
	mu              sync.RWMutex
	idlePool        map[int]*sync.Pool
	idleRateLimiter map[int]*sync.Pool
}

func (p *pool) getPool(pools map[int]*sync.Pool, limit int, f func() any) *sync.Pool {
	p.mu.RLock()
	idle, exists := pools[limit]
	p.mu.RUnlock()
	if exists {
		return idle
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	idle, exists = pools[limit]
	if exists {
		return idle
	}
	idle = &sync.Pool{New: f}
	pools[limit] = idle
	return idle
}

func (p *pool) getIdlePool(limit int) *sync.Pool {
	return p.getPool(p.idlePool, limit, func() any {
		return make(chan struct{}, limit)
	})
}

func (p *pool) getIdleRateLimiter(limit int) *sync.Pool {
	return p.getPool(p.idleRateLimiter, limit, func() any {
		return NewRateLimiter(limit)
	})
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

type TaskPool[T any] struct {
	mode    Mode
	limiter *RateLimiter
	task    chan *Task[T]
	stop    chan struct{}
	worker  chan struct{}
	counter *Counter
	stopped *atomic.Bool
}

func (t *TaskPool[T]) start() {
	go t.limiter.Start()
	for {
		select {
		case <-t.stop:
			return
		case task := <-t.task:
			go t.dispatch(task)
		}
	}
}

// Stop does not affect the submitted tasks, but no new tasks can be submitted
func (t *TaskPool[T]) Stop() {
	if t.stopped.CompareAndSwap(false, true) {
		close(t.stop)
		go func() {
			var once sync.Once
			for {
				if t.counter.Pending() == 0 {
					once.Do(func() {
						t.limiter.Stop()
					})
				} else {
					time.Sleep(time.Second)
				}
			}
		}()
	}
}

func (t *TaskPool[T]) dispatch(task *Task[T]) {
	if task.concurrency > 0 {
		t.limitedRun(task)
	} else {
		t.unlimitedRun(task)
	}
}

func (t *TaskPool[T]) run(f func(T), param T, onPanic func(T, any)) {
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
	if t.mode == ConcurrentMode {
		<-t.worker
		defer func() {
			t.worker <- struct{}{}
		}()
	}
	t.counter.pending.Add(-1)
	t.counter.running.Add(1)
	f(param)
	t.counter.running.Add(-1)
	t.counter.completed.Add(1)
}

// limitedRun
// in ConcurrentMode Task maximum concurrency is min(Task.concurrency, Capacity)
// in RateLimiterMode Task maximum qps is min(Task.concurrency, Capacity)
func (t *TaskPool[T]) limitedRun(task *Task[T]) {
	wg := new(sync.WaitGroup)
	limit := min(task.concurrency, t.limiter.Capacity())
	idlePool := poolProvider.getIdlePool(limit)
	idle := idlePool.Get().(chan struct{})
	defer func() {
		wg.Wait()
		task.done()
		for len(idle) > 0 {
			<-idle
		}
		idlePool.Put(idle)
	}()

	if t.mode == RateLimiterMode {
		rateLimiterPool := poolProvider.getIdleRateLimiter(limit)
		rateLimiter := rateLimiterPool.Get().(*RateLimiter)
		defer rateLimiterPool.Put(rateLimiter)
		defer rateLimiter.Stop()
		stop := make(chan struct{})
		defer close(stop)
		go rateLimiter.Start()
		go func() {
			for {
				select {
				case <-rateLimiter.Wait():
					idle <- struct{}{}
				case <-stop:
					return
				}
			}
		}()
	} else {
		for range limit {
			idle <- struct{}{}
		}
	}

	for _, param := range task.param {
		<-idle
		select {
		case <-task.ctx.Done():
			return
		case <-t.limiter.Wait():
			wg.Go(func() {
				t.run(task.taskFunc, param, task.recover)
				if t.mode == ConcurrentMode {
					idle <- struct{}{}
				}
			})
		}
	}
}

func (t *TaskPool[T]) Counter() *Counter {
	return t.counter
}

// unlimitedRun
// in ConcurrentMode Task maximum concurrency Capacity
// in RateLimiterMode Task maximum qps is Capacity
func (t *TaskPool[T]) unlimitedRun(task *Task[T]) {
	wg := new(sync.WaitGroup)
	defer func() {
		wg.Wait()
		task.done()
	}()

	for _, param := range task.param {
		select {
		case <-task.ctx.Done():
			return
		case <-t.limiter.Wait():
			wg.Go(func() {
				t.run(task.taskFunc, param, task.recover)
			})
		}
	}
}

func (t *TaskPool[T]) Submit(tasks ...*Task[T]) *Future {
	if t.stopped.Load() {
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
		t.counter.pending.Add(int64(len(task.param)))
		t.task <- task
	}
	return &Future{wg: wg, cancelFuncs: cancelFuncs}
}

func (t *TaskPool[T]) Wait(futures ...*Future) {
	for _, future := range futures {
		future.Wait()
	}
}

func NewTaskPool[T any](capacity int, mode Mode) *TaskPool[T] {
	p := &TaskPool[T]{
		mode:    mode,
		limiter: NewRateLimiter(capacity),
		task:    make(chan *Task[T], 64),
		stop:    make(chan struct{}),
		counter: &Counter{
			running:   new(atomic.Int64),
			pending:   new(atomic.Int64),
			completed: new(atomic.Int64),
		},
		stopped: new(atomic.Bool),
	}
	if mode == ConcurrentMode {
		p.worker = make(chan struct{}, capacity)
		for range capacity {
			p.worker <- struct{}{}
		}
	}
	go p.start()
	return p
}

func NewConcurrentTaskPool[T any](capacity int) *TaskPool[T] {
	return NewTaskPool[T](capacity, ConcurrentMode)
}

func NewRateLimiterTaskPool[T any](capacity int) *TaskPool[T] {
	return NewTaskPool[T](capacity, RateLimiterMode)
}
