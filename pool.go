package gpool

import (
	"errors"
	"sync"
	"sync/atomic"
)

var poolProvider = &pool{idlePool: make(map[int]*sync.Pool)}
var PoolStoppedError = errors.New("pool has been stopped")

type pool struct {
	mu       sync.RWMutex
	idlePool map[int]*sync.Pool
}

func (p *pool) get(limit int) *sync.Pool {
	p.mu.RLock()
	idlePool, exists := p.idlePool[limit]
	p.mu.RUnlock()
	if exists {
		return idlePool
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	idlePool, exists = p.idlePool[limit]
	if exists {
		return idlePool
	}
	idlePool = &sync.Pool{New: func() any {
		return make(chan struct{}, limit)
	}}
	p.idlePool[limit] = idlePool
	return idlePool
}

type TaskPool[T any] struct {
	limiter   *limiter
	task      chan *Task[T]
	stop      chan struct{}
	stopped   bool
	running   *atomic.Int64
	pending   *atomic.Int64
	completed *atomic.Int64
	total     *atomic.Int64
	mu        sync.Mutex
}

func (t *TaskPool[T]) Capacity() int {
	return t.limiter.capacity()
}

func (t *TaskPool[T]) SetCapacity(max int) {
	t.limiter.setCapacity(max)
}

func (t *TaskPool[T]) start() {
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
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.stopped {
		t.stopped = true
		close(t.stop)
	}
}

func (t *TaskPool[T]) dispatch(task *Task[T]) {
	if task.limit > 0 {
		t.limitedRun(task)
	} else {
		t.unlimitedRun(task)
	}
}

func (t *TaskPool[T]) run(task *Task[T], param T) {
	defer func() {
		if err := recover(); err != nil && task.onPanic != nil {
			task.onPanic(param, err)
		}
	}()
	t.pending.Add(-1)
	t.running.Add(1)
	task.f(param)
	t.running.Add(-1)
	t.completed.Add(1)
}

// limitedRun concurrency mode, the maximum concurrency is min(Task.limit, Capacity)
func (t *TaskPool[T]) limitedRun(task *Task[T]) {
	wg := new(sync.WaitGroup)
	limit := min(task.limit, t.Capacity())
	idlePool := poolProvider.get(limit)
	idle := idlePool.Get().(chan struct{})
	defer func() {
		wg.Wait()
		task.done()
		for len(idle) > 0 {
			<-idle
		}
		idlePool.Put(idle)
	}()

	for range limit {
		idle <- struct{}{}
	}
	for _, param := range task.params {
		<-idle
		select {
		case <-task.ctx.Done():
			return
		case <-t.limiter.wait():
			wg.Go(func() {
				t.run(task, param)
				idle <- struct{}{}
			})
		}
	}
}

func (t *TaskPool[T]) PendingCount() int64 {
	return t.pending.Load()
}

func (t *TaskPool[T]) RunningCount() int64 {
	return t.running.Load()
}

func (t *TaskPool[T]) CompletedCount() int64 {
	return t.completed.Load()
}

func (t *TaskPool[T]) ResetCompletedCount() {
	t.completed.Store(0)
}

// unlimitedRun qps mode, the maximum qps is Capacity
func (t *TaskPool[T]) unlimitedRun(task *Task[T]) {
	wg := new(sync.WaitGroup)
	defer func() {
		wg.Wait()
		task.done()
	}()
	for _, param := range task.params {
		select {
		case <-task.ctx.Done():
			return
		case <-t.limiter.wait():
			wg.Go(func() {
				t.run(task, param)
			})
		}
	}
}

func (t *TaskPool[T]) Submit(tasks ...*Task[T]) (*sync.WaitGroup, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return nil, PoolStoppedError
	}
	wg := new(sync.WaitGroup)
	wg.Add(len(tasks))
	for _, task := range tasks {
		task.wg = wg
		t.pending.Add(int64(len(task.params)))
		t.task <- task
	}
	return wg, nil
}

func NewTaskPool[T any](capacity int) *TaskPool[T] {
	p := &TaskPool[T]{
		limiter:   newLimiter(capacity),
		task:      make(chan *Task[T], 64),
		stop:      make(chan struct{}),
		running:   new(atomic.Int64),
		pending:   new(atomic.Int64),
		completed: new(atomic.Int64),
	}
	go p.start()
	return p
}
