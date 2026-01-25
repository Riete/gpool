package gpool

import (
	"errors"
	"sync"
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
	idlePool = &sync.Pool{New: func() any {
		return make(chan struct{}, limit)
	}}
	p.idlePool[limit] = idlePool
	return idlePool
}

type TaskPool[T any] struct {
	limiter *limiter
	task    chan *Task[T]
	stop    chan struct{}
	stopped bool
	mu      sync.Mutex
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
	task.f(param)
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
		t.task <- task
	}
	return wg, nil
}

func NewTaskPool[T any](capacity int) *TaskPool[T] {
	p := &TaskPool[T]{
		limiter: newLimiter(capacity),
		task:    make(chan *Task[T], 64),
		stop:    make(chan struct{}),
	}
	go p.start()
	return p
}
