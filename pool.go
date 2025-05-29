package gpool

import (
	"context"
	"sync"
)

// Task
// limit is the maximum concurrency it should not exceed the Pool.Capacity
// runtime value will be set to min(limit, Pool.Capacity), zero means no limit
type Task[T any] struct {
	ctx     context.Context
	f       func(T)
	params  []T
	limit   int
	onPanic func(T, any)
	wg      *sync.WaitGroup
}

func (t *Task[T]) done() {
	t.wg.Done()
}

func NewTask[T any](f func(T), params []T, limit int, onPanic func(T, any)) *Task[T] {
	return NewTaskContext[T](context.Background(), f, params, limit, onPanic)
}

func NewTaskContext[T any](ctx context.Context, f func(T), params []T, limit int, onPanic func(T, any)) *Task[T] {
	return &Task[T]{ctx: ctx, f: f, params: params, limit: limit, onPanic: onPanic}
}

type Pool[T any] struct {
	limiter *limiter
	task    chan *Task[T]
	stop    chan struct{}
	once    sync.Once
}

func (p *Pool[T]) Capacity() int {
	return p.limiter.capacity()
}

func (p *Pool[T]) SetCapacity(max int) {
	p.limiter.setCapacity(max)
}

func (p *Pool[T]) start() {
	for {
		select {
		case <-p.stop:
			return
		case task := <-p.task:
			go p.dispatch(task)
		}
	}
}

func (p *Pool[T]) Stop() {
	p.once.Do(func() {
		close(p.stop)
	})
}

func (p *Pool[T]) dispatch(task *Task[T]) {
	if task.limit > 0 {
		p.limitedRun(task)
	} else {
		p.unlimitedRun(task)
	}
}

func (p *Pool[T]) run(wg *sync.WaitGroup, idle chan struct{}, task *Task[T], param T) {
	defer func() {
		wg.Done()
		if task.limit > 0 {
			idle <- struct{}{}
		}
		if err := recover(); err != nil && task.onPanic != nil {
			task.onPanic(param, err)
		}
	}()
	task.f(param)
}

// limitedRun concurrency mode, the maximum concurrency is min(Task.limit, Capacity)
func (p *Pool[T]) limitedRun(task *Task[T]) {
	defer task.done()
	wg := new(sync.WaitGroup)
	limit := min(task.limit, p.Capacity())
	idle := make(chan struct{}, limit)
	defer close(idle)
	for i := 0; i < limit; i++ {
		idle <- struct{}{}
	}
	for _, param := range task.params {
		<-idle
		select {
		case <-task.ctx.Done():
			return
		case <-p.limiter.wait():
			wg.Add(1)
			go p.run(wg, idle, task, param)
		}
	}
	wg.Wait()
}

// unlimitedRun qps mode, the maximum qps is Capacity
func (p *Pool[T]) unlimitedRun(task *Task[T]) {
	defer task.done()
	wg := new(sync.WaitGroup)
	for _, param := range task.params {
		select {
		case <-task.ctx.Done():
			return
		case <-p.limiter.wait():
			wg.Add(1)
			go p.run(wg, nil, task, param)
		}
	}
	wg.Wait()
}

func (p *Pool[T]) Submit(tasks ...*Task[T]) *sync.WaitGroup {
	wg := new(sync.WaitGroup)
	wg.Add(len(tasks))
	for _, task := range tasks {
		task.wg = wg
		p.task <- task
	}
	return wg
}

func NewPool[T any](capacity int) *Pool[T] {
	p := &Pool[T]{
		limiter: newLimiter(capacity),
		task:    make(chan *Task[T]),
		stop:    make(chan struct{}),
	}
	go p.start()
	return p
}
