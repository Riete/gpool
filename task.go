package gpool

import (
	"context"
	"sync"
)

// Task runtime concurrency: min(concurrency, TaskPool.Capacity) if concurrency > 0, else TaskPool.Capacity
// On task panic, Task.recover is preferred over default recover(print panic message and goroutine stack trace)
// Use TaskBuilder to build task
type Task[T any] struct {
	ctx         context.Context
	taskFunc    func(T)
	param       []T
	concurrency int
	recover     func(T, any)
	wg          *sync.WaitGroup
}

func (t *Task[T]) done() {
	t.wg.Done()
}

type TaskBuilder[T any] struct {
	ctx         context.Context
	taskFunc    func(T)
	concurrency int
	recover     func(T, any)
}

func (t *TaskBuilder[T]) WithContext(ctx context.Context) *TaskBuilder[T] {
	t.ctx = ctx
	return t
}

func (t *TaskBuilder[T]) WithConcurrency(concurrency int) *TaskBuilder[T] {
	t.concurrency = concurrency
	return t
}

func (t *TaskBuilder[T]) WithRecover(recover func(T, any)) *TaskBuilder[T] {
	t.recover = recover
	return t
}

func (t *TaskBuilder[T]) WitTaskFunc(f func(T)) *TaskBuilder[T] {
	t.taskFunc = f
	return t
}

func (t *TaskBuilder[T]) Build(params ...[]T) []*Task[T] {
	tasks := make([]*Task[T], 0, len(params))
	for _, param := range params {
		tasks = append(tasks,
			&Task[T]{
				ctx:         t.ctx,
				taskFunc:    t.taskFunc,
				param:       param,
				concurrency: t.concurrency,
				recover:     t.recover,
			},
		)
	}
	return tasks
}

func NewTaskBuilder[T any]() *TaskBuilder[T] {
	return &TaskBuilder[T]{ctx: context.Background()}
}
