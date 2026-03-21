package gpool

import (
	"context"
	"sync"
)

// Task limit is the maximum concurrency it should not exceed the TaskPool.Capacity
// runtime value will be set to min(limit, TaskPool.Capacity), zero means no limit
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

type TaskBuilder[T any] struct {
	ctx     context.Context
	limit   int
	onPanic func(T, any)
}

func (t *TaskBuilder[T]) WithContext(ctx context.Context) *TaskBuilder[T] {
	t.ctx = ctx
	return t
}

func (t *TaskBuilder[T]) WithLimit(limit int) *TaskBuilder[T] {
	t.limit = limit
	return t
}

func (t *TaskBuilder[T]) WithOnPanic(onPanic func(T, any)) *TaskBuilder[T] {
	t.onPanic = onPanic
	return t
}

func (t *TaskBuilder[T]) Build(f func(T), params []T) *Task[T] {
	return NewTaskContext[T](t.ctx, f, params, t.limit, t.onPanic)
}

func NewTaskBuilder[T any](limit int, onPanic func(T, any)) *TaskBuilder[T] {
	return NewTaskBuilderContext[T](context.Background(), limit, onPanic)
}

func NewTaskBuilderContext[T any](ctx context.Context, limit int, onPanic func(T, any)) *TaskBuilder[T] {
	return &TaskBuilder[T]{
		ctx:     ctx,
		limit:   limit,
		onPanic: onPanic,
	}
}
