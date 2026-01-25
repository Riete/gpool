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
