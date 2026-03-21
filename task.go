package gpool

import (
	"context"
	"sync"
)

type TaskFunc[T any] struct {
	Func    func(T)
	Params  []T
	OnPanic func(T, any)
}

// Task limit is the maximum concurrency it should not exceed the TaskPool.Capacity
// runtime value will be set to min(limit, TaskPool.Capacity), zero means no limit
// TaskFunc.OnPanic is preferred to Task.onPanic
type Task[T any] struct {
	ctx      context.Context
	taskFunc *TaskFunc[T]
	limit    int
	onPanic  func(T, any)
	wg       *sync.WaitGroup
}

func (t *Task[T]) done() {
	t.wg.Done()
}

func NewTask[T any](taskFunc *TaskFunc[T], limit int, onPanic func(T, any)) *Task[T] {
	return NewTaskContext[T](context.Background(), taskFunc, limit, onPanic)
}

func NewTaskContext[T any](ctx context.Context, taskFunc *TaskFunc[T], limit int, onPanic func(T, any)) *Task[T] {
	return &Task[T]{ctx: ctx, taskFunc: taskFunc, limit: limit, onPanic: onPanic}
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

func (t *TaskBuilder[T]) BuildTasks(taskFuncs ...*TaskFunc[T]) []*Task[T] {
	var tasks []*Task[T]
	for _, taskFunc := range taskFuncs {
		tasks = append(tasks, t.BuildTask(taskFunc))
	}
	return tasks
}

func (t *TaskBuilder[T]) BuildTask(taskFunc *TaskFunc[T]) *Task[T] {
	return NewTaskContext[T](t.ctx, taskFunc, t.limit, t.onPanic)
}

func NewTaskBuilder[T any]() *TaskBuilder[T] {
	return NewTaskBuilderContext[T](context.Background())
}

func NewTaskBuilderContext[T any](ctx context.Context) *TaskBuilder[T] {
	return &TaskBuilder[T]{ctx: ctx}
}
