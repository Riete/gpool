package conrate

import (
	"context"
	"sync"

	"github.com/riete/robinx"
)

// Task runtime maxConcurrency will use min(Task.maxConcurrency, Executor.limiter.capacity) if Task.maxConcurrency > 0
// else Executor.limiter.capacity in both ConcurrencyMode and RateLimitMode
// On task panic, Task.recover is preferred over default recover (print panic message and goroutine stack trace)
// Task weight is used for SWRR scheduling.
// Use TaskBuilder to build task
type Task[T any] struct {
	ctx            context.Context
	taskFunc       func(context.Context, T)
	param          []T
	maxConcurrency int
	recover        func(T, any)
	weight         int
	wait           chan struct{}
	weightedItemId robinx.ID
	wg             *sync.WaitGroup
}

func (t *Task[T]) done() {
	t.wg.Done()
}

type TaskBuilder[T any] struct {
	ctx            context.Context
	taskFunc       func(context.Context, T)
	maxConcurrency int
	recover        func(T, any)
	weight         int
}

func (t *TaskBuilder[T]) WithContext(ctx context.Context) *TaskBuilder[T] {
	t.ctx = ctx
	return t
}

func (t *TaskBuilder[T]) WithMaxConcurrency(maxConcurrency int) *TaskBuilder[T] {
	t.maxConcurrency = maxConcurrency
	return t
}

func (t *TaskBuilder[T]) WithRecover(recover func(T, any)) *TaskBuilder[T] {
	t.recover = recover
	return t
}

func (t *TaskBuilder[T]) WithWeight(weight int) *TaskBuilder[T] {
	if weight < 1 {
		weight = 1
	}
	t.weight = weight
	return t
}

func (t *TaskBuilder[T]) WithTaskFunc(f func(context.Context, T)) *TaskBuilder[T] {
	t.taskFunc = f
	return t
}

func (t *TaskBuilder[T]) BuildTask(param []T) *Task[T] {
	return &Task[T]{
		ctx:            t.ctx,
		taskFunc:       t.taskFunc,
		param:          param,
		maxConcurrency: t.maxConcurrency,
		recover:        t.recover,
		weight:         t.weight,
	}
}

func (t *TaskBuilder[T]) BuildTasks(params ...[]T) []*Task[T] {
	tasks := make([]*Task[T], 0, len(params))
	for _, param := range params {
		tasks = append(tasks, t.BuildTask(param))
	}
	return tasks
}

func NewTaskBuilder[T any]() *TaskBuilder[T] {
	return &TaskBuilder[T]{ctx: context.Background(), weight: 1}
}
