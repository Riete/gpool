package gpool

import (
	"context"
	"sync"
)

// GenericRateLimiterPool is generic implementation of RateLimiterPool
type GenericRateLimiterPool[T any] struct {
	*Limiter
	f func(T)
}

func (g *GenericRateLimiterPool[T]) run(wg *sync.WaitGroup, v T, onPanic func(T, any)) {
	defer func() {
		wg.Done()
		if err := recover(); err != nil && onPanic != nil {
			onPanic(v, err)
		}
	}()
	g.f(v)
}

func (g *GenericRateLimiterPool[T]) Run(v []T, onPanic func(T, any)) {
	g.RunContext(context.Background(), v, onPanic)
}

func (g *GenericRateLimiterPool[T]) RunContext(ctx context.Context, v []T, onPanic func(T, any)) {
	wg := new(sync.WaitGroup)
	for _, i := range v {
		select {
		case <-ctx.Done():
			return
		case <-g.Wait():
			wg.Add(1)
			go g.run(wg, i, onPanic)
		}
	}
	wg.Wait()
}

func NewGenericRateLimiterPool[T any](capacity int, f func(T)) *GenericRateLimiterPool[T] {
	return &GenericRateLimiterPool[T]{NewLimiter(capacity), f}
}

// GenericConcurrentPool is generic implementation of ConcurrentPool
type GenericConcurrentPool[T any] struct {
	*Limiter
	f func(T)
}

func (g *GenericConcurrentPool[T]) run(idle chan struct{}, wg *sync.WaitGroup, v T, onPanic func(T, any)) {
	defer func() {
		idle <- struct{}{}
		wg.Done()
		if err := recover(); err != nil && onPanic != nil {
			onPanic(v, err)
		}
	}()
	g.f(v)
}

// Run max is number of maximum concurrency
func (g *GenericConcurrentPool[T]) Run(max int, v []T, onPanic func(T, any)) {
	g.RunContext(context.Background(), max, v, onPanic)
}

func (g *GenericConcurrentPool[T]) RunMax(v []T, onPanic func(T, any)) {
	g.Run(g.Capacity(), v, onPanic)
}

func (g *GenericConcurrentPool[T]) RunMaxContext(ctx context.Context, v []T, onPanic func(T, any)) {
	g.RunContext(ctx, g.Capacity(), v, onPanic)
}

func (g *GenericConcurrentPool[T]) RunContext(ctx context.Context, max int, v []T, onPanic func(T, any)) {
	wg := new(sync.WaitGroup)
	idle := make(chan struct{}, max)
	defer close(idle)
	for i := 0; i < max; i++ {
		idle <- struct{}{}
	}
	for _, i := range v {
		<-idle
		select {
		case <-ctx.Done():
			return
		case <-g.Wait():
			wg.Add(1)
			go g.run(idle, wg, i, onPanic)
		}
	}
	wg.Wait()
}

func NewGenericConcurrentPool[T any](capacity int, f func(T)) *GenericConcurrentPool[T] {
	return &GenericConcurrentPool[T]{NewLimiter(capacity), f}
}
