package gpool

import (
	"context"
	"sync"
	"sync/atomic"
)

// GenericRateLimiterPool is generic implementation of RateLimiterPool
type GenericRateLimiterPool[T any] struct {
	*Limiter
	f func(T)
}

func (g *GenericRateLimiterPool[T]) run(wg *sync.WaitGroup, v T, onPanic func(T, any)) {
	defer wg.Done()
	defer func() {
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
	wg := &sync.WaitGroup{}
	wg.Add(len(v))
	for _, i := range v {
		select {
		case <-ctx.Done():
		default:
			g.Wait()
			go g.run(wg, i, onPanic)
		}
	}
	wg.Wait()
}

func NewGenericRateLimiterPool[T any](l *Limiter, f func(T)) *GenericRateLimiterPool[T] {
	return &GenericRateLimiterPool[T]{l, f}
}

// GenericConcurrentPool is generic implementation of ConcurrentPool
type GenericConcurrentPool[T any] struct {
	*Limiter
	f func(T)
}

func (g *GenericConcurrentPool[T]) run(idle *atomic.Int64, wg *sync.WaitGroup, v T, onPanic func(T, any)) {
	defer wg.Done()
	defer idle.Add(1)
	defer func() {
		if err := recover(); err != nil && onPanic != nil {
			onPanic(v, err)
		}
	}()
	g.f(v)
}

// Run max is number of maximum concurrency
func (g *GenericConcurrentPool[T]) Run(max int64, v []T, onPanic func(T, any)) {
	g.RunContext(context.Background(), max, v, onPanic)
}

func (g *GenericConcurrentPool[T]) RunContext(ctx context.Context, max int64, v []T, onPanic func(T, any)) {
	wg := &sync.WaitGroup{}
	wg.Add(len(v))
	idle := new(atomic.Int64)
	idle.Store(max)
	for _, i := range v {
		for {
			select {
			case <-ctx.Done():
			default:
				g.Wait()
				if idle.Load() > 0 {
					idle.Add(-1)
					go g.run(idle, wg, i, onPanic)
					break
				}
			}
		}
	}
	wg.Wait()
}

func NewGenericConcurrentPool[T any](l *Limiter, f func(T)) *GenericConcurrentPool[T] {
	return &GenericConcurrentPool[T]{l, f}
}
