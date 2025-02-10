package gpool

import (
	"context"
	"sync"
	"sync/atomic"
)

type RateLimiterPool struct {
	*Limiter
}

func (l *RateLimiterPool) run(wg *sync.WaitGroup, f func(any), v any, onPanic func(any, any)) {
	defer wg.Done()
	defer func() {
		if err := recover(); err != nil && onPanic != nil {
			onPanic(v, err)
		}
	}()
	f(v)
}

func (l *RateLimiterPool) Run(f func(any), v []any, onPanic func(any, any)) {
	l.RunContext(context.Background(), f, v, onPanic)
}

func (l *RateLimiterPool) RunContext(ctx context.Context, f func(any), v []any, onPanic func(any, any)) {
	wg := &sync.WaitGroup{}
	wg.Add(len(v))
	for _, i := range v {
		select {
		case <-ctx.Done():
			return
		case <-l.Wait():
			go l.run(wg, f, i, onPanic)
		}
	}
	wg.Wait()
}

func NewRateLimiterPool(l *Limiter) *RateLimiterPool {
	return &RateLimiterPool{l}
}

// ConcurrentPool is different from RateLimiterPool it can limit the maximum concurrency for each task
type ConcurrentPool struct {
	*Limiter
}

func (c *ConcurrentPool) run(idle *atomic.Int64, wg *sync.WaitGroup, f func(any), v any, onPanic func(any, any)) {
	defer wg.Done()
	defer idle.Add(1)
	defer func() {
		if err := recover(); err != nil && onPanic != nil {
			onPanic(v, err)
		}
	}()
	f(v)
}

// Run max is number of maximum concurrency
func (c *ConcurrentPool) Run(max int64, f func(any), v []any, onPanic func(any, any)) {
	c.RunContext(context.Background(), max, f, v, onPanic)
}

func (c *ConcurrentPool) RunContext(ctx context.Context, max int64, f func(any), v []any, onPanic func(any, any)) {
	wg := &sync.WaitGroup{}
	wg.Add(len(v))
	idle := new(atomic.Int64)
	idle.Store(max)
	for _, i := range v {
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.Wait():
				if idle.Load() > 0 {
					idle.Add(-1)
					go c.run(idle, wg, f, i, onPanic)
					break
				}
			}
		}
	}
	wg.Wait()
}

func NewConcurrentPool(l *Limiter) *ConcurrentPool {
	return &ConcurrentPool{l}
}
