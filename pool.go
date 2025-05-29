package gpool

import (
	"context"
	"sync"
)

type RateLimiterPool struct {
	*Limiter
}

func (l *RateLimiterPool) run(wg *sync.WaitGroup, f func(any), v any, onPanic func(any, any)) {
	defer func() {
		wg.Done()
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
	wg := new(sync.WaitGroup)
	for _, i := range v {
		select {
		case <-ctx.Done():
			return
		case <-l.Wait():
			wg.Add(1)
			go l.run(wg, f, i, onPanic)
		}
	}
	wg.Wait()
}

func NewRateLimiterPool(capacity int) *RateLimiterPool {
	return &RateLimiterPool{NewLimiter(capacity)}
}

// ConcurrentPool is different from RateLimiterPool it can limit the maximum concurrency for each task
type ConcurrentPool struct {
	*Limiter
}

func (c *ConcurrentPool) run(idle chan struct{}, wg *sync.WaitGroup, f func(any), v any, onPanic func(any, any)) {
	defer func() {
		idle <- struct{}{}
		wg.Done()
		if err := recover(); err != nil && onPanic != nil {
			onPanic(v, err)
		}
	}()
	f(v)
}

// Run max is number of maximum concurrency
func (c *ConcurrentPool) Run(max int, f func(any), v []any, onPanic func(any, any)) {
	c.RunContext(context.Background(), max, f, v, onPanic)
}

func (c *ConcurrentPool) RunMax(f func(any), v []any, onPanic func(any, any)) {
	c.Run(c.Capacity(), f, v, onPanic)
}

func (c *ConcurrentPool) RunMaxContext(ctx context.Context, f func(any), v []any, onPanic func(any, any)) {
	c.RunContext(ctx, c.Capacity(), f, v, onPanic)
}

func (c *ConcurrentPool) RunContext(ctx context.Context, max int, f func(any), v []any, onPanic func(any, any)) {
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
		case <-c.Wait():
			wg.Add(1)
			go c.run(idle, wg, f, i, onPanic)
		}
	}
	wg.Wait()
}

func NewConcurrentPool(capacity int) *ConcurrentPool {
	return &ConcurrentPool{NewLimiter(capacity)}
}
