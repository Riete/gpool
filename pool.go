package gpool

import (
	"sync"
	"sync/atomic"
	"time"
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
	wg := &sync.WaitGroup{}
	wg.Add(len(v))
	for _, i := range v {
		l.Wait()
		go l.run(wg, f, i, onPanic)
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

func (c *ConcurrentPool) run(max *int64, wg *sync.WaitGroup, f func(any), v any, onPanic func(any, any)) {
	defer wg.Done()
	defer atomic.AddInt64(max, 1)
	defer func() {
		if err := recover(); err != nil && onPanic != nil {
			onPanic(v, err)
		}
	}()
	f(v)
}

// Run max is number of maximum concurrency
func (c *ConcurrentPool) Run(max int64, f func(any), v []any, onPanic func(any, any)) {
	wg := &sync.WaitGroup{}
	wg.Add(len(v))
	for _, i := range v {
		for {
			c.Wait()
			if atomic.LoadInt64(&max) > 0 {
				atomic.AddInt64(&max, -1)
				go c.run(&max, wg, f, i, onPanic)
				break
			}
			time.Sleep(time.Second)
		}
	}
	wg.Wait()
}

func NewConcurrentPool(l *Limiter) *ConcurrentPool {
	return &ConcurrentPool{l}
}
