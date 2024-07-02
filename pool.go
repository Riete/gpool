package gpool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

type Limiter struct {
	limiter *rate.Limiter
}

func (l *Limiter) Limit() int {
	return int(l.limiter.Limit())
}

func (l *Limiter) Burst() int {
	return l.limiter.Burst()
}

func (l *Limiter) Capacity() (int, int) {
	return l.Limit(), l.Burst()
}

// SetLimit if new limit > burst then set new limit to burst
func (l *Limiter) SetLimit(limit int) {
	l.limiter.SetLimit(rate.Limit(min(limit, l.Burst())))
}

// SetBurst if new burst < limit then set limit to new burst
func (l *Limiter) SetBurst(burst int) {
	if burst < l.Limit() {
		l.SetLimit(burst)
	}
	l.limiter.SetBurst(burst)
}

func (l *Limiter) SetCapacity(limit int, burst int) {
	l.SetBurst(burst)
	l.SetLimit(limit)
}

func (l *Limiter) Wait() {
	_ = l.limiter.Wait(context.Background())
}

func (l *Limiter) WaitN(n int) {
	_ = l.limiter.WaitN(context.Background(), min(n, l.Burst()))
}

// NewLimiter limit is normal QPS and allows bursts of up to burst to exceed the normal QPS,
func NewLimiter(limit, burst int) *Limiter {
	return &Limiter{limiter: rate.NewLimiter(rate.Limit(min(limit, burst)), burst)}
}

type LimiterPool struct {
	*Limiter
}

func (l *LimiterPool) run(wg *sync.WaitGroup, f func(any), v any, onPanic func(any, any)) {
	defer wg.Done()
	defer func() {
		if err := recover(); err != nil && onPanic != nil {
			onPanic(v, err)
		}
	}()
	f(v)
}

func (l *LimiterPool) Run(f func(any), v []any, onPanic func(any, any)) {
	wg := &sync.WaitGroup{}
	wg.Add(len(v))
	for _, i := range v {
		l.Wait()
		go l.run(wg, f, i, onPanic)
	}
	wg.Wait()
}

func NewLimiterPool(l *Limiter) *LimiterPool {
	return &LimiterPool{l}
}

type GenericLimiterPool[T any] struct {
	*Limiter
	f func(T)
}

func (g *GenericLimiterPool[T]) run(wg *sync.WaitGroup, v T, onPanic func(T, any)) {
	defer wg.Done()
	defer func() {
		if err := recover(); err != nil && onPanic != nil {
			onPanic(v, err)
		}
	}()
	g.f(v)
}

func (g *GenericLimiterPool[T]) Run(v []T, onPanic func(T, any)) {
	wg := &sync.WaitGroup{}
	wg.Add(len(v))
	for _, i := range v {
		g.Wait()
		go g.run(wg, i, onPanic)
	}
	wg.Wait()
}

func NewGenericLimiterPool[T any](l *Limiter, f func(T)) *GenericLimiterPool[T] {
	return &GenericLimiterPool[T]{l, f}
}

// ConcurrentPool is different from LimiterPool it can limit the maximum concurrency when running
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

func (c *ConcurrentPool) Run(max int64, f func(any), v []any, onPanic func(any, any)) {
	wg := &sync.WaitGroup{}
	wg.Add(len(v))
	for _, i := range v {
		c.Wait()
		for {
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

type GenericConcurrentPool[T any] struct {
	*Limiter
	f func(T)
}

func (g *GenericConcurrentPool[T]) run(max *int64, wg *sync.WaitGroup, v T, onPanic func(T, any)) {
	defer wg.Done()
	defer atomic.AddInt64(max, 1)
	defer func() {
		if err := recover(); err != nil && onPanic != nil {
			onPanic(v, err)
		}
	}()
	g.f(v)
}

func (g *GenericConcurrentPool[T]) Run(max int64, v []T, onPanic func(T, any)) {
	wg := &sync.WaitGroup{}
	wg.Add(len(v))
	for _, i := range v {
		g.Wait()
		for {
			if atomic.LoadInt64(&max) > 0 {
				atomic.AddInt64(&max, -1)
				go g.run(&max, wg, i, onPanic)
				break
			}
			time.Sleep(time.Second)
		}
	}
	wg.Wait()
}

func NewGenericConcurrentPool[T any](l *Limiter, f func(T)) *GenericConcurrentPool[T] {
	return &GenericConcurrentPool[T]{l, f}
}
