package gpool

import (
	"context"
	"sync"

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
	l.limiter.Tokens()
}

// NewLimiter limit is normal QPS and allows bursts of up to burst to exceed the normal QPS,
func NewLimiter(limit, burst int) *Limiter {
	return &Limiter{limiter: rate.NewLimiter(rate.Limit(min(limit, burst)), burst)}
}

type Pool struct {
	*Limiter
}

func (p *Pool) run(wg *sync.WaitGroup, f func(any), v any, onPanic func(any, any)) {
	defer wg.Done()
	defer func() {
		if err := recover(); err != nil && onPanic != nil {
			onPanic(v, err)
		}
	}()
	f(v)
}

func (p *Pool) Run(f func(any), v []any, onPanic func(any, any)) {
	wg := &sync.WaitGroup{}
	wg.Add(len(v))
	for _, i := range v {
		p.Wait()
		go p.run(wg, f, i, onPanic)
	}
	wg.Wait()
}

func NewPool(l *Limiter) *Pool {
	return &Pool{l}
}

type GenericPool[T any] struct {
	*Limiter
	f func(T)
}

func (p *GenericPool[T]) run(wg *sync.WaitGroup, v T, onPanic func(T, any)) {
	defer wg.Done()
	defer func() {
		if err := recover(); err != nil && onPanic != nil {
			onPanic(v, err)
		}
	}()
	p.f(v)
}

func (p *GenericPool[T]) Run(v []T, onPanic func(T, any)) {
	wg := &sync.WaitGroup{}
	wg.Add(len(v))
	for _, i := range v {
		p.Wait()
		go p.run(wg, i, onPanic)
	}
	wg.Wait()
}

func NewGenericPool[T any](l *Limiter, f func(T)) *GenericPool[T] {
	return &GenericPool[T]{l, f}
}
