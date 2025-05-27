package gpool

import (
	"context"

	"golang.org/x/time/rate"
)

type Limiter struct {
	limiter *rate.Limiter
	wait    chan struct{}
}

func (l *Limiter) Capacity() int {
	return l.limiter.Burst()
}

func (l *Limiter) SetCapacity(max int) {
	l.limiter.SetLimit(rate.Limit(max))
	l.limiter.SetBurst(max)
}

func (l *Limiter) Wait() chan struct{} {
	go func() {
		_ = l.limiter.Wait(context.Background())
		l.wait <- struct{}{}
	}()
	return l.wait
}

// NewLimiter max is maximum token rate
func NewLimiter(max int) *Limiter {
	return &Limiter{limiter: rate.NewLimiter(rate.Limit(max), max), wait: make(chan struct{})}
}
