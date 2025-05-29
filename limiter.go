package gpool

import (
	"context"

	"golang.org/x/time/rate"
)

type limiter struct {
	rl *rate.Limiter
	w  chan struct{}
}

func (l *limiter) capacity() int {
	return l.rl.Burst()
}

func (l *limiter) setCapacity(max int) {
	l.rl.SetLimit(rate.Limit(max))
	l.rl.SetBurst(max)
}

func (l *limiter) wait() chan struct{} {
	go func() {
		_ = l.rl.Wait(context.Background())
		l.w <- struct{}{}
	}()
	return l.w
}

// newLimiter max is the maximum token rate
func newLimiter(max int) *limiter {
	return &limiter{rl: rate.NewLimiter(rate.Limit(max), max), w: make(chan struct{})}
}
