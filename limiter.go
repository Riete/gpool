package gpool

import (
	"context"

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
