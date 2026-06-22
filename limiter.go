package gpool

import (
	"time"

	"golang.org/x/time/rate"
)

type RateLimiter struct {
	l        *rate.Limiter
	w        chan struct{}
	stop     chan struct{}
	capacity int
}

func (r *RateLimiter) setCapacity(capacity int) {
	r.l.SetLimit(rate.Limit(capacity))
	r.l.SetBurst(capacity)
}

func (r *RateLimiter) Capacity() int {
	return r.capacity
}

func (r *RateLimiter) SetCapacity(capacity int) {
	r.capacity = capacity
	r.setCapacity(capacity)
}

func (r *RateLimiter) Start() {
	r.setCapacity(r.capacity)
	r.stop = make(chan struct{})
	for {
		select {
		case <-r.stop:
			return
		case <-time.After(r.l.Reserve().Delay()):
			r.w <- struct{}{}
		}
	}
}

func (r *RateLimiter) Stop() {
	close(r.stop)
	r.setCapacity(0)
}

func (r *RateLimiter) Wait() chan struct{} {
	return r.w
}

func (r *RateLimiter) Allow() bool {
	return r.l.Allow()
}

// NewRateLimiter capacity is the maximum token rate
func NewRateLimiter(capacity int) *RateLimiter {
	rl := &RateLimiter{
		l:        rate.NewLimiter(rate.Limit(capacity), capacity),
		w:        make(chan struct{}, capacity),
		capacity: capacity,
	}
	return rl
}
