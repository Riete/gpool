package gpool

import (
	"time"

	"golang.org/x/time/rate"
)

type RateLimiter struct {
	limiter  *rate.Limiter
	wait     chan struct{}
	stop     chan struct{}
	capacity int
}

func (r *RateLimiter) setCapacity(capacity int) {
	r.limiter.SetLimit(rate.Limit(capacity))
	r.limiter.SetBurst(capacity)
}

func (r *RateLimiter) Capacity() int {
	return r.capacity
}

func (r *RateLimiter) SetCapacity(capacity int) {
	r.capacity = capacity
}

func (r *RateLimiter) Start() {
	r.setCapacity(r.capacity)
	r.stop = make(chan struct{})
	go func() {
		for {
			select {
			case <-r.stop:
				return
			case <-time.After(r.limiter.Reserve().Delay()):
				r.wait <- struct{}{}
			}
		}
	}()
}

func (r *RateLimiter) Stop() {
	r.setCapacity(0)
	close(r.stop)
	for len(r.wait) > 0 {
		<-r.wait
	}
}

func (r *RateLimiter) Stopped() chan struct{} {
	return r.stop
}

func (r *RateLimiter) Wait() chan struct{} {
	return r.wait
}

func (r *RateLimiter) Allow() bool {
	return r.limiter.Allow()
}

// NewRateLimiter capacity is the maximum token rate
func NewRateLimiter(capacity int) *RateLimiter {
	return &RateLimiter{
		limiter:  rate.NewLimiter(rate.Limit(capacity), capacity),
		wait:     make(chan struct{}, 64),
		capacity: capacity,
	}
}
