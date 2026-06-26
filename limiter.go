package gpool

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

var limiterProvider = &sync.Pool{New: func() any {
	return NewRateLimiter(0)
}}

type RateLimiter struct {
	limiter  *rate.Limiter
	wait     chan struct{}
	stop     chan struct{}
	stopped  bool
	started  bool
	capacity int
	mu       sync.Mutex
}

func (r *RateLimiter) Capacity() int {
	return r.capacity
}

func (r *RateLimiter) SetCapacity(capacity int) {
	r.capacity = capacity
	r.limiter.SetLimit(rate.Limit(capacity))
}

func (r *RateLimiter) Pause() {
	r.limiter.SetBurst(0)
}

func (r *RateLimiter) Unpause() {
	r.limiter.SetBurst(r.capacity)
}

func (r *RateLimiter) Start() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.started {
		r.limiter.SetBurst(r.capacity)
		r.stop = make(chan struct{})
		r.started = true
		r.stopped = false
		go func() {
			for {
				select {
				case <-r.stop:
					return
				default:
					if r.limiter.Burst() == 0 {
						time.Sleep(time.Second)
					} else {
						time.Sleep(r.limiter.Reserve().Delay())
						r.wait <- struct{}{}
					}
				}
			}
		}()
	}
}

func (r *RateLimiter) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.stopped {
		r.limiter.SetBurst(0)
		close(r.stop)
		for len(r.wait) > 0 {
			<-r.wait
		}
		r.stopped = true
		r.started = false
	}
}

func (r *RateLimiter) Stopped() chan struct{} {
	return r.stop
}

func (r *RateLimiter) IsStopped() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopped
}

func (r *RateLimiter) IsStarted() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.started
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
