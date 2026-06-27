package gpool

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type RateLimiter struct {
	limiter  *rate.Limiter
	wait     chan struct{}
	stop     chan struct{}
	stopped  bool
	paused   bool
	capacity int
	wg       sync.WaitGroup
	mu       sync.Mutex
}

func (r *RateLimiter) Capacity() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.capacity
}

func (r *RateLimiter) setCapacity(capacity int) {
	r.limiter.SetLimit(rate.Limit(capacity))
	r.limiter.SetBurst(capacity)
}

func (r *RateLimiter) SetCapacity(capacity int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.capacity = capacity
	if !r.paused {
		r.setCapacity(capacity)
	}
}

func (r *RateLimiter) Pause() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.paused {
		r.setCapacity(0)
		r.paused = true
	}
}

func (r *RateLimiter) Resume() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.stopped && r.paused {
		r.setCapacity(r.capacity)
		r.paused = false
	}
}

func (r *RateLimiter) dispatch() {
	for {
		select {
		case <-r.stop:
			return
		default:
		}

		if r.limiter.Burst() == 0 {
			select {
			case <-r.stop:
				return
			case <-time.After(time.Second):
				continue
			}
		}
		reserver := r.limiter.Reserve()
		select {
		case <-r.stop:
			reserver.Cancel()
			return
		case <-time.After(reserver.Delay()):
			select {
			case <-r.stop:
				reserver.Cancel()
				return
			case r.wait <- struct{}{}:
			}
		}
	}
}

func (r *RateLimiter) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.stopped {
		r.setCapacity(0)
		close(r.stop)
		r.stopped = true
		r.paused = true
		r.wg.Wait()
		for len(r.wait) > 0 {
			<-r.wait
		}
	}
}

func (r *RateLimiter) Stopped() <-chan struct{} {
	return r.stop
}

func (r *RateLimiter) IsStopped() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopped
}

func (r *RateLimiter) IsPaused() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.paused
}

func (r *RateLimiter) Wait() chan struct{} {
	return r.wait
}

// NewRateLimiter capacity is the maximum token rate
func NewRateLimiter(capacity int) *RateLimiter {
	limiter := &RateLimiter{
		limiter:  rate.NewLimiter(rate.Limit(capacity), capacity),
		stop:     make(chan struct{}),
		wait:     make(chan struct{}, 64),
		capacity: capacity,
	}
	limiter.wg.Go(func() {
		limiter.dispatch()
	})
	return limiter
}
