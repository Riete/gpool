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
	r.setCapacity(capacity)
}

func (r *RateLimiter) Pause() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started && !r.paused {
		r.setCapacity(0)
		r.paused = true
	}
}

func (r *RateLimiter) Resume() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started && r.paused {
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

		r.mu.Lock()
		burst := r.limiter.Burst()
		r.mu.Unlock()

		if burst == 0 {
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

func (r *RateLimiter) Start() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.started {
		r.setCapacity(r.capacity)
		r.stop = make(chan struct{})
		r.started = true
		r.stopped = false
		r.paused = false
		r.wg.Go(func() {
			r.dispatch()
		})
	}
}

func (r *RateLimiter) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.stopped {
		r.setCapacity(0)
		close(r.stop)
		r.started = false
		r.stopped = true
		r.paused = true
		r.wg.Wait()
		for len(r.wait) > 0 {
			<-r.wait
		}
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
	return &RateLimiter{
		limiter:  rate.NewLimiter(rate.Limit(capacity), capacity),
		wait:     make(chan struct{}, 64),
		capacity: capacity,
	}
}
