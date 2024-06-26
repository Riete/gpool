package gpool

import (
	"sync"
	"time"

	"github.com/juju/ratelimit"
)

// Limiter qps is normal QPS and allows bursts of up to burst to exceed the normal QPS,
type Limiter struct {
	bucket *ratelimit.Bucket
	qps    int64
	burst  int64
	rw     sync.RWMutex
}

func (l *Limiter) GetCapacity() (int64, int64) {
	return l.qps, l.burst
}

func (l *Limiter) SetCapacity(qps, burst int64) {
	l.rw.Lock()
	defer l.rw.Unlock()
	l.qps = min(qps, burst)
	l.burst = burst
	l.bucket = ratelimit.NewBucketWithRate(float64(l.qps), l.burst)
}

func (l *Limiter) takeToken(count int64) int64 {
	for {
		tokens := l.bucket.TakeAvailable(min(count, l.burst))
		if tokens > 0 {
			return tokens
		}
		time.Sleep(time.Second)
	}
}

func (l *Limiter) TakeAvailableToken() int64 {
	return l.takeToken(l.burst)
}

func (l *Limiter) TakeToken(count int64) int64 {
	return l.takeToken(count)
}

func (l *Limiter) WaitAvailable() {
	_ = l.takeToken(1)
}

func NewLimiter(qps, burst int64) *Limiter {
	l := new(Limiter)
	l.SetCapacity(qps, burst)
	return l
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
	p.rw.RLock()
	defer p.rw.RUnlock()
	wg := &sync.WaitGroup{}
	wg.Add(len(v))
	for _, i := range v {
		p.WaitAvailable()
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
	p.rw.RLock()
	defer p.rw.RUnlock()
	wg := &sync.WaitGroup{}
	wg.Add(len(v))
	for _, i := range v {
		p.WaitAvailable()
		go p.run(wg, i, onPanic)
	}
	wg.Wait()
}

func NewGenericPool[T any](l *Limiter, f func(T)) *GenericPool[T] {
	return &GenericPool[T]{l, f}
}
