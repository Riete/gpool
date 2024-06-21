package gpool

import (
	"sync"
	"time"

	"github.com/juju/ratelimit"
)

type Pool[T any] struct {
	limiter *ratelimit.Bucket
	qps     int64
	burst   int64
	f       func(T)
	rw      sync.RWMutex
}

func (p *Pool[T]) run(wg *sync.WaitGroup, v []T, onPanic func(T, any)) {
	for _, i := range v {
		go func(i T) {
			defer wg.Done()
			defer func() {
				if err := recover(); err != nil && onPanic != nil {
					onPanic(i, err)
				}
			}()
			p.f(i)
		}(i)
	}
}

func (p *Pool[T]) GetQPSLimit() (int64, int64) {
	return p.qps, p.burst
}

func (p *Pool[T]) SetQPSLimit(qps, burst int64) {
	p.rw.Lock()
	defer p.rw.Unlock()
	if qps > burst {
		qps = burst
	}
	p.qps = qps
	p.burst = burst
	p.limiter = ratelimit.NewBucketWithRate(float64(qps), burst)
}

func (p *Pool[T]) Run(v []T, onPanic func(T, any)) {
	p.rw.RLock()
	defer p.rw.RUnlock()
	wg := &sync.WaitGroup{}
	total := len(v)
	wg.Add(total)
	start := 0
	next := 0
	for {
		tokens := int(p.limiter.TakeAvailable(p.burst))
		if tokens < 1 {
			time.Sleep(time.Second)
			continue
		}
		next = start + tokens
		if next > total {
			next = total
		}
		p.run(wg, v[start:next], onPanic)
		if next == total {
			break
		}
		start = next
	}
	wg.Wait()
}

// New qps is normal QPS and allows bursts of up to burst to exceed the normal QPS,
func New[T any](qps, burst int64, f func(T)) *Pool[T] {
	p := &Pool[T]{f: f}
	p.SetQPSLimit(qps, burst)
	return p
}
