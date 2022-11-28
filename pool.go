package gpool

import (
	"sync/atomic"
)

type Pool struct {
	worker chan struct{}
	done   chan struct{}
	total  int64 // how many task to be run
	max    int64
}

func (p *Pool) initWorker() {
	p.done = make(chan struct{})
	p.worker = make(chan struct{}, p.max)
	for i := p.max; i > 0; i-- {
		p.worker <- struct{}{}
	}
}

func (p Pool) Done() chan struct{} {
	return p.done
}

func (p Pool) Get() {
	<-p.worker
}

func (p *Pool) close() {
	close(p.done)
	close(p.worker)
}

func (p *Pool) Put() {
	p.worker <- struct{}{}
	atomic.AddInt64(&p.total, -1)
	if atomic.LoadInt64(&p.total) == 0 {
		p.close()
	}
}

func NewPool(max, total int64) *Pool {
	if max > total {
		max = total
	}
	p := &Pool{max: max, total: total}
	p.initWorker()
	return p
}
