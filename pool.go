package gpool

import (
	"sync/atomic"
)

type pool struct {
	callFunc    func(interface{})
	worker      chan struct{}
	maxWorker   int64
	queue       chan interface{}
	restInQueue int64
	done        chan struct{}
}

type Pool struct {
	pool
}

func (p *pool) run(v interface{}) {
	p.callFunc(v)
	p.worker <- struct{}{}
	atomic.AddInt64(&p.restInQueue, -1)
	if atomic.LoadInt64(&p.restInQueue) == 0 {
		close(p.done)
	}
}

func (p *pool) start(vs []interface{}) {
	for _, v := range vs {
		p.queue <- v
	}
}

func (p *pool) loop() {
	for {
		select {
		case v := <-p.queue:
			<-p.worker
			go p.run(v)
		case <-p.done:
			return
		}
	}
}

func (p *pool) workerPrepare() {
	p.worker = make(chan struct{}, p.maxWorker)
	for i := p.maxWorker; i > 0; i-- {
		p.worker <- struct{}{}
	}
}

func (p pool) Cap() int64 {
	return p.maxWorker
}

func (p *pool) SetMaxWorker(size int64) {
	if size < 1 {
		size = 1
	}
	p.maxWorker = size
}

func (p *pool) Map(f func(interface{}), vs []interface{}) {
	p.callFunc = f
	p.queue = make(chan interface{})
	p.restInQueue = int64(len(vs))
	p.done = make(chan struct{})
	p.workerPrepare()
	go p.start(vs)
	p.loop()
}

func (p *pool) Close() {
	close(p.queue)
	close(p.worker)
}

func NewPool(max int64) *Pool {
	return &Pool{pool{maxWorker: max}}
}
