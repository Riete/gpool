package gpool

import (
	"sync/atomic"
)

type pool struct {
	worker        func(interface{})
	maxWorker     int64
	currentWorker int64
	queue         chan interface{}
	restInQueue   int64
	done          chan struct{}
}

type Pool struct {
	pool
}

func (p *pool) run(v interface{}) {
	p.worker(v)
	atomic.AddInt64(&p.restInQueue, -1)
	if atomic.LoadInt64(&p.restInQueue) == 0 {
		close(p.done)
	}
	atomic.AddInt64(&p.currentWorker, -1)
}

func (p *pool) start(vs []interface{}) {
	for _, v := range vs {
		p.queue <- v
	}
	close(p.queue)
}

func (p *pool) loop() {
	for {
		if atomic.LoadInt64(&p.currentWorker) >= p.maxWorker {
			continue
		}
		select {
		case v, ok := <-p.queue:
			if ok {
				atomic.AddInt64(&p.currentWorker, 1)
				go p.run(v)
			}
		case <-p.done:
			return
		}
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
	p.worker = f
	p.queue = make(chan interface{})
	p.restInQueue = int64(len(vs))
	p.done = make(chan struct{})
	go p.start(vs)
	p.loop()
}

func NewPool(max int64) Pool {
	return Pool{pool{maxWorker: max}}
}
