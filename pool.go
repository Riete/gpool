package gpool

import (
	"sync/atomic"
)

type queue struct {
	callFunc func(interface{})
	param    interface{}
}

type pool struct {
	worker      chan struct{}
	maxWorker   int64
	queue       chan *queue
	restInQueue int64
	done        chan struct{}
}

type Pool struct {
	pool
}

func (p *pool) run(q *queue) {
	q.callFunc(q.param)
	p.worker <- struct{}{}
	atomic.AddInt64(&p.restInQueue, -1)
	if atomic.LoadInt64(&p.restInQueue) == 0 {
		close(p.done)
	}
}

func (p *pool) init() {
	p.queue = make(chan *queue)
	p.workerPrepare()
}

func (p *pool) put(f func(interface{}), v interface{}) {
	p.queue <- &queue{callFunc: f, param: v}
}

func (p *pool) puts(f func(interface{}), vs []interface{}) {
	for _, v := range vs {
		p.put(f, v)
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
	p.workerPrepare()

}

func (p *pool) Map(f func(interface{}), vs []interface{}) {
	p.done = make(chan struct{})
	p.restInQueue = int64(len(vs))
	go p.puts(f, vs)
	p.loop()
}

func (p *pool) Submit(f func(interface{}), v interface{}) {
	go p.put(f, v)
	atomic.AddInt64(&p.restInQueue, 1)
}

func (p *pool) Wait() {
	p.done = make(chan struct{})
	p.loop()
}

func (p *pool) Close() {
	close(p.queue)
	close(p.worker)
}

func NewPool(max int64) *Pool {
	p := &Pool{pool{maxWorker: max}}
	p.init()
	return p
}
