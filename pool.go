package gpool

import (
	"sync/atomic"
)

type task struct {
	f     func(interface{})
	param interface{}
}

type pool struct {
	worker    chan struct{}
	maxWorker int64
	task      chan *task
	restTask  int64
	done      chan struct{}
	wait      chan struct{}
	running   int64
}

type Pool struct {
	pool
}

func (p *pool) getWorker() {
	<-p.worker
}

func (p *pool) putWorker() {
	p.worker <- struct{}{}
}

func (p *pool) checkIsTaskDone() {
	if atomic.LoadInt64(&p.restTask) == 0 {
		close(p.done)
	}
}

func (p *pool) run(q *task) {
	q.f(q.param)
	p.putWorker()
	atomic.AddInt64(&p.restTask, -1)
	p.checkIsTaskDone()
}

func (p *pool) init() {
	p.task = make(chan *task)
	p.workerPrepare()
}

func (p *pool) putTask(f func(interface{}), v interface{}) {
	p.task <- &task{f: f, param: v}
}

func (p *pool) loop() {
	for {
		select {
		case v := <-p.task:
			p.getWorker()
			go p.run(v)
		case <-p.done:
			close(p.wait)
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
	if atomic.LoadInt64(&p.running) == 1 {
		return
	}
	if size < 1 {
		size = 1
	}
	p.maxWorker = size
	p.workerPrepare()
}

func (p *pool) Submit(f func(interface{}), v interface{}) {
	go p.putTask(f, v)
	atomic.AddInt64(&p.restTask, 1)
}

func (p *pool) Run() {
	atomic.SwapInt64(&p.running, 1)
	p.done = make(chan struct{})
	p.wait = make(chan struct{})
	go p.loop()
}

func (p *pool) Wait() {
	for {
		select {
		case <-p.wait:
			atomic.SwapInt64(&p.running, 0)
			return
		}
	}
}

func (p *pool) Close() {
	close(p.task)
	close(p.worker)
}

func NewPool(max int64) *Pool {
	p := &Pool{pool{maxWorker: max}}
	p.init()
	return p
}
