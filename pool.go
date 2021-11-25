package gpool

import (
	"sync/atomic"
)

type task struct {
	f     func(interface{})
	param interface{}
}

type pool struct {
	worker   chan struct{}
	task     chan *task
	restTask int64
	done     chan struct{}
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

func (p *pool) putTask(f func(interface{}), vs []interface{}) {
	for _, v := range vs {
		p.task <- &task{f: f, param: v}
	}
}

func (p *pool) loop() {
	for {
		select {
		case v := <-p.task:
			p.getWorker()
			go p.run(v)
		case <-p.done:
			return
		}
	}
}

func (p *pool) workerPrepare(size int64) {
	p.worker = make(chan struct{}, size)
	for i := size; i > 0; i-- {
		p.worker <- struct{}{}
	}
}

func (p *pool) SetMaxWorker(size int64) {
	if size < 1 {
		size = 1
	}
	p.workerPrepare(size)
}

func (p *pool) Submit(f func(interface{}), vs []interface{}) {
	go p.putTask(f, vs)
	p.restTask += int64(len(vs))
}

func (p *pool) Run() {
	p.done = make(chan struct{})
	p.loop()
}

func (p *pool) Close() {
	close(p.task)
	close(p.worker)
}

func NewPool(size int64) *Pool {
	p := &Pool{pool{task: make(chan *task)}}
	p.workerPrepare(size)
	return p
}
