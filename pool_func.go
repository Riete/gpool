package gpool

type poolWithFunc struct {
	pool
	pf func(interface{})
}

type PoolWithFunc struct {
	poolWithFunc
}

func (p *poolWithFunc) putTask(f func(interface{}), vs []interface{}) {
	for _, v := range vs {
		p.task <- &task{f: f, param: v}
	}
}

func (p *poolWithFunc) Run(vs []interface{}) {
	p.done = make(chan struct{})
	p.wait = make(chan struct{})
	p.restTask = int64(len(vs))
	go p.putTask(p.pf, vs)
	p.loop()
}

func NewPoolWithFunc(f func(interface{}), size int64) *PoolWithFunc {
	p := &PoolWithFunc{poolWithFunc{pool{maxWorker: size}, f}}
	p.init()
	return p
}
