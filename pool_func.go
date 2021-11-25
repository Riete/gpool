package gpool

type poolWithFunc struct {
	pool
	pf func(interface{})
}

type PoolWithFunc struct {
	poolWithFunc
}

func (p *poolWithFunc) Run(vs []interface{}) {
	p.done = make(chan struct{})
	p.restTask = int64(len(vs))
	go p.putTask(p.pf, vs)
	p.loop()
}

func (p *poolWithFunc) SetFunc(f func(interface{})) {
	p.pf = f
}

func NewPoolWithFunc(f func(interface{}), size int64) *PoolWithFunc {
	p := &PoolWithFunc{poolWithFunc{pool{task: make(chan *task)}, f}}
	p.workerPrepare(size)
	return p
}
