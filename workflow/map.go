package workflow

import (
	"context"
	"runtime"
	"sync"
)

type MapFunc func(interface{}) interface{}

type Map struct {
	prenode     Outlet
	output      chan interface{}
	parallelism int
	ctx         context.Context
	cancelFunc  context.CancelFunc
	mapFunc     MapFunc
}

func NewMap(mapfunc MapFunc, parallelism int) *Map {
	ctx, cancel := context.WithCancel(context.Background())
	ret := &Map{
		output:      make(chan interface{}),
		parallelism: parallelism,
		ctx:         ctx,
		cancelFunc:  cancel,
		mapFunc:     mapfunc,
	}
	if ret.parallelism <= 0 {
		ret.parallelism = runtime.NumCPU()
	}
	return ret
}

var _ Flow = (*Map)(nil)

func (m *Map) setPreNode(outlet Outlet) {
	m.prenode = outlet
	m.ctx, m.cancelFunc = context.WithCancel(outlet.context())
}
func (m *Map) context() context.Context {
	return m.ctx
}
func (m *Map) Out() <-chan interface{} {
	return m.output
}
func (m *Map) Via(f Flow) Flow {
	f.setPreNode(m)
	f.run()
	return f
}
func (m *Map) Vias(flows ...Flow) []Flow {
	for _, f := range flows {
		f.setPreNode(m)
		f.run()
	}
	return flows
}
func (m *Map) Close() {
	m.cancelFunc()
}
func (m *Map) run() {
	go func() {
		defer close(m.output)
		var wg sync.WaitGroup
		wg.Add(m.parallelism)
		fn := func() {
			for i := range m.prenode.Out() {
				select {
				case m.output <- m.mapFunc(i):
				case <-m.ctx.Done():
					m.cancelFunc()
					return
				}
			}
		}
		for i := 0; i < m.parallelism; i++ {
			go func() {
				defer wg.Done()
				fn()
			}()
		}
		wg.Wait()
	}()
}

func (m *Map) To(s Sink) {
	s.setPreNode(m)
}
