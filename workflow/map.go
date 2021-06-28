package workflow

import (
	"context"
	"runtime"
	"sync"
)

type MapFunc func(interface{}) interface{}

type Map struct {
	input       chan interface{}
	output      chan interface{}
	parallelism int
	ctx         context.Context
	cancelFunc  context.CancelFunc
	mapFunc     MapFunc
}

func NewMap(mapfunc MapFunc, parallelism int) *Map {
	ctx, cancel := context.WithCancel(context.Background())
	ret := &Map{
		input:       make(chan interface{}),
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

func (m *Map) In() chan<- interface{} {
	return m.input
}
func (m *Map) Out(int) <-chan interface{} {
	return m.output
}
func (m *Map) context() context.Context {
	return m.ctx
}

func (m *Map) Via(num int, f Flow) Flow {
	go func() {
		defer close(f.In())
		for i := range m.Out(num) {
			select {
			case f.In() <- i:
			case <-m.ctx.Done():
				return
			}
		}
	}()
	go f.run()
	return f
}
func (m *Map) Vias(num int, flows ...Flow) []Flow {
	go func() {
		wgs := make([]sync.WaitGroup, len(flows))
		for i, f := range flows {
			defer func(index int, flow Flow) {
				wgs[index].Wait()
				close(flow.In())
			}(i, f)
			go f.run()
		}
		for i := range m.Out(num) {

			for index, _ := range wgs {
				wgs[index].Add(1)
			}
			select {
			case <-m.ctx.Done():
				return
			default:
				for index, flow := range flows {
					go func(index int, flow Flow, i interface{}) {
						defer wgs[index].Done()
						flow.In() <- i
					}(index, flow, i)
				}
			}
		}
	}()
	return flows
}
func (m *Map) Close() {
	m.cancelFunc()
}
func (m *Map) run() {
	defer close(m.output)
	var wg sync.WaitGroup
	wg.Add(m.parallelism)
	fn := func() {
		for i := range m.input {
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
}

func (m *Map) To(num int, s Sink) {
	go func() {
		defer close(s.In())
		for i := range m.Out(num) {
			select {
			case s.In() <- i:
			case <-m.ctx.Done():
				return
			}
		}
	}()
	go s.run()
}
