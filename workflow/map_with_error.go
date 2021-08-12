package workflow

import (
	"context"
	"runtime"
	"sync"
)

type ErrorMapFunc func(interface{}) (interface{}, error)
type HandleErrorItem func(interface{}) interface{}

// TODO: need to handle error
type ErrorMap struct {
	input           chan interface{}
	output          chan interface{}
	errorC          chan interface{}
	parallelism     int
	ctx             context.Context
	cancelFunc      context.CancelFunc
	errorMapFunc    ErrorMapFunc
	handleErrorItem HandleErrorItem
}

func NewErrorMap(errorMapFunc ErrorMapFunc, parallelism int, h HandleErrorItem) *ErrorMap {
	ctx, cancel := context.WithCancel(context.Background())
	ret := &ErrorMap{
		input:        make(chan interface{}),
		output:       make(chan interface{}),
		parallelism:  parallelism,
		ctx:          ctx,
		cancelFunc:   cancel,
		errorMapFunc: errorMapFunc,
		handleErrorItem: func(i interface{}) interface{} {
			return i
		},
	}
	if ret.parallelism <= 0 {
		ret.parallelism = runtime.NumCPU()
	}
	if h != nil {
		ret.handleErrorItem = h
	}
	return ret
}

var _ Flow = (*ErrorMap)(nil)

func (m *ErrorMap) In() chan<- interface{} {
	return m.input
}
func (m *ErrorMap) Out(int) <-chan interface{} {
	return m.output
}
func (m *ErrorMap) context() context.Context {
	return m.ctx
}

func (m *ErrorMap) ErrorTo(f Flow) Flow {
	go func() {
		defer close(f.In())
		for i := range m.errorC {
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

func (m *ErrorMap) Via(num int, f Flow) Flow {
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
func (m *ErrorMap) Vias(num int, flows ...Flow) []Flow {
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
func (m *ErrorMap) Close() {
	m.cancelFunc()
}
func (m *ErrorMap) run() {
	defer close(m.errorC)
	defer close(m.output)
	var wg sync.WaitGroup
	wg.Add(m.parallelism)
	fn := func() {
		for i := range m.input {
			res, err := m.errorMapFunc(i)
			if err != nil {
				select {
				case m.errorC <- m.handleErrorItem(i):
				case <-m.ctx.Done():
					m.cancelFunc()
					return
				}
			} else {
				select {
				case m.output <- res:
				case <-m.ctx.Done():
					m.cancelFunc()
					return
				}
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

func (m *ErrorMap) To(num int, s Sink) {
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
