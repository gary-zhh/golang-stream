package workflow

import (
	"context"
	"runtime"
	"sync"
)

type FilterFunc func(interface{}) bool

type Filter struct {
	input       chan interface{}
	output      chan interface{}
	parallelism int
	ctx         context.Context
	cancelFunc  context.CancelFunc
	filterFunc  FilterFunc
}

func NewFilter(filterFunc FilterFunc, parallelism int) *Filter {
	ctx, cancel := context.WithCancel(context.Background())
	ret := &Filter{
		input:       make(chan interface{}),
		output:      make(chan interface{}),
		parallelism: parallelism,
		ctx:         ctx,
		cancelFunc:  cancel,
		filterFunc:  filterFunc,
	}
	if ret.parallelism <= 0 {
		ret.parallelism = runtime.NumCPU()
	}
	return ret
}

var _ Flow = (*Filter)(nil)

func (f *Filter) In() chan<- interface{} {
	return f.input
}
func (f *Filter) Out(int) <-chan interface{} {
	return f.output
}
func (f *Filter) context() context.Context {
	return f.ctx
}

func (filter *Filter) Via(num int, f Flow) Flow {
	go func() {
		defer close(f.In())
		for i := range filter.Out(num) {
			select {
			case f.In() <- i:
			case <-filter.ctx.Done():
				return
			}
		}
	}()
	go f.run()
	return f
}
func (f *Filter) Vias(num int, flows ...Flow) []Flow {
	go func() {
		wgs := make([]sync.WaitGroup, len(flows))
		for i, f := range flows {
			defer func(index int, flow Flow) {
				wgs[index].Wait()
				close(flow.In())
			}(i, f)
			go f.run()
		}
		for i := range f.Out(num) {

			for index, _ := range wgs {
				wgs[index].Add(1)
			}
			select {
			case <-f.ctx.Done():
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
func (f *Filter) Close() {
	f.cancelFunc()
}
func (f *Filter) run() {
	defer close(f.output)
	var wg sync.WaitGroup
	wg.Add(f.parallelism)
	fn := func() {
		for i := range f.input {
			if f.filterFunc(i) {
				select {
				case f.output <- i:
				case <-f.ctx.Done():
					f.cancelFunc()
					return
				}
			}
		}
	}
	for i := 0; i < f.parallelism; i++ {
		go func() {
			defer wg.Done()
			fn()
		}()
	}
	wg.Wait()
}

func (f *Filter) To(num int, s Sink) {
	go func() {
		defer close(s.In())
		for i := range f.Out(num) {
			select {
			case s.In() <- i:
			case <-f.ctx.Done():
				return
			}
		}
	}()
	go s.run()
}
