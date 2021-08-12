package workflow

import (
	"context"
	"runtime"
	"sync"
)

type Combine struct {
	input       []chan interface{}
	output      chan interface{}
	parallelism int
	ctx         context.Context
	cancelFunc  context.CancelFunc
}

func NewCombine(parallelism int, flows ...Flow) *Combine {
	ctx, cancel := context.WithCancel(context.Background())
	ret := &Combine{
		input:       make([]chan interface{}, len(flows)),
		output:      make(chan interface{}),
		parallelism: parallelism,
		ctx:         ctx,
		cancelFunc:  cancel,
	}
	for i := 0; i < len(flows); i++ {
		ret.input[i] = make(chan interface{})
		go func(i int) {
			for item := range flows[i].Out(0) {
				select {
				case ret.input[i] <- item:
				case <-ret.ctx.Done():
					return
				}
			}
		}(i)
	}
	if ret.parallelism <= 0 {
		ret.parallelism = runtime.NumCPU()
	}
	go ret.run()
	return ret
}

var _ Flow = (*Combine)(nil)

func (c *Combine) In() chan<- interface{} {
	panic("combine dont support In()")
	return nil
}
func (c *Combine) Out(int) <-chan interface{} {
	return c.output
}
func (c *Combine) context() context.Context {
	return c.ctx
}

func (c *Combine) Via(num int, f Flow) Flow {
	go func() {
		defer close(f.In())
		for i := range c.Out(num) {
			select {
			case f.In() <- i:
			case <-c.ctx.Done():
				return
			}
		}
	}()
	go f.run()
	return f
}
func (c *Combine) Vias(num int, flows ...Flow) []Flow {
	go func() {
		wgs := make([]sync.WaitGroup, len(flows))
		for i, f := range flows {
			defer func(index int, flow Flow) {
				wgs[index].Wait()
				close(flow.In())
			}(i, f)
			go f.run()
		}
		for i := range c.Out(num) {

			for index, _ := range wgs {
				wgs[index].Add(1)
			}
			select {
			case <-c.ctx.Done():
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
func (c *Combine) Close() {
	c.cancelFunc()
}
func (c *Combine) run() {
	defer close(c.output)
	var wg sync.WaitGroup
	wg.Add(c.parallelism * len(c.input))
	fn := func(ch chan interface{}) {
		defer wg.Done()
		for i := range ch {
			select {
			case c.output <- i:
			case <-c.ctx.Done():
				c.cancelFunc()
				return
			}
		}
	}
	for i := 0; i < c.parallelism; i++ {
		for _, ch := range c.input {
			go func(ch chan interface{}) {
				fn(ch)
			}(ch)
		}
	}
	wg.Wait()
}

func (c *Combine) To(num int, s Sink) {
	go func() {
		defer close(s.In())
		for i := range c.Out(num) {
			select {
			case s.In() <- i:
			case <-c.ctx.Done():
				return
			}
		}
	}()
	go s.run()
}
