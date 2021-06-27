package workflow

import (
	"context"
	"sync"
)

type SliceSource struct {
	output     chan interface{}
	ctx        context.Context
	cancelFunc context.CancelFunc
	items      []interface{}
}

var _ Source = (*SliceSource)(nil)

func NewSliceSource(items []interface{}) *SliceSource {
	ctx, cancel := context.WithCancel(context.Background())
	return &SliceSource{
		output:     make(chan interface{}),
		ctx:        ctx,
		cancelFunc: cancel,
		items:      items,
	}
}
func (s *SliceSource) Out() <-chan interface{} {
	return s.output
}

func (s *SliceSource) context() context.Context {
	return s.ctx
}

func (s *SliceSource) Via(f Flow) Flow {
	go func() {
		defer close(f.In())
		for i := range s.Out() {
			select {
			case f.In() <- i:
			case <-s.ctx.Done():
				return
			}
		}
	}()
	go f.run()
	return f
}
func (s *SliceSource) Vias(flows ...Flow) []Flow {
	go func() {
		wgs := make([]sync.WaitGroup, len(flows))
		for i := range s.Out() {
			for index, _ := range wgs {
				wgs[index].Add(1)
			}
			select {
			case <-s.ctx.Done():
				return
			default:
				for index, flow := range flows {
					go func(index int, f Flow, i interface{}) {
						defer wgs[index].Done()
						f.In() <- i
					}(index, flow, i)
				}
			}
		}
		for i, f := range flows {
			defer func(index int, flow Flow) {
				wgs[index].Wait()
				close(flow.In())
			}(i, f)
			go f.run()
		}
	}()

	return flows
}

func (s *SliceSource) Close() {
	s.cancelFunc()
}

func (s *SliceSource) Start() {
	go func() {
		defer close(s.output)
		for _, item := range s.items {
			select {
			case s.output <- item:
			case <-s.ctx.Done():
				return
			}
		}
	}()
}
