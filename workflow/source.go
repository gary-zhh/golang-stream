package workflow

import "context"

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
	f.setPreNode(s)
	f.run()
	return f
}
func (s *SliceSource) Vias(flows ...Flow) []Flow {
	for _, f := range flows {
		f.setPreNode(s)
		f.run()
	}
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
