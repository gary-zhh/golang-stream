package workflow

import (
	"context"
	"runtime"
	"sync"
)

type SplitMapFunc func(interface{}) []interface{}

type SplitMap struct {
	input        chan interface{}
	output       []chan interface{}
	parallelism  int
	splitnum     int
	ctx          context.Context
	cancelFunc   context.CancelFunc
	splitMapFunc SplitMapFunc
}

func NewSplitMap(splitMapFunc SplitMapFunc, parallelism int, splitNum int) *SplitMap {
	ctx, cancel := context.WithCancel(context.Background())
	ret := &SplitMap{
		input:        make(chan interface{}),
		output:       make([]chan interface{}, splitNum),
		parallelism:  parallelism,
		splitnum:     splitNum,
		ctx:          ctx,
		cancelFunc:   cancel,
		splitMapFunc: splitMapFunc,
	}
	if ret.parallelism <= 0 {
		ret.parallelism = runtime.NumCPU()
	}
	for i := 0; i < splitNum; i++ {
		ret.output[i] = make(chan interface{})
	}
	return ret
}

var _ Flow = (*SplitMap)(nil)

func (s *SplitMap) In() chan<- interface{} {
	return s.input
}
func (s *SplitMap) Out(i int) <-chan interface{} {
	return s.output[i]
}
func (s *SplitMap) context() context.Context {
	return s.ctx
}

func (s *SplitMap) Via(num int, f Flow) Flow {
	go func() {
		defer close(f.In())
		for i := range s.Out(num) {
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
func (s *SplitMap) Vias(num int, flows ...Flow) []Flow {
	go func() {
		wgs := make([]sync.WaitGroup, len(flows))
		for i := range s.Out(num) {
			for index, _ := range wgs {
				wgs[index].Add(1)
			}
			select {
			case <-s.ctx.Done():
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
func (s *SplitMap) Close() {
	s.cancelFunc()
}
func (s *SplitMap) run() {
	var wg sync.WaitGroup
	wg.Add(s.parallelism)

	outwgs := make([]sync.WaitGroup, s.splitnum)
	fn := func() {
		for c := range s.input {
			for i, _ := range outwgs {
				outwgs[i].Add(1)
				//fmt.Println("get ", c, ",wg[", i, "] +1", "  ", time.Now())
			}
			for i, ins := range s.splitMapFunc(c) {
				select {
				case <-s.ctx.Done():
					s.cancelFunc()
					return
				default:
					go func(index int, ins interface{}) {
						defer outwgs[index].Done()
						s.output[index] <- ins
						//fmt.Println("send ", ins, "wg[", index, "] -1", "  ", time.Now())
					}(i, ins)
				}
			}
		}
	}

	for i, _ := range outwgs {
		defer func(index int) {
			outwgs[index].Wait()
			//fmt.Println("close ", index, "  ", time.Now())
			close(s.output[index])
		}(i)
	}

	defer wg.Wait()

	for i := 0; i < s.parallelism; i++ {
		go func() {
			defer wg.Done()
			fn()
		}()
	}
}

func (s *SplitMap) To(num int, sink Sink) {
	go func() {
		for i := range s.Out(num) {
			select {
			case sink.In() <- i:
			case <-s.ctx.Done():
				return
			}
		}
	}()
}
