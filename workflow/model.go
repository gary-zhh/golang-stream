package workflow

import "context"

type Inlet interface {
	In() chan<- interface{}
}

type Outlet interface {
	Out(int) <-chan interface{}
	context() context.Context
}

type Stop interface {
	Close()
}

type Flow interface {
	Inlet
	Outlet
	Stop
	Via(int, Flow) Flow
	Vias(int, ...Flow) []Flow
	To(int, Sink)
	run()
}

type Source interface {
	Outlet
	Via(int, Flow) Flow
	Vias(int, ...Flow) []Flow
	Start()
	Stop
}

type Sink interface {
	Inlet
	run()
}
