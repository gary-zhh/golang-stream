package workflow

import "context"

type Inlet interface {
	setPreNode(Outlet)
}

type Outlet interface {
	Out() <-chan interface{}
	context() context.Context
}

type Stop interface {
	Close()
}

type Flow interface {
	Inlet
	Outlet
	Stop
	Via(Flow) Flow
	Vias(...Flow) []Flow
	To(Sink)
	run()
}

type Source interface {
	Outlet
	Via(Flow) Flow
	Vias(...Flow) []Flow
	Start()
	Stop
}

type Sink interface {
	Inlet
	run()
}
