package workflow

import (
	"fmt"
	"testing"
)

func TestFlow(t *testing.T) {
	s := NewSliceSource([]interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9})
	w1 := NewMap(func(i interface{}) interface{} {
		return i.(int) * i.(int)
	}, 0)

	w2 := NewMap(func(i interface{}) interface{} {
		return float64(i.(int)) / 10.0
	}, 0)

	w3 := NewMap(func(i interface{}) interface{} {
		return float64(i.(int)) + 1.0
	}, 0)

	s.Via(w1).Vias(w2, w3)
	s.Start()
	for i := range w2.Out() {
		fmt.Println(i)
	}
	for i := range w3.Out() {
		fmt.Println(i)
	}
}
