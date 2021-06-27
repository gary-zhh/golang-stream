package workflow

import (
	"fmt"
	"testing"
)

func TestFlow(t *testing.T) {
	s := NewSliceSource([]interface{}{1, 2, 3, 4, 5, 6})
	w1 := NewMap(func(i interface{}) interface{} {
		return i.(int) * i.(int)
	}, 0)

	w2 := NewMap(func(i interface{}) interface{} {
		return float64(i.(int)) + 3.0
	}, 0)

	w3 := NewMap(func(i interface{}) interface{} {
		return float64(i.(int)) + 1.0
	}, 0)
	w4 := NewSplitMap(func(i interface{}) []interface{} {
		input := float64(i.(float64))
		return []interface{}{input, input / 10.0, input / 100.0}
	}, 0, 3)
	//s.Via(0, w1).Vias(0, w2.Via(0, w4), w3)
	//s.Via(0, w1).Via(0, )
	w2.Via(0, w4)
	s.Via(0, w1).Vias(0, w2, w3)
	s.Start()
	for i := range w4.Out(2) {
		fmt.Println(i)
	}

	fmt.Println()
	for i := range w3.Out(0) {
		fmt.Println(i)
	}
	_, _, _, _ = w1, w2, w3, w4
}
