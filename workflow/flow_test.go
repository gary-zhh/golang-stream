package workflow

import (
	"errors"
	"fmt"
	"testing"
)

func TestFlow(t *testing.T) {
	// s1 := NewSliceSource([]interface{}{1, 2, 3})
	// w1 := NewMap(func(i interface{}) interface{} {
	// 	return i
	// }, 0)
	// s1.Via(0, w1)
	// s1.Start()
	//
	// s2 := NewSliceSource([]interface{}{4, 5, 6})
	// w2 := NewMap(func(i interface{}) interface{} {
	// 	return i
	// }, 0)
	// s2.Via(0, w2)
	// s2.Start()
	//
	// con := NewCombine(4, w1, w2)
	// for i := range con.Out(0) {
	// 	fmt.Println(i)
	// }

	s := NewSliceSource([]interface{}{1})
	w1 := NewMap(func(i interface{}) interface{} {
		return i
	}, 0)
	s.Via(0, w1)

	errw := NewMap(func(i interface{}) interface{} {
		return i
	}, 0)

	w2 := NewErrorMap(func(i interface{}) (interface{}, error) {
		tmp := i.(int)
		if tmp%2 == 1 {
			return 0, errors.New("error")
		} else {
			return tmp * tmp, nil
		}
	}, 0, func(i interface{}) interface{} {
		return i.(int) + 1
	})
	w2.ErrorTo(errw)

	con := NewCombine(4, w1, errw)
	con.Via(0, w2)

	s.Start()
	for i := range w2.Out(0) {
		fmt.Println(i)
	}

	// w2 := NewMap(func(i interface{}) interface{} {
	// 	return float64(i.(int)) + 3.0
	// }, 0)
	//
	// w3 := NewMap(func(i interface{}) interface{} {
	// 	return float64(i.(int)) + 1.0
	// }, 0)
	// w4 := NewSplitMap(func(i interface{}) []interface{} {
	// 	input := float64(i.(float64))
	// 	return []interface{}{input, input / 10.0, input / 100.0}
	// }, 0, 3)
	//s.Via(0, w1).Vias(0, w2.Via(0, w4), w3)
	//s.Via(0, w1).Via(0, )
	// w2.Via(0, w4)
	// s.Via(0, w1).Vias(0, w2, w3)

}
