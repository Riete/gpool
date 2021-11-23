package gpool

import (
	"fmt"
	"testing"
	"time"
)

func TestPool(t *testing.T) {
	f1 := func(v interface{}) { fmt.Println("f1 ", v) }
	f2 := func(v interface{}) { time.Sleep(time.Second); fmt.Println("f2 ", v) }
	pool := NewPool(7)
	data := []interface{}{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"}
	data2 := []interface{}{"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "kk", "ll", "mm", "nn"}
	pool.Map(f1, data)
	pool.SetMaxWorker(2)
	pool.Map(f2, data2)
}
