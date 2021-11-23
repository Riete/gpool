package gpool

import (
	"fmt"
	"testing"
	"time"
)

func TestPool(t *testing.T) {
	f1 := func(v interface{}) { time.Sleep(1 * time.Second); fmt.Println("f1 ", v) }
	f2 := func(v interface{}) { time.Sleep(1 * time.Second); fmt.Println("f2 ", v) }
	pool := NewPool(2)
	defer pool.Close()
	data := []interface{}{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"}
	data2 := []interface{}{"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "kk", "ll", "mm", "nn"}
	pool.Map(f1, data)
	pool.SetMaxWorker(3)
	pool.Map(f2, data2)
}
