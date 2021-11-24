package gpool

import (
	"fmt"
	"testing"
	"time"
)

func TestPoolMap(t *testing.T) {
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

func TestPoolSubmit(t *testing.T) {
	f1 := func(v interface{}) { time.Sleep(1 * time.Second); fmt.Println("f1 ", v) }
	f2 := func(v interface{}) { time.Sleep(1 * time.Second); fmt.Println("f2 ", v) }
	pool := NewPool(2)
	defer pool.Close()
	data := []interface{}{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"}
	f := []func(interface{}){f1, f2, f1, f2, f1, f2, f1, f2, f1, f2, f1, f2, f1, f2}
	for i := len(data) - 1; i >= 0; i-- {
		pool.Submit(f[i], data[i])
	}
	pool.Wait()
}
