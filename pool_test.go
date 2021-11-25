package gpool

import (
	"fmt"
	"testing"
	"time"
)

func TestPool(t *testing.T) {
	f1 := func(v interface{}) { time.Sleep(1 * time.Second); fmt.Println("f1 ", v) }
	f2 := func(v interface{}) { time.Sleep(2 * time.Second); fmt.Println("f2 ", v) }
	pool := NewPool(4)
	defer pool.Close()
	data := []interface{}{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"}
	pool.Submit(f1, data)
	pool.SetMaxWorker(6)
	pool.Submit(f2, data)
	pool.Run()
}

func TestPoolWithFunc(t *testing.T) {
	f1 := func(v interface{}) { time.Sleep(1 * time.Second); fmt.Println("f1: ", v) }
	f2 := func(v interface{}) { time.Sleep(1 * time.Second); fmt.Println("f2: ", v) }
	pool := NewPoolWithFunc(f1, 4)
	defer pool.Close()
	data := []interface{}{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"}
	data2 := []interface{}{"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "kk", "ll", "mm", "nn"}
	pool.Run(data)
	pool.SetMaxWorker(6)
	pool.SetFunc(f2)
	pool.Run(data2)
}
