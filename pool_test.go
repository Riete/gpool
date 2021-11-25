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
	pool.Run()
	data := []interface{}{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"}
	for _, d := range data {
		pool.Submit(f1, d)
		pool.Submit(f2, d)
	}
	pool.Wait()
	pool.SetMaxWorker(2)
	fmt.Println(pool.Cap())
}

func TestPoolWithFunc(t *testing.T) {
	f := func(v interface{}) { time.Sleep(1 * time.Second); fmt.Println(v) }
	pool := NewPoolWithFunc(f, 4)
	defer pool.Close()
	data := []interface{}{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"}
	data2 := []interface{}{"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "kk", "ll", "mm", "nn"}
	pool.Run(data)
	pool.SetMaxWorker(6)
	pool.Run(data2)

}
