package gpool

import (
	"fmt"
	"testing"
	"time"
)

func TestPool(t *testing.T) {
	var nums []int
	for i := 1; i < 100; i++ {
		nums = append(nums, i)
	}
	p := NewPool(5, int64(len(nums)))
	ch := make(chan int)
	go func() {
		for i := range ch {
			t.Log(i)
		}
		t.Log("quit")
	}()
	for _, i := range nums {
		p.Get()
		go func(i int) {
			defer p.Put()
			ch <- i
			time.Sleep(time.Second)
		}(i)
	}
	<-p.Done()
	close(ch)
	t.Log("done")
}

func TestPoolWithFunc(t *testing.T) {
	type tt struct {
		a int
		b int
	}
	var tts []*tt
	for i := 1; i < 100; i++ {
		tts = append(tts, &tt{a: i, b: i * i})
	}
	f := func(i *tt) {
		fmt.Println(i.a, i.b)
		time.Sleep(time.Second)
	}
	p := NewPoolWithFunc(5, tts, f)
	p.Run()
}
