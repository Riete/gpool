package gpool

import (
	"testing"
	"time"
)

func TestPool(t *testing.T) {
	var nums []int
	for i := 1; i < 100; i++ {
		nums = append(nums, i)
	}
	p := NewPool(5, int64(len(nums)))
	for _, i := range nums {
		p.Get()
		go func(i int) {
			t.Log(i)
			time.Sleep(time.Second)
			p.Put()
		}(i)
	}
	<-p.Done()
	t.Log("done")
}
