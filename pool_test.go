package gpool

import (
	"log"
	"testing"
	"time"
)

func TestNewPool(t *testing.T) {
	p := NewPool[int](40)

	var items []int
	for i := 1; i < 101; i++ {
		items = append(items, i)
	}
	var items2 []int
	for i := 101; i < 201; i++ {
		items2 = append(items2, i)
	}
	var items3 []int
	for i := 201; i < 301; i++ {
		items2 = append(items2, i)
	}
	f := func(i int) {
		log.Println(i)
		time.Sleep(2 * time.Second)
	}
	r := p.Submit(
		NewTask[int](f, items, 10, nil),
		NewTask[int](f, items2, 20, nil),
		NewTask[int](f, items3, 10, nil),
	)
	r.Wait()
}
