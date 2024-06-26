package gpool

import (
	"log"
	"testing"
	"time"
)

var limiter = NewLimiter(14, 20)

func TestPool(t *testing.T) {
	p := NewPool(limiter)
	var items []any
	for i := 1; i < 101; i++ {
		items = append(items, i)
	}
	var items2 []any
	for i := 101; i < 201; i++ {
		items2 = append(items2, i)
	}
	f := func(i any) {
		log.Println(i)
		time.Sleep(time.Second)
	}
	go p.Run(f, items, nil)
	p.Run(f, items2, nil)
}

func TestGenericPool(t *testing.T) {
	p := NewGenericPool[int](limiter, func(i int) {
		log.Println(i)
		time.Sleep(time.Second)
	})
	var items []int
	for i := 1; i < 101; i++ {
		items = append(items, i)
	}
	p.Run(items, nil)
}
