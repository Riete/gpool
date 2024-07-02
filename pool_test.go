package gpool

import (
	"fmt"
	"log"
	"testing"
	"time"
)

var limiter = NewLimiter(10, 10)

func TestLimiterPool(t *testing.T) {
	p := NewRateLimiterPool(limiter)
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
		time.Sleep(5 * time.Second)
	}
	go p.Run(f, items, nil)
	p.Run(f, items2, nil)
}

func TestGenericLimiterPool(t *testing.T) {
	p := NewGenericRateLimiterPool[int](limiter, func(i int) {
		log.Println(i)
		time.Sleep(time.Second)
	})
	var items []int
	for i := 1; i < 101; i++ {
		items = append(items, i)
	}
	go func() {
		time.Sleep(time.Second)
		p.SetCapacity(5, 5)
		p.SetBurst(3)
		fmt.Println(p.Limit(), p.Burst())
	}()
	p.Run(items, nil)
}

func TestConcurrentPool(t *testing.T) {
	p := NewConcurrentPool(limiter)
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
		time.Sleep(5 * time.Second)
	}
	go p.Run(8, f, items, nil)
	p.Run(2, f, items2, nil)
}

func TestGenericConcurrentPool(t *testing.T) {
	p := NewGenericConcurrentPool(limiter, func(i int) {
		log.Println(i)
		time.Sleep(3 * time.Second)
	})
	var items []int
	for i := 1; i < 101; i++ {
		items = append(items, i)
	}
	var items2 []int
	for i := 101; i < 201; i++ {
		items2 = append(items2, i)
	}
	go p.Run(6, items, nil)
	p.Run(4, items2, nil)
}
