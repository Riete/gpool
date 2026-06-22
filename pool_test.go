package gpool

import (
	"fmt"
	"testing"
	"time"
)

func TestNewConcurrentTaskPool(t *testing.T) {
	p := NewConcurrentTaskPool[int](60)
	go func() {
		time.Sleep(2 * time.Second)
		p.Stop()
	}()
	counter := p.Counter()
	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Println("running task:", counter.Running(), "pending task:", counter.Pending(), "completed task:", counter.Completed())
		}
	}()
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
		items3 = append(items3, i)
	}
	f := func(i int) {
		// log.Println(i)
		time.Sleep(time.Second)
	}
	taskBuilder := NewTaskBuilder[int]().WitTaskFunc(f).WithConcurrency(20)
	future1 := p.Submit(taskBuilder.Build(items, items2, items3)...)
	future2 := p.Submit(taskBuilder.Build(items, items2, items3)...)
	p.Wait(future1, future2)
	fmt.Println("running task:", counter.Running(), "pending task:", counter.Pending(), "completed task:", counter.Completed())
	time.Sleep(3 * time.Second)
}

func TestNewRateLimiterTaskPool(t *testing.T) {
	p := NewRateLimiterTaskPool[int](60)
	counter := p.Counter()
	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Println("running task:", counter.Running(), "pending task:", counter.Pending(), "completed task:", counter.Completed())
		}
	}()
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
		items3 = append(items3, i)
	}
	f := func(i int) {
		// log.Println(i)
		time.Sleep(time.Second)
	}
	taskBuilder := NewTaskBuilder[int]().WitTaskFunc(f).WithConcurrency(20)
	future1 := p.Submit(taskBuilder.Build(items, items2, items3)...)
	future2 := p.Submit(taskBuilder.Build(items, items2, items3)...)
	p.Wait(future1, future2)
	fmt.Println("running task:", counter.Running(), "pending task:", counter.Pending(), "completed task:", counter.Completed())
}
