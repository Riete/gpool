package gpool

import (
	"fmt"
	"log"
	"testing"
	"time"
)

func TestNewTaskPool(t *testing.T) {
	p := NewTaskPool[int](60)
	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Println("running task:", p.RunningCount(), "pending task:", p.PendingCount(), "completed task:", p.CompletedCount())
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
		log.Println(i)
		time.Sleep(time.Second)
	}
	tb := NewTaskBuilder[int](20, nil)
	r, err := p.Submit(
		tb.Build(f, items),
		tb.Build(f, items2),
		tb.Build(f, items3),
	)
	if err != nil {
		t.Error(err)
	} else {
		r.Wait()
	}
	t.Log("===========================================================")
	time.Sleep(2 * time.Second)
	r, err = p.Submit(
		NewTask(f, items, 2, nil),
		NewTask(f, items2, 2, nil),
		NewTask(f, items3, 2, nil),
	)
	if err != nil {
		t.Error(err)
	} else {
		r.Wait()
	}
}
