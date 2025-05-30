package gpool

import (
	"context"
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
		items3 = append(items3, i)
	}
	f := func(i int) {
		log.Println(i)
		time.Sleep(2 * time.Second)
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	r, err := p.Submit(
		NewTask(f, items, 1, nil),
		NewTask(f, items2, 1, nil),
		NewTaskContext(ctx, f, items3, 1, nil),
	)
	if err != nil {
		t.Error(err)
	} else {
		r.Wait()
	}
}
