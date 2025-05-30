package gpool

import (
	"log"
	"testing"
	"time"
)

func TestNewPool(t *testing.T) {
	p := NewPool[int](60)

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
	r, err := p.Submit(
		NewTask(f, items, 20, nil),
		NewTask(f, items2, 20, nil),
		NewTask(f, items3, 20, nil),
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
