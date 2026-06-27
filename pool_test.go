package gpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func ints(n int) []int {
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}
	return items
}

func waitUntil(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout waiting for condition")
}

func waitCounterSettled(t *testing.T, c *Counter, timeout time.Duration) {
	t.Helper()
	waitUntil(t, timeout, func() bool {
		return c.Pending() == 0 && c.Running() == 0
	})
}

func assertCounterZeroPending(t *testing.T, c *Counter) {
	t.Helper()
	if p := c.Pending(); p != 0 {
		t.Fatalf("Pending() = %d, want 0", p)
	}
	if r := c.Running(); r != 0 {
		t.Fatalf("Running() = %d, want 0", r)
	}
}

func TestSubmitAfterStop(t *testing.T) {
	p := NewConcurrentPool[int](4)
	p.Stop()

	f := p.Submit(NewTaskBuilder[int]().WithTaskFunc(func(context.Context, int) {}).BuildTask(ints(1)))
	if !errors.Is(f.Error(), ErrPoolStopped) {
		t.Fatalf("Error() = %v, want ErrPoolStopped", f.Error())
	}
	f.Wait()
}

func TestDoubleStop(t *testing.T) {
	p := NewConcurrentPool[int](4)
	p.Stop()
	p.Stop()
	if !p.IsStopped() {
		t.Fatal("expected pool stopped")
	}
}

func TestConcurrentPoolUnlimitedCompletesAll(t *testing.T) {
	p := NewConcurrentPool[int](4)
	defer p.Stop()

	const n = 20
	var ran atomic.Int64
	f := p.Submit(NewTaskBuilder[int]().WithTaskFunc(func(context.Context, int) {
		ran.Add(1)
	}).BuildTask(ints(n)))
	p.Wait(f)
	waitCounterSettled(t, p.Counter(), 5*time.Second)

	if got := ran.Load(); got != n {
		t.Fatalf("ran %d items, want %d", got, n)
	}
	assertCounterZeroPending(t, p.Counter())
	if got := p.Counter().Completed(); got != n {
		t.Fatalf("Completed() = %d, want %d", got, n)
	}
}

func TestConcurrentPoolLimitedTaskConcurrency(t *testing.T) {
	p := NewConcurrentPool[int](8)
	defer p.Stop()

	var mu sync.Mutex
	running := 0
	maxRunning := 0
	const n = 24

	f := p.Submit(NewTaskBuilder[int]().
		WithTaskFunc(func(context.Context, int) {
			mu.Lock()
			running++
			if running > maxRunning {
				maxRunning = running
			}
			mu.Unlock()
			time.Sleep(30 * time.Millisecond)
			mu.Lock()
			running--
			mu.Unlock()
		}).
		WithMaxConcurrency(3).
		BuildTask(ints(n)))
	p.Wait(f)
	waitCounterSettled(t, p.Counter(), 10*time.Second)

	if maxRunning > 3 {
		t.Fatalf("max running %d, want <= 3", maxRunning)
	}
	if got := p.Counter().Completed(); got != n {
		t.Fatalf("Completed() = %d, want %d", got, n)
	}
}

func TestConcurrentPoolPoolCapacity(t *testing.T) {
	p := NewConcurrentPool[int](2)
	defer p.Stop()

	var mu sync.Mutex
	running := 0
	maxRunning := 0
	const n = 12

	f := p.Submit(NewTaskBuilder[int]().WithTaskFunc(func(context.Context, int) {
		mu.Lock()
		running++
		if running > maxRunning {
			maxRunning = running
		}
		mu.Unlock()
		time.Sleep(40 * time.Millisecond)
		mu.Lock()
		running--
		mu.Unlock()
	}).BuildTask(ints(n)))
	p.Wait(f)
	waitCounterSettled(t, p.Counter(), 10*time.Second)

	if maxRunning > 2 {
		t.Fatalf("max running %d, want <= 2", maxRunning)
	}
}

func TestRateLimitPoolUnlimitedCompletesAll(t *testing.T) {
	p := NewRateLimitPool[int](200)
	defer p.Stop()

	const n = 15
	var ran atomic.Int64
	f := p.Submit(NewTaskBuilder[int]().WithTaskFunc(func(context.Context, int) {
		ran.Add(1)
	}).BuildTask(ints(n)))
	p.Wait(f)
	waitCounterSettled(t, p.Counter(), 5*time.Second)

	if got := ran.Load(); got != n {
		t.Fatalf("ran %d items, want %d", got, n)
	}
}

func TestRateLimitPoolLimitedTaskConcurrency(t *testing.T) {
	p := NewRateLimitPool[int](100)
	defer p.Stop()

	const n = 10
	var ran atomic.Int64
	f := p.Submit(NewTaskBuilder[int]().
		WithTaskFunc(func(context.Context, int) {
			ran.Add(1)
		}).
		WithMaxConcurrency(5).
		BuildTask(ints(n)))
	p.Wait(f)
	waitCounterSettled(t, p.Counter(), 5*time.Second)

	if got := ran.Load(); got != n {
		t.Fatalf("ran %d items, want %d", got, n)
	}
}

func TestFutureCancel(t *testing.T) {
	p := NewConcurrentPool[int](8)
	defer p.Stop()

	const n = 100
	started := make(chan struct{})
	release := make(chan struct{})
	var ran atomic.Int64

	f := p.Submit(NewTaskBuilder[int]().WithTaskFunc(func(context.Context, int) {
		if ran.Add(1) == 1 {
			close(started)
		}
		<-release
	}).BuildTask(ints(n)))

	<-started
	f.Cancel()
	close(release)
	p.Wait(f)
	waitCounterSettled(t, p.Counter(), 10*time.Second)

	assertCounterZeroPending(t, p.Counter())
	completed := p.Counter().Completed()
	canceled := p.Counter().Canceled()
	if completed+canceled != n {
		t.Fatalf("completed(%d) + canceled(%d) = %d, want %d", completed, canceled, completed+canceled, n)
	}
	if canceled == 0 {
		t.Fatal("expected some canceled params")
	}
}

func TestCounterAccountingAfterComplete(t *testing.T) {
	p := NewConcurrentPool[int](4)
	defer p.Stop()

	const n = 12
	f := p.Submit(NewTaskBuilder[int]().WithTaskFunc(func(context.Context, int) {}).BuildTask(ints(n)))
	p.Wait(f)
	waitCounterSettled(t, p.Counter(), 5*time.Second)

	c := p.Counter()
	assertCounterZeroPending(t, c)
	if got := c.Completed(); got != n {
		t.Fatalf("Completed() = %d, want %d", got, n)
	}
	if got := c.Canceled(); got != 0 {
		t.Fatalf("Canceled() = %d, want 0", got)
	}
}

func TestPauseResumeConcurrentPool(t *testing.T) {
	p := NewConcurrentPool[int](10)
	defer p.Stop()

	const n = 30
	f := p.Submit(NewTaskBuilder[int]().WithTaskFunc(func(context.Context, int) {
		time.Sleep(30 * time.Millisecond)
	}).BuildTask(ints(n)))

	time.Sleep(80 * time.Millisecond)
	p.Pause()
	if !p.IsPaused() {
		t.Fatal("expected pool paused")
	}

	p.Resume()
	if p.IsPaused() {
		t.Fatal("expected pool resumed")
	}

	p.Wait(f)
	waitCounterSettled(t, p.Counter(), 10*time.Second)
	if got := p.Counter().Completed(); got != n {
		t.Fatalf("Completed() = %d, want %d", got, n)
	}
}

func TestStopWhilePaused(t *testing.T) {
	p := NewConcurrentPool[int](4)

	block := make(chan struct{})
	f := p.Submit(NewTaskBuilder[int]().WithTaskFunc(func(context.Context, int) {
		<-block
	}).BuildTask(ints(4)))

	waitUntil(t, 3*time.Second, func() bool { return p.Counter().Running() > 0 })

	p.Pause()
	done := make(chan struct{})
	go func() {
		p.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop() blocked while pool paused")
	}
	close(block)
	p.Wait(f)
}

func TestGracefulStop(t *testing.T) {
	p := NewConcurrentPool[int](4)

	const n = 8
	var ran atomic.Int64
	f := p.Submit(NewTaskBuilder[int]().WithTaskFunc(func(context.Context, int) {
		time.Sleep(20 * time.Millisecond)
		ran.Add(1)
	}).BuildTask(ints(n)))

	waitUntil(t, 3*time.Second, func() bool {
		return p.Counter().Running() > 0 || p.Counter().Completed() > 0
	})

	done := make(chan struct{})
	go func() {
		p.GracefulStop(5 * time.Second)
		close(done)
	}()

	p.Wait(f)
	<-done

	if !p.IsStopped() {
		t.Fatal("expected pool stopped after GracefulStop()")
	}
	if got := ran.Load(); got != n {
		t.Fatalf("ran %d items, want %d", got, n)
	}
}

func TestTaskPanicRecover(t *testing.T) {
	p := NewConcurrentPool[int](2)
	defer p.Stop()

	var recovered atomic.Int64
	f := p.Submit(NewTaskBuilder[int]().
		WithTaskFunc(func(context.Context, int) {
			panic("boom")
		}).
		WithRecover(func(int, any) {
			recovered.Add(1)
		}).
		BuildTask(ints(3)))
	p.Wait(f)
	waitCounterSettled(t, p.Counter(), 5*time.Second)

	if got := recovered.Load(); got != 3 {
		t.Fatalf("recover called %d times, want 3", got)
	}
	if got := p.Counter().Completed(); got != 3 {
		t.Fatalf("Completed() = %d, want 3", got)
	}
}

func TestMultipleSubmitWait(t *testing.T) {
	p := NewConcurrentPool[int](4)
	defer p.Stop()

	var total atomic.Int64
	builder := NewTaskBuilder[int]().WithTaskFunc(func(context.Context, int) {
		total.Add(1)
	})

	f1 := p.Submit(builder.BuildTask(ints(5)))
	f2 := p.Submit(builder.BuildTask(ints(7)))
	p.Wait(f1, f2)
	waitCounterSettled(t, p.Counter(), 5*time.Second)

	if got := total.Load(); got != 12 {
		t.Fatalf("total runs %d, want 12", got)
	}
}

func TestBuildTasks(t *testing.T) {
	tasks := NewTaskBuilder[int]().
		WithTaskFunc(func(context.Context, int) {}).
		BuildTasks(ints(2), ints(3))
	if len(tasks) != 2 {
		t.Fatalf("len(tasks) = %d, want 2", len(tasks))
	}
	if len(tasks[0].param) != 2 || len(tasks[1].param) != 3 {
		t.Fatalf("unexpected param lengths: %d, %d", len(tasks[0].param), len(tasks[1].param))
	}
}
