package conrate

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

// waitUntil polls fn until it returns true or timeout elapses.
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

// waitCounterSettled expects Pending()==0 and Running()==0.
func waitCounterSettled(t *testing.T, c *Counter, timeout time.Duration) {
	t.Helper()
	waitUntil(t, timeout, func() bool {
		return c.Pending() == 0 && c.Running() == 0
	})
}

// assertCounterZeroPending expects Pending()==0 and Running()==0.
func assertCounterZeroPending(t *testing.T, c *Counter) {
	t.Helper()
	if p := c.Pending(); p != 0 {
		t.Fatalf("Pending() = %d, want 0", p)
	}
	if r := c.Running(); r != 0 {
		t.Fatalf("Running() = %d, want 0", r)
	}
}

// TestSubmitAfterStop expects Submit after Stop to return Future.Error()==ErrExecutorStopped; Wait must not block.
func TestSubmitAfterStop(t *testing.T) {
	p := NewConcurrentExecutor[int](4)
	p.Stop()

	f := p.Submit(NewTaskBuilder[int]().WithTaskFunc(func(context.Context, int) {}).BuildTask(ints(1)))
	if !errors.Is(f.Error(), ErrExecutorStopped) {
		t.Fatalf("Error() = %v, want ErrExecutorStopped", f.Error())
	}
	f.Wait()
}

// TestDoubleStop expects two consecutive Stop calls not to panic and IsStopped()==true.
func TestDoubleStop(t *testing.T) {
	p := NewConcurrentExecutor[int](4)
	p.Stop()
	p.Stop()
	if !p.IsStopped() {
		t.Fatal("expected executor stopped")
	}
}

// TestConcurrentExecutorUnlimitedCompletesAll expects:
//   - Concurrency mode with no task-level maxConcurrency: all n params run;
//   - Pending/Running return to zero and Completed()==n.
func TestConcurrentExecutorUnlimitedCompletesAll(t *testing.T) {
	p := NewConcurrentExecutor[int](4)
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

// TestConcurrentExecutorLimitedTaskConcurrency expects:
//   - WithMaxConcurrency(3): at most 3 params running at once;
//   - Completed()==n after all finish.
func TestConcurrentExecutorLimitedTaskConcurrency(t *testing.T) {
	p := NewConcurrentExecutor[int](8)
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

// TestConcurrentExecutorCapacity expects executor capacity=2 to cap concurrency params at 2.
func TestConcurrentExecutorCapacity(t *testing.T) {
	p := NewConcurrentExecutor[int](2)
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

// TestRateLimitExecutorUnlimitedCompletesAll expects rate-limit mode without task-level limit to run all n params.
func TestRateLimitExecutorUnlimitedCompletesAll(t *testing.T) {
	p := NewRateLimitExecutor[int](200)
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

// TestRateLimitExecutorLimitedTaskConcurrency expects:
//   - rate-limit mode with WithMaxConcurrency(5): all n params complete via limitedRateLimitRun.
func TestRateLimitExecutorLimitedTaskConcurrency(t *testing.T) {
	p := NewRateLimitExecutor[int](100)
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

// TestFutureCancel expects:
//   - params not yet run after Cancel count toward Canceled;
//   - completed + canceled == n, Pending/Running zero, and canceled > 0.
func TestFutureCancel(t *testing.T) {
	p := NewConcurrentExecutor[int](8)
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

// TestCounterAccountingAfterComplete expects Pending==0, Completed==n, and Canceled==0 after normal completion.
func TestCounterAccountingAfterComplete(t *testing.T) {
	p := NewConcurrentExecutor[int](4)
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

// TestPauseResumeConcurrentExecutor expects:
//   - Pause/Resume to toggle IsPaused correctly;
//   - all tasks to finish after Resume with Completed()==n.
func TestPauseResumeConcurrentExecutor(t *testing.T) {
	p := NewConcurrentExecutor[int](10)
	defer p.Stop()

	const n = 30
	f := p.Submit(NewTaskBuilder[int]().WithTaskFunc(func(context.Context, int) {
		time.Sleep(30 * time.Millisecond)
	}).BuildTask(ints(n)))

	time.Sleep(80 * time.Millisecond)
	p.Pause()
	if !p.IsPaused() {
		t.Fatal("expected executor paused")
	}

	p.Resume()
	if p.IsPaused() {
		t.Fatal("expected executor resumed")
	}

	p.Wait(f)
	waitCounterSettled(t, p.Counter(), 10*time.Second)
	if got := p.Counter().Completed(); got != n {
		t.Fatalf("Completed() = %d, want %d", got, n)
	}
}

// TestStopWhilePaused expects Stop() while paused to return within 3s, not block forever on a paused limiter.
func TestStopWhilePaused(t *testing.T) {
	p := NewConcurrentExecutor[int](4)

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
		t.Fatal("Stop() blocked while executor paused")
	}
	close(block)
	p.Wait(f)
}

// TestGracefulStop expects:
//   - GracefulStop with inflight work to finish after tasks complete;
//   - IsStopped()==true and ran==n.
func TestGracefulStop(t *testing.T) {
	p := NewConcurrentExecutor[int](4)

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
		t.Fatal("expected executo stopped after GracefulStop()")
	}
	if got := ran.Load(); got != n {
		t.Fatalf("ran %d items, want %d", got, n)
	}
}

// TestTaskPanicRecover expects:
//   - WithRecover invoked once per panicking param (3 times total);
//   - panics still count as Completed; Pending/Running return to zero.
func TestTaskPanicRecover(t *testing.T) {
	p := NewConcurrentExecutor[int](2)
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

// TestMultipleSubmitWait expects both submits (5 + 7 params) to run for a total of 12 executions.
func TestMultipleSubmitWait(t *testing.T) {
	p := NewConcurrentExecutor[int](4)
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

// TestBuildTasks expects BuildTasks to return 2 tasks with param lengths 2 and 3.
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

func TestWeighted(t *testing.T) {
	c1 := new(atomic.Int64)
	c2 := new(atomic.Int64)
	c3 := new(atomic.Int64)
	go func() {
		for {
			time.Sleep(time.Second)
			t.Log("c1", c1.Load(), "c2", c2.Load(), "c3", c3.Load())
		}
	}()
	task1 := NewTaskBuilder[int]().WithWeight(10).WithMaxConcurrency(10).WithTaskFunc(func(_ context.Context, _ int) {
		c1.Add(1)
		time.Sleep(time.Second)
	}).BuildTask(ints(50))
	task2 := NewTaskBuilder[int]().WithWeight(1).WithMaxConcurrency(10).WithTaskFunc(func(_ context.Context, _ int) {
		c2.Add(1)
		time.Sleep(time.Second)
	}).BuildTask(ints(50))
	task3 := NewTaskBuilder[int]().WithWeight(1).WithMaxConcurrency(10).WithTaskFunc(func(_ context.Context, _ int) {
		c3.Add(1)
		time.Sleep(time.Second)
	}).BuildTask(ints(50))
	executor := NewConcurrentExecutor[int](10)
	executor.Wait(executor.Submit(task2, task1, task3))
	time.Sleep(10 * time.Second)
}
