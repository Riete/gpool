package conrate

import (
	"testing"
	"time"
)

// TestRateLimiterCapacity expects Capacity() to return 42 after NewRateLimiter(42).
func TestRateLimiterCapacity(t *testing.T) {
	l := NewRateLimiter(42)
	defer l.Stop()

	if got := l.Capacity(); got != 42 {
		t.Fatalf("Capacity() = %d, want 42", got)
	}
}

// TestRateLimiterDispatchesTokens expects at least one token within 2s after the limiter starts.
func TestRateLimiterDispatchesTokens(t *testing.T) {
	l := NewRateLimiter(100)
	defer l.Stop()

	select {
	case <-l.Wait():
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for token")
	}
}

// TestRateLimiterPauseResume expects:
//   - after Pause: IsPaused()==true and no token within 200ms;
//   - after Resume: IsPaused()==false and a token within 2s.
func TestRateLimiterPauseResume(t *testing.T) {
	l := NewRateLimiter(100)
	defer l.Stop()

	l.Pause()
	if !l.IsPaused() {
		t.Fatal("expected paused after Pause()")
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-l.Wait():
			t.Error("received token while paused")
		case <-time.After(200 * time.Millisecond):
		}
		close(done)
	}()
	<-done

	l.Resume()
	if l.IsPaused() {
		t.Fatal("expected not paused after Resume()")
	}

	select {
	case <-l.Wait():
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for token after Resume()")
	}
}

// TestRateLimiterSetCapacityWhilePaused expects:
//   - SetCapacity while paused updates the field only; no tokens while paused;
//   - after Resume, tokens dispatch at the new capacity.
func TestRateLimiterSetCapacityWhilePaused(t *testing.T) {
	l := NewRateLimiter(10)
	defer l.Stop()

	l.Pause()
	l.SetCapacity(20)
	if got := l.Capacity(); got != 20 {
		t.Fatalf("Capacity() = %d, want 20", got)
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-l.Wait():
			t.Error("received token while paused")
		case <-time.After(200 * time.Millisecond):
		}
		close(done)
	}()
	<-done

	l.Resume()
	select {
	case <-l.Wait():
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for token after Resume() with new capacity")
	}
}

// TestRateLimiterStop expects:
//   - after Stop: IsStopped()==true and IsPaused()==true;
//   - Stopped() channel closed;
//   - repeated Stop does not panic.
func TestRateLimiterStop(t *testing.T) {
	l := NewRateLimiter(100)

	select {
	case <-l.Wait():
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for token before Stop()")
	}

	l.Stop()
	if !l.IsStopped() {
		t.Fatal("expected stopped after Stop()")
	}
	if !l.IsPaused() {
		t.Fatal("expected paused flag after Stop()")
	}

	select {
	case _, ok := <-l.Stopped():
		if ok {
			t.Fatal("expected closed stop channel")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for stopped channel close")
	}

	l.Stop() // idempotent
}

// TestRateLimiterResumeAfterStopNoOp expects Resume after Stop to be a no-op; IsStopped() remains true.
func TestRateLimiterResumeAfterStopNoOp(t *testing.T) {
	l := NewRateLimiter(100)
	l.Stop()

	l.Resume()
	if !l.IsStopped() {
		t.Fatal("expected still stopped after Resume()")
	}
}
