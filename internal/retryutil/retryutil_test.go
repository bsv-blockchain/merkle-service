package retryutil

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"syscall"
	"testing"
	"time"
)

// TestBackoff_ExponentialWithCap pins the schedule shape shared by the
// subtree-fetcher (30s cap) and the subtree-worker (5s cap): base doubling
// per 1-based attempt, saturating at the caller's cap.
func TestBackoff_ExponentialWithCap(t *testing.T) {
	cases := []struct {
		name       string
		baseMs     int
		attempt    int
		maxBackoff time.Duration
		want       time.Duration
	}{
		{"attempt 1", 1000, 1, 30 * time.Second, 1 * time.Second},
		{"attempt 2", 1000, 2, 30 * time.Second, 2 * time.Second},
		{"attempt 3", 1000, 3, 30 * time.Second, 4 * time.Second},
		{"attempt 6 capped", 1000, 6, 30 * time.Second, 30 * time.Second}, // 32s capped
		{"shift overflow guard", 1000, 50, 30 * time.Second, 30 * time.Second},
		{"worker cap 5s at attempt 4", 1000, 4, 5 * time.Second, 5 * time.Second}, // 8s capped
		{"worker cap 5s below cap", 1000, 3, 5 * time.Second, 4 * time.Second},
		{"attempt < 1 treated as 1", 1000, 0, 30 * time.Second, 1 * time.Second},
	}
	for _, tc := range cases {
		if got := Backoff(tc.baseMs, tc.attempt, tc.maxBackoff); got != tc.want {
			t.Errorf("%s: Backoff(%d, %d, %v) = %v, want %v",
				tc.name, tc.baseMs, tc.attempt, tc.maxBackoff, got, tc.want)
		}
	}
}

// TestBackoff_NonPositiveBaseDisables pins the contract every struct-literal
// test config relies on: base <= 0 disables the backoff entirely.
func TestBackoff_NonPositiveBaseDisables(t *testing.T) {
	if got := Backoff(0, 3, 30*time.Second); got != 0 {
		t.Errorf("Backoff with base 0 = %v, want 0 (disabled)", got)
	}
	if got := Backoff(-1, 3, 30*time.Second); got != 0 {
		t.Errorf("Backoff with base -1 = %v, want 0 (disabled)", got)
	}
}

func TestWait_NonPositiveReturnsImmediately(t *testing.T) {
	begin := time.Now()
	if err := Wait(context.Background(), 0); err != nil {
		t.Fatalf("Wait(ctx, 0): %v", err)
	}
	if err := Wait(context.Background(), -time.Second); err != nil {
		t.Fatalf("Wait(ctx, -1s): %v", err)
	}
	if elapsed := time.Since(begin); elapsed > time.Second {
		t.Errorf("non-positive Wait took %v, want immediate return", elapsed)
	}
}

func TestWait_CompletesAfterDuration(t *testing.T) {
	begin := time.Now()
	if err := Wait(context.Background(), 30*time.Millisecond); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if elapsed := time.Since(begin); elapsed < 30*time.Millisecond {
		t.Errorf("Wait returned after %v, want >= 30ms", elapsed)
	}
}

func TestWait_AbortsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	begin := time.Now()
	err := Wait(ctx, time.Minute)
	if err == nil {
		t.Fatal("expected ctx error when the wait is interrupted by cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Wait returned %v, want context.Canceled", err)
	}
	if elapsed := time.Since(begin); elapsed > 5*time.Second {
		t.Fatalf("Wait did not abort on cancel (took %v)", elapsed)
	}
}

// TestIsDiskFull covers the full-filesystem matrix: the errno chain a blob
// store preserves, the text-only variants one that doesn't, and the negative
// cases that must keep flowing through the ordinary retry budget.
func TestIsDiskFull(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"bare ENOSPC", syscall.ENOSPC, true},
		{
			"wrapped fs.PathError ENOSPC",
			fmt.Errorf("writing blob: %w", &fs.PathError{Op: "write", Path: "/data/x", Err: syscall.ENOSPC}),
			true,
		},
		{"no-space text", errors.New("write /data/x: no space left on device"), true},
		{"quota text", errors.New("mkdir /data/y: disk quota exceeded"), true},
		{"unrelated error", errors.New("aerospike timeout"), false},
		{"unrelated errno", &fs.PathError{Op: "write", Path: "/data/x", Err: syscall.EACCES}, false},
	}
	for _, tc := range cases {
		if got := IsDiskFull(tc.err); got != tc.want {
			t.Errorf("%s: IsDiskFull(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}
