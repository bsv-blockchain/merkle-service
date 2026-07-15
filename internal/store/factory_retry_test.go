package store

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func retryTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestNewRegistryWithRetry_RecoversFromTransientFailure is the regression
// test for the api-server crash-loop: on dev-ovh-1, a transient Aerospike
// "command execution timed out" during startup exited the process immediately
// (no retry), and k8s restart-looped every api-server pod 5-7 times. Startup
// must retry the registry build with backoff instead of failing on the first
// attempt.
func TestNewRegistryWithRetry_RecoversFromTransientFailure(t *testing.T) {
	var calls int
	build := func() (*Registry, error) {
		calls++
		if calls < 3 {
			return nil, errors.New("aerospike client: failed to connect: command execution timed out")
		}
		return &Registry{}, nil
	}

	r, err := newRegistryWithRetry(context.Background(), 5, time.Millisecond, retryTestLogger(), build)
	if err != nil {
		t.Fatalf("expected recovery after transient failures, got: %v", err)
	}
	if r == nil {
		t.Fatal("expected a registry, got nil")
	}
	if calls != 3 {
		t.Errorf("build called %d times, want 3 (two failures + one success)", calls)
	}
}

// TestNewRegistryWithRetry_ExhaustsAttempts verifies a persistent failure
// still fails startup — after the full attempt budget — and that the error
// reports the attempt count and wraps the last cause.
func TestNewRegistryWithRetry_ExhaustsAttempts(t *testing.T) {
	cause := errors.New("aerospike client: failed to connect")
	var calls int
	build := func() (*Registry, error) {
		calls++
		return nil, cause
	}

	r, err := newRegistryWithRetry(context.Background(), 4, time.Millisecond, retryTestLogger(), build)
	if err == nil {
		t.Fatalf("expected an error after exhausting attempts, got registry %v", r)
	}
	if calls != 4 {
		t.Errorf("build called %d times, want 4", calls)
	}
	if !errors.Is(err, cause) {
		t.Errorf("returned error must wrap the last build failure, got: %v", err)
	}
}

// TestNewRegistryWithRetry_AbortsOnContextCancel verifies shutdown (SIGTERM
// during a crash-loop) interrupts the backoff wait instead of holding the
// process for the remaining schedule.
func TestNewRegistryWithRetry_AbortsOnContextCancel(t *testing.T) {
	build := func() (*Registry, error) {
		return nil, errors.New("still down")
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	begin := time.Now()
	_, err := newRegistryWithRetry(ctx, 5, time.Minute, retryTestLogger(), build)
	if err == nil {
		t.Fatal("expected an error when the context is canceled mid-backoff")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error must wrap context.Canceled, got: %v", err)
	}
	if elapsed := time.Since(begin); elapsed > 5*time.Second {
		t.Fatalf("retry loop did not abort on cancel (took %v)", elapsed)
	}
}
