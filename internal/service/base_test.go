package service

import (
	"testing"
)

func TestInitBase(t *testing.T) {
	var bs BaseService
	bs.InitBase("test-service")

	if bs.Name != "test-service" {
		t.Errorf("expected name 'test-service', got %q", bs.Name)
	}
	if bs.Logger == nil {
		t.Fatal("expected logger to be set after InitBase")
	}
}

func TestContextCancel(t *testing.T) {
	var bs BaseService
	bs.InitBase("cancel-test")

	ctx := bs.Context()
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}

	// Context should not be done before cancel.
	select {
	case <-ctx.Done():
		t.Fatal("context should not be done before cancel")
	default:
		// expected
	}

	bs.Cancel()

	// Context should now be done.
	select {
	case <-ctx.Done():
		// expected
	default:
		t.Fatal("context should be done after cancel")
	}
}

func TestStartedState(t *testing.T) {
	var bs BaseService
	bs.InitBase("started-test")

	if bs.IsStarted() {
		t.Error("expected IsStarted() to return false initially")
	}

	bs.SetStarted(true)
	if !bs.IsStarted() {
		t.Error("expected IsStarted() to return true after SetStarted(true)")
	}

	bs.SetStarted(false)
	if bs.IsStarted() {
		t.Error("expected IsStarted() to return false after SetStarted(false)")
	}
}

func TestCancelNilSafe(t *testing.T) {
	// Cancel should not panic when called on zero-value BaseService.
	var bs BaseService
	bs.Cancel() // should be a no-op, not panic
}

func TestInitBase_SetsContext(t *testing.T) {
	var bs BaseService
	bs.InitBase("ctx-test")

	ctx := bs.Context()
	if ctx == nil {
		t.Fatal("expected non-nil context after InitBase")
	}
	if ctx.Err() != nil {
		t.Error("expected context to not have error after InitBase")
	}
}

func TestInitBase_MultipleInits(t *testing.T) {
	var bs BaseService

	bs.InitBase("first")
	if bs.Name != "first" {
		t.Errorf("expected name 'first', got %q", bs.Name)
	}

	// Re-init should overwrite cleanly.
	bs.InitBase("second")
	if bs.Name != "second" {
		t.Errorf("expected name 'second', got %q", bs.Name)
	}
	if bs.IsStarted() {
		t.Error("expected started=false after re-init")
	}
}
