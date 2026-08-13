package store

import (
	"errors"
	"testing"
)

func TestRegistry_CloseJoinsCloserErrors(t *testing.T) {
	reg := &Registry{}
	errA := errors.New("close a")
	errB := errors.New("close b")
	var order []string

	reg.AddCloser(func() error {
		order = append(order, "a")
		return errA
	})
	reg.AddCloser(func() error {
		order = append(order, "b")
		return errB
	})
	reg.AddCloser(func() error {
		order = append(order, "ok")
		return nil
	})

	err := reg.Close()
	if err == nil {
		t.Fatal("expected joined closer errors, got nil")
	}
	if !errors.Is(err, errA) {
		t.Errorf("joined error missing first closer: %v", err)
	}
	if !errors.Is(err, errB) {
		t.Errorf("joined error missing second closer: %v", err)
	}

	wantOrder := []string{"ok", "b", "a"}
	if len(order) != len(wantOrder) {
		t.Fatalf("closer order %v, want %v", order, wantOrder)
	}
	for i := range wantOrder {
		if order[i] != wantOrder[i] {
			t.Fatalf("closer order %v, want %v", order, wantOrder)
		}
	}

	if err := reg.Close(); err != nil {
		t.Errorf("second Close after clearing closers: %v", err)
	}
}

func TestRegistry_CloseNilWhenNoErrors(t *testing.T) {
	reg := &Registry{}
	reg.AddCloser(nil)
	reg.AddCloser(func() error { return nil })
	if err := reg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
