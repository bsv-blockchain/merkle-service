package sql

import (
	"testing"
	"time"
)

func registryHasURL(t *testing.T, r *callbackURLRegistry, url string) bool {
	t.Helper()
	all, err := r.GetAll()
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	for _, e := range all {
		if e.URL == url {
			return true
		}
	}
	return false
}

// WS4: RecordFailure trips the breaker at the threshold, GetAll then excludes
// the URL, and a fresh Add re-enables it (resetting the counter).
func TestCallbackURLRegistry_BreakerDisablesAndReenables(t *testing.T) {
	db, d := newTestDB(t)
	r := newCallbackURLRegistry(db, d, time.Hour)

	const url = "http://flaky"
	if err := r.Add(url, "tok"); err != nil {
		t.Fatal(err)
	}

	// Failures below the threshold leave the URL enabled.
	for i := 1; i < 3; i++ {
		disabled, err := r.RecordFailure(url, 3)
		if err != nil {
			t.Fatalf("RecordFailure %d: %v", i, err)
		}
		if disabled {
			t.Fatalf("URL disabled too early (failure %d of threshold 3)", i)
		}
	}
	if !registryHasURL(t, r, url) {
		t.Fatal("URL should still appear in GetAll before the threshold")
	}

	// Reaching the threshold disables it and drops it from GetAll.
	disabled, err := r.RecordFailure(url, 3)
	if err != nil {
		t.Fatalf("RecordFailure at threshold: %v", err)
	}
	if !disabled {
		t.Fatal("expected URL to be disabled at the threshold")
	}
	if registryHasURL(t, r, url) {
		t.Fatal("a disabled URL must not appear in GetAll")
	}

	// Re-registration clears the breaker (URL re-enabled, counter reset).
	if err = r.Add(url, "tok"); err != nil {
		t.Fatal(err)
	}
	if !registryHasURL(t, r, url) {
		t.Fatal("re-registered URL should appear in GetAll again")
	}
	disabled, err = r.RecordFailure(url, 3)
	if err != nil {
		t.Fatalf("RecordFailure after re-register: %v", err)
	}
	if disabled {
		t.Fatal("breaker should have reset on re-registration (one failure must not re-trip threshold 3)")
	}
}

// WS4: a non-positive threshold or an unknown URL is a no-op.
func TestCallbackURLRegistry_RecordFailureNoOp(t *testing.T) {
	db, d := newTestDB(t)
	r := newCallbackURLRegistry(db, d, time.Hour)

	if err := r.Add("http://known", "tok"); err != nil {
		t.Fatal(err)
	}
	if disabled, err := r.RecordFailure("http://known", 0); err != nil || disabled {
		t.Fatalf("threshold 0 must be a no-op: disabled=%v err=%v", disabled, err)
	}
	if disabled, err := r.RecordFailure("http://unknown", 3); err != nil || disabled {
		t.Fatalf("unknown URL must be a no-op: disabled=%v err=%v", disabled, err)
	}
}
