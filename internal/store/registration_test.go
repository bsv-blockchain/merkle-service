package store

import (
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// newBatchTTLTestStore returns an aerospikeRegistration whose updateTTLOne is
// stubbed by fn. The Aerospike client is left nil because BatchUpdateTTL never
// touches it once updateTTLOne is overridden — this lets us exercise the
// aggregation logic without a live cluster.
func newBatchTTLTestStore(fn func(txid string, ttl time.Duration) error) *aerospikeRegistration {
	return &aerospikeRegistration{
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		updateTTLOne: fn,
	}
}

func TestBatchUpdateTTL_EmptyReturnsNil(t *testing.T) {
	called := 0
	s := newBatchTTLTestStore(func(string, time.Duration) error {
		called++
		return nil
	})
	if err := s.BatchUpdateTTL(nil, time.Second); err != nil {
		t.Fatalf("nil txids: unexpected err: %v", err)
	}
	if err := s.BatchUpdateTTL([]string{}, time.Second); err != nil {
		t.Fatalf("empty txids: unexpected err: %v", err)
	}
	if called != 0 {
		t.Fatalf("expected 0 per-record calls, got %d", called)
	}
}

func TestBatchUpdateTTL_AllSucceedReturnsNil(t *testing.T) {
	var seen []string
	s := newBatchTTLTestStore(func(txid string, _ time.Duration) error {
		seen = append(seen, txid)
		return nil
	})
	if err := s.BatchUpdateTTL([]string{"a", "b", "c"}, time.Second); err != nil {
		t.Fatalf("expected nil err on all-success, got %v", err)
	}
	if got := strings.Join(seen, ","); got != "a,b,c" {
		t.Fatalf("expected per-record calls in order a,b,c; got %s", got)
	}
}

func TestBatchUpdateTTL_DoesNotFailFast(t *testing.T) {
	// Every per-record call fails. We must observe one call per txid (no
	// short-circuit) and the returned error must aggregate every failure.
	var calls []string
	s := newBatchTTLTestStore(func(txid string, _ time.Duration) error {
		calls = append(calls, txid)
		return errors.New("boom-" + txid)
	})

	err := s.BatchUpdateTTL([]string{"a", "b", "c"}, time.Second)
	if err == nil {
		t.Fatal("expected non-nil error when every per-record op fails")
	}
	if got := strings.Join(calls, ","); got != "a,b,c" {
		t.Fatalf("expected all 3 per-record calls (no fail-fast); got %s", got)
	}
	msg := err.Error()
	for _, want := range []string{"boom-a", "boom-b", "boom-c"} {
		if !strings.Contains(msg, want) {
			t.Errorf("aggregated error missing %q: %s", want, msg)
		}
	}
}

func TestBatchUpdateTTL_PartialFailureAggregates(t *testing.T) {
	// Only "b" fails; "a" and "c" succeed. We must still return non-nil and
	// the error must mention "b" but not the successful txids.
	sentinel := errors.New("sentinel-b-failed")
	s := newBatchTTLTestStore(func(txid string, _ time.Duration) error {
		if txid == "b" {
			return sentinel
		}
		return nil
	})

	err := s.BatchUpdateTTL([]string{"a", "b", "c"}, time.Second)
	if err == nil {
		t.Fatal("expected non-nil error when one per-record op fails")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected errors.Is(err, sentinel) to be true; err=%v", err)
	}
	if !strings.Contains(err.Error(), "b") {
		t.Fatalf("expected error to identify failing txid 'b'; err=%v", err)
	}
}

func TestBatchUpdateTTL_WrapsViaErrorsJoin(t *testing.T) {
	// Two distinct sentinels — both must remain reachable via errors.Is.
	e1 := errors.New("e1")
	e2 := errors.New("e2")
	s := newBatchTTLTestStore(func(txid string, _ time.Duration) error {
		switch txid {
		case "x":
			return e1
		case "y":
			return e2
		default:
			return nil
		}
	})

	err := s.BatchUpdateTTL([]string{"x", "y", "z"}, time.Second)
	if err == nil {
		t.Fatal("expected aggregated error")
	}
	if !errors.Is(err, e1) {
		t.Errorf("errors.Is(err, e1) = false; want true")
	}
	if !errors.Is(err, e2) {
		t.Errorf("errors.Is(err, e2) = false; want true")
	}
}
