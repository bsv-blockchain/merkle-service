package cache

import (
	"io"
	"log/slog"
	"testing"
)

func newTestRegistrationCache(t *testing.T) *RegistrationCache {
	t.Helper()
	rc, err := NewRegistrationCache(1, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewRegistrationCache: %v", err)
	}
	return rc
}

// validHexTxid returns a 64-character hex string parseable by hex.DecodeString.
func validHexTxid(c byte) string {
	out := make([]byte, 64)
	for i := range out {
		out[i] = c
	}
	return string(out)
}

func TestRegistrationCache_PositivePersists(t *testing.T) {
	rc := newTestRegistrationCache(t)
	txid := validHexTxid('a')

	if isReg, isCached := rc.GetCached(txid); isCached || isReg {
		t.Fatalf("expected miss for fresh txid, got isReg=%v isCached=%v", isReg, isCached)
	}

	if err := rc.SetRegistered(txid); err != nil {
		t.Fatalf("SetRegistered: %v", err)
	}

	isReg, isCached := rc.GetCached(txid)
	if !isCached {
		t.Fatal("expected positive cache hit after SetRegistered")
	}
	if !isReg {
		t.Fatal("expected isRegistered=true for positively cached txid")
	}
}

// TestRegistrationCache_NegativesAreNotCached is the F-020 regression test:
// if a txid lookup returned "not registered" (e.g. observed by the
// subtree-fetcher before /watch ever ran), the cache must NOT remember
// that result. Otherwise a later registration would be hidden behind a
// stale negative entry until cache wraparound.
//
// We exercise the property at the API surface: the cache exposes
// SetRegistered / SetMultiRegistered / Invalidate but no SetNotRegistered.
// A txid that has never been positively cached must remain a miss
// regardless of how many times the caller has previously queried it.
func TestRegistrationCache_NegativesAreNotCached(t *testing.T) {
	rc := newTestRegistrationCache(t)
	txid := validHexTxid('b')

	// Many "not found" lookups must never plant a cached negative.
	for i := 0; i < 100; i++ {
		if _, isCached := rc.GetCached(txid); isCached {
			t.Fatalf("unexpected cache hit for unregistered txid on lookup %d", i)
		}
	}

	// Caller now registers the txid externally (e.g. POST /watch).
	if err := rc.SetRegistered(txid); err != nil {
		t.Fatalf("SetRegistered: %v", err)
	}

	// A subsequent lookup must observe the registration immediately —
	// not a stale "not registered" entry.
	isReg, isCached := rc.GetCached(txid)
	if !isCached {
		t.Fatal("F-020 regression: expected cache to reflect SetRegistered after prior misses")
	}
	if !isReg {
		t.Fatal("F-020 regression: expected isRegistered=true after SetRegistered")
	}
}

// TestRegistrationCache_LookupRegisterLookup simulates the exact flow
// described in F-020: lookup-before-register → register → lookup-after-
// register must return true.
func TestRegistrationCache_LookupRegisterLookup(t *testing.T) {
	rc := newTestRegistrationCache(t)
	txid := validHexTxid('c')

	// Step 1: subtree-fetcher observes txid in mempool, looks it up,
	// finds nothing in registration store. Caller previously would
	// have called SetNotRegistered here; that method is intentionally
	// gone.
	uncached, cached := rc.FilterUncached([]string{txid})
	if len(uncached) != 1 || len(cached) != 0 {
		t.Fatalf("expected 1 uncached / 0 cached, got %d / %d", len(uncached), len(cached))
	}

	// Step 2: user registers the txid via /watch.
	if err := rc.SetRegistered(txid); err != nil {
		t.Fatalf("SetRegistered: %v", err)
	}

	// Step 3: subtree-fetcher sees the txid again, this time in a
	// later subtree. The cache must report it as positively cached.
	uncached, cached = rc.FilterUncached([]string{txid})
	if len(cached) != 1 || cached[0] != txid {
		t.Fatalf("F-020 regression: expected txid in cachedRegistered, got uncached=%v cached=%v", uncached, cached)
	}
	if len(uncached) != 0 {
		t.Fatalf("expected no uncached, got %v", uncached)
	}
}

func TestRegistrationCache_SetMultiRegistered(t *testing.T) {
	rc := newTestRegistrationCache(t)
	txids := []string{validHexTxid('1'), validHexTxid('2'), validHexTxid('3')}

	if err := rc.SetMultiRegistered(txids); err != nil {
		t.Fatalf("SetMultiRegistered: %v", err)
	}

	for _, txid := range txids {
		isReg, isCached := rc.GetCached(txid)
		if !isCached || !isReg {
			t.Errorf("expected %s to be positively cached, got isReg=%v isCached=%v", txid, isReg, isCached)
		}
	}
}

func TestRegistrationCache_InvalidateClearsEntry(t *testing.T) {
	rc := newTestRegistrationCache(t)
	txid := validHexTxid('d')

	if err := rc.SetRegistered(txid); err != nil {
		t.Fatalf("SetRegistered: %v", err)
	}
	if _, isCached := rc.GetCached(txid); !isCached {
		t.Fatal("expected positive cache hit before Invalidate")
	}

	if err := rc.Invalidate(txid); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}

	if _, isCached := rc.GetCached(txid); isCached {
		t.Fatal("expected miss after Invalidate")
	}
}

func TestRegistrationCache_InvalidateUnknownTxidIsNoop(t *testing.T) {
	rc := newTestRegistrationCache(t)
	txid := validHexTxid('e')

	// Invalidating a txid that was never cached must not error.
	if err := rc.Invalidate(txid); err != nil {
		t.Fatalf("Invalidate on missing key: %v", err)
	}
}

func TestRegistrationCache_InvalidHexTxidIsRejected(t *testing.T) {
	rc := newTestRegistrationCache(t)

	if err := rc.SetRegistered("not-hex"); err == nil {
		t.Error("SetRegistered should reject non-hex txid")
	}
	if err := rc.Invalidate("not-hex"); err == nil {
		t.Error("Invalidate should reject non-hex txid")
	}

	// GetCached returns isCached=false for an invalid txid rather than
	// erroring — preserve that contract.
	if isReg, isCached := rc.GetCached("not-hex"); isReg || isCached {
		t.Errorf("expected miss for invalid txid, got isReg=%v isCached=%v", isReg, isCached)
	}
}

func TestRegistrationCache_FilterUncachedSegregatesPositivesAndUnknowns(t *testing.T) {
	rc := newTestRegistrationCache(t)
	txReg := validHexTxid('a')
	txUnknown := validHexTxid('b')

	if err := rc.SetRegistered(txReg); err != nil {
		t.Fatalf("SetRegistered: %v", err)
	}

	uncached, cachedRegistered := rc.FilterUncached([]string{txReg, txUnknown})
	if len(uncached) != 1 || uncached[0] != txUnknown {
		t.Errorf("expected uncached=[%s], got %v", txUnknown, uncached)
	}
	if len(cachedRegistered) != 1 || cachedRegistered[0] != txReg {
		t.Errorf("expected cachedRegistered=[%s], got %v", txReg, cachedRegistered)
	}
}
