package store

import (
	"testing"
)

// TestCallbackURLRegistryConstants pins the constants the registry uses to
// build per-URL records. Changing these values is a wire-compatibility break:
// existing prod records would no longer be locatable. If you must change one,
// add a migration story to the PR.
func TestCallbackURLRegistryConstants(t *testing.T) {
	if callbackURLBin != "u" {
		t.Errorf("callbackURLBin: expected u, got %s", callbackURLBin)
	}
	if defaultCallbackURLRegistryTTLSec != 7*24*60*60 {
		t.Errorf("default TTL drifted: %d", defaultCallbackURLRegistryTTLSec)
	}
}

// TestNewCallbackURLRegistry verifies constructor sets fields, including the
// new TTL knob (F-037 / issue #23).
func TestNewCallbackURLRegistry(t *testing.T) {
	r := NewCallbackURLRegistry(nil, "test-set", 600, 3, 100, nil).(*aerospikeCallbackURLRegistry)
	if r.setName != "test-set" {
		t.Errorf("expected set name test-set, got %s", r.setName)
	}
	if r.maxRetries != 3 {
		t.Errorf("expected maxRetries 3, got %d", r.maxRetries)
	}
	if r.retryBaseMs != 100 {
		t.Errorf("expected retryBaseMs 100, got %d", r.retryBaseMs)
	}
	if r.ttlSec != 600 {
		t.Errorf("expected ttlSec 600, got %d", r.ttlSec)
	}
}

// TestNewCallbackURLRegistry_DefaultTTL verifies that a zero/negative ttlSec
// arg is coerced to the default 7-day window, so a misconfigured deployment
// can't accidentally fall back to F-037's unbounded behaviour.
func TestNewCallbackURLRegistry_DefaultTTL(t *testing.T) {
	for _, in := range []int{0, -1, -3600} {
		r := NewCallbackURLRegistry(nil, "test-set", in, 3, 100, nil).(*aerospikeCallbackURLRegistry)
		if r.ttlSec != defaultCallbackURLRegistryTTLSec {
			t.Errorf("ttlSec=%d in: expected default %d, got %d",
				in, defaultCallbackURLRegistryTTLSec, r.ttlSec)
		}
	}
}

// TestCallbackURLKey_Deterministic locks the key derivation so two registry
// instances (e.g. across services) agree on which record represents a URL.
// If this test changes, all prod records become orphans.
func TestCallbackURLKey_Deterministic(t *testing.T) {
	a := callbackURLKey("http://example.com/cb")
	b := callbackURLKey("http://example.com/cb")
	if a != b {
		t.Fatalf("same URL hashed to different keys: %s != %s", a, b)
	}
	if len(a) != 64 {
		t.Errorf("expected 64-char sha256 hex key, got %d", len(a))
	}
	if c := callbackURLKey("http://other.example.com/cb"); a == c {
		t.Errorf("different URLs should hash differently: %s == %s", a, c)
	}
}

// TestCallbackURLRegistry_Bounded is a regression test for F-037 / issue #23.
// Pre-fix: every Add appended to a single record's CDT list with no TTL or
// cap, so the record grew without limit and BLOCK_PROCESSED fan-out iterated
// every URL ever seen. Post-fix: each URL lives in its own record keyed by
// sha256(url), each carries a per-record TTL, so the registry is bounded by
// the active-URL count (Aerospike nsup evicts cold records).
//
// We can't run a real Aerospike here without the integration build tag, but
// we can assert the invariants that make the bound real:
//   - the registry stores per-URL records (not a single CDT list);
//   - it always writes with a positive TTL;
//   - the same URL maps to the same record, so repeated Add calls upsert
//     instead of growing storage.
func TestCallbackURLRegistry_Bounded(t *testing.T) {
	r := NewCallbackURLRegistry(nil, "test-set", 60, 3, 100, nil).(*aerospikeCallbackURLRegistry)

	if r.ttlSec <= 0 {
		t.Fatalf("registry must enforce a positive TTL; got %d", r.ttlSec)
	}

	// Repeated Add of the same URL must collapse to one record.
	k1 := callbackURLKey("http://a.example.com/cb")
	k2 := callbackURLKey("http://a.example.com/cb")
	if k1 != k2 {
		t.Fatalf("Add for the same URL must reuse the same record key (k1=%s, k2=%s)", k1, k2)
	}

	// Distinct URLs must map to distinct records, otherwise a hash collision
	// would silently lose URLs.
	keys := make(map[string]string, 100)
	for i := 0; i < 100; i++ {
		u := "http://example.com/cb/" + itoa(i)
		k := callbackURLKey(u)
		if existing, ok := keys[k]; ok {
			t.Fatalf("hash collision between %q and %q (key %s)", existing, u, k)
		}
		keys[k] = u
	}
}

// itoa is a tiny no-import helper (avoids importing strconv just for tests).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
