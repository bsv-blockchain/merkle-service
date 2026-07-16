package datahub

import (
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bsv-blockchain/merkle-service/internal/metrics"
)

func TestPeerHealth_HealthyByDefault(t *testing.T) {
	p := NewPeerHealth(3, time.Minute)
	if !p.IsHealthy("https://example.com/api") {
		t.Fatal("unknown peer should be healthy")
	}
}

func TestPeerHealth_MarksUnhealthyAtThreshold(t *testing.T) {
	p := NewPeerHealth(3, time.Minute)
	url := "https://bad.example.com/api"

	p.RecordFailure(url)
	p.RecordFailure(url)
	if !p.IsHealthy(url) {
		t.Fatalf("peer should still be healthy after %d failures", 2)
	}
	p.RecordFailure(url)
	if p.IsHealthy(url) {
		t.Fatal("peer should be unhealthy at threshold")
	}
}

func TestPeerHealth_SuccessResetsCounter(t *testing.T) {
	p := NewPeerHealth(3, time.Minute)
	url := "https://flap.example.com/api"

	p.RecordFailure(url)
	p.RecordFailure(url)
	p.RecordSuccess(url)
	p.RecordFailure(url)
	p.RecordFailure(url)
	if !p.IsHealthy(url) {
		t.Fatal("success should reset the failure counter so two more failures do not trip")
	}
}

func TestPeerHealth_CooldownExpiry(t *testing.T) {
	p := NewPeerHealth(2, time.Minute)
	now := time.Unix(1_700_000_000, 0)
	p.now = func() time.Time { return now }
	url := "https://dead.example.com/api"

	p.RecordFailure(url)
	p.RecordFailure(url)
	if p.IsHealthy(url) {
		t.Fatal("peer should be unhealthy after threshold")
	}

	now = now.Add(2 * time.Minute)
	if !p.IsHealthy(url) {
		t.Fatal("peer should recover after cooldown")
	}

	// Counter should be reset by the auto-recovery — a single new failure
	// should not immediately re-trip the unhealthy mark.
	p.RecordFailure(url)
	if !p.IsHealthy(url) {
		t.Fatal("peer should still be healthy with only one failure after recovery")
	}
}

func TestPeerHealth_HostKeyedAcrossPaths(t *testing.T) {
	p := NewPeerHealth(2, time.Minute)
	p.RecordFailure("https://h.example.com/block/abc")
	p.RecordFailure("https://h.example.com/subtree/xyz")
	if p.IsHealthy("https://h.example.com/something/else") {
		t.Fatal("failures on different paths of same host should aggregate")
	}
}

func TestPeerHealth_DifferentHostsIndependent(t *testing.T) {
	p := NewPeerHealth(2, time.Minute)
	p.RecordFailure("https://a.example.com/x")
	p.RecordFailure("https://a.example.com/x")
	if p.IsHealthy("https://a.example.com/x") {
		t.Fatal("host a should be unhealthy after threshold")
	}
	if !p.IsHealthy("https://b.example.com/x") {
		t.Fatal("unrelated host should not be affected by another's failures")
	}
}

func TestPeerHealth_UnparseableURLIsTreatedAsHealthy(t *testing.T) {
	p := NewPeerHealth(1, time.Minute)
	// peerKey returns "" for both, so neither is tracked.
	p.RecordFailure("not a url")
	if !p.IsHealthy("also not a url") {
		t.Fatal("unparseable URL should be reported healthy (un-trackable)")
	}
}

func TestPeerHealth_HostKeyIsCaseInsensitive(t *testing.T) {
	p := NewPeerHealth(2, time.Minute)
	p.RecordFailure("https://Example.COM/x")
	p.RecordFailure("https://example.com/y")
	if p.IsHealthy("https://EXAMPLE.com/z") {
		t.Fatal("host key should be case-insensitive")
	}
}

func TestPeerHealth_NilReceiverIsSafe(t *testing.T) {
	var p *PeerHealth
	// Should not panic.
	p.RecordFailure("https://x.example.com/")
	p.RecordSuccess("https://x.example.com/")
	if !p.IsHealthy("https://x.example.com/") {
		t.Fatal("nil PeerHealth should report all peers healthy")
	}
}

// TestPeerHealth_RecordFailureReportsTripExactlyOnce pins the breaker-trip
// signal call sites use for WARN logging: RecordFailure returns true exactly
// once per healthy→unhealthy transition — not before the threshold, and not
// again while the breaker is already open.
func TestPeerHealth_RecordFailureReportsTripExactlyOnce(t *testing.T) {
	p := NewPeerHealth(3, time.Minute)
	url := "https://trip.example.com/api"

	if p.RecordFailure(url) {
		t.Fatal("failure 1 of 3 must not report a trip")
	}
	if p.RecordFailure(url) {
		t.Fatal("failure 2 of 3 must not report a trip")
	}
	if !p.RecordFailure(url) {
		t.Fatal("failure 3 of 3 must report the healthy→unhealthy transition")
	}
	if p.RecordFailure(url) {
		t.Fatal("failure while already unhealthy must not report a second trip")
	}

	// Recovery and a fresh run of failures reports a fresh trip.
	p.RecordSuccess(url)
	p.RecordFailure(url)
	p.RecordFailure(url)
	if !p.RecordFailure(url) {
		t.Fatal("re-tripping after a success-reset must report the transition again")
	}
}

// TestPeerHealth_RecordFailureReportsTripAfterCooldownExpiry covers the lazy
// recovery path: once the cooldown has elapsed the peer is healthy again, so
// the next threshold-crossing failure is a new healthy→unhealthy transition.
func TestPeerHealth_RecordFailureReportsTripAfterCooldownExpiry(t *testing.T) {
	p := NewPeerHealth(2, time.Minute)
	now := time.Unix(1_700_000_000, 0)
	p.now = func() time.Time { return now }
	url := "https://retrip.example.com/api"

	p.RecordFailure(url)
	if !p.RecordFailure(url) {
		t.Fatal("threshold crossing must report a trip")
	}

	// Cooldown elapses; the peer is healthy again even without an IsHealthy
	// call having cleared the state.
	now = now.Add(2 * time.Minute)
	if !p.RecordFailure(url) {
		t.Fatal("first re-trip after cooldown expiry must report a new transition")
	}
	if p.RecordFailure(url) {
		t.Fatal("failure while the re-opened breaker is unexpired must not report a trip")
	}
}

// TestPeerHealth_ThresholdCooldownAccessors pins the getters trip-logging
// call sites use to include breaker parameters in the WARN line.
func TestPeerHealth_ThresholdCooldownAccessors(t *testing.T) {
	p := NewPeerHealth(4, 7*time.Minute)
	if p.Threshold() != 4 {
		t.Errorf("Threshold: expected 4, got %d", p.Threshold())
	}
	if p.Cooldown() != 7*time.Minute {
		t.Errorf("Cooldown: expected 7m, got %s", p.Cooldown())
	}
}

// TestPeerHealth_NilRecordFailureReturnsFalse extends the nil-receiver
// contract to the trip signal.
func TestPeerHealth_NilRecordFailureReturnsFalse(t *testing.T) {
	var p *PeerHealth
	if p.RecordFailure("https://x.example.com/") {
		t.Fatal("nil PeerHealth must never report a trip")
	}
}

// TestPeerHealth_BreakerMetrics verifies the observability surface added for
// the 2026-07-15 breaker-poisoning incident: a transitions counter that
// increments once per healthy→unhealthy flip, and a per-peer health gauge
// that tracks state through trip, success recovery, and cooldown-expiry
// recovery (both inside IsHealthy and inside RecordFailure's lazy check).
func TestPeerHealth_BreakerMetrics(t *testing.T) {
	p := NewPeerHealth(2, time.Minute)
	now := time.Unix(1_700_000_000, 0)
	p.now = func() time.Time { return now }

	// Unique host per test run concern: this test owns this label.
	url := "https://metrics-peer.example.com/api"
	const host = "metrics-peer.example.com"

	transitions := func() float64 {
		return testutil.ToFloat64(metrics.PeerUnhealthyTransitionsTotal.WithLabelValues(host))
	}
	gauge := func() float64 {
		return testutil.ToFloat64(metrics.PeerHealthyGauge.WithLabelValues(host))
	}

	// First sight via IsHealthy: gauge exists and reads healthy.
	if !p.IsHealthy(url) {
		t.Fatal("unknown peer should be healthy")
	}
	if gauge() != 1 {
		t.Errorf("gauge after first sight: expected 1, got %v", gauge())
	}

	base := transitions()

	// One failure below threshold: still healthy.
	p.RecordFailure(url)
	if gauge() != 1 {
		t.Errorf("gauge below threshold: expected 1, got %v", gauge())
	}
	if transitions() != base {
		t.Errorf("transitions below threshold: expected %v, got %v", base, transitions())
	}

	// Trip.
	p.RecordFailure(url)
	if gauge() != 0 {
		t.Errorf("gauge after trip: expected 0, got %v", gauge())
	}
	if transitions() != base+1 {
		t.Errorf("transitions after trip: expected %v, got %v", base+1, transitions())
	}

	// Additional failure while open: no new transition.
	p.RecordFailure(url)
	if transitions() != base+1 {
		t.Errorf("transitions while open: expected %v, got %v", base+1, transitions())
	}

	// Recovery via RecordSuccess.
	p.RecordSuccess(url)
	if gauge() != 1 {
		t.Errorf("gauge after success recovery: expected 1, got %v", gauge())
	}

	// Trip again, then recover via cooldown expiry observed by IsHealthy.
	p.RecordFailure(url)
	p.RecordFailure(url)
	if transitions() != base+2 {
		t.Errorf("transitions after second trip: expected %v, got %v", base+2, transitions())
	}
	if gauge() != 0 {
		t.Errorf("gauge after second trip: expected 0, got %v", gauge())
	}
	now = now.Add(2 * time.Minute)
	if !p.IsHealthy(url) {
		t.Fatal("peer should recover after cooldown")
	}
	if gauge() != 1 {
		t.Errorf("gauge after cooldown-expiry recovery: expected 1, got %v", gauge())
	}
}

func TestPeerHealth_ConcurrentAccess(t *testing.T) {
	p := NewPeerHealth(50, time.Minute)
	url := "https://race.example.com/api"
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				p.RecordFailure(url)
				_ = p.IsHealthy(url)
				p.RecordSuccess(url)
			}
		}()
	}
	wg.Wait()
}
