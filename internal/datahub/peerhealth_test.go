package datahub

import (
	"sync"
	"testing"
	"time"
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
