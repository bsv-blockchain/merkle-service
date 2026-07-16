package datahub

import (
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/metrics"
)

// DefaultPeerHealthFailureThreshold is the number of consecutive failures
// against a single DataHub peer before it is marked unhealthy.
const DefaultPeerHealthFailureThreshold = 3

// DefaultPeerHealthCooldown is the default duration a peer stays marked
// unhealthy after crossing the failure threshold. Cooldown expiry is
// checked lazily on the next IsHealthy call.
const DefaultPeerHealthCooldown = 5 * time.Minute

// DefaultStale404Grace is the default announcement age beyond which a 404
// on a subtree fetch is attributed to our own consumer lag rather than the
// announcing peer (config key datahub.peerhealth.stale404GraceSec).
// Teranode's asset cache retains subtree data for a bounded window (~2h);
// an announcement that sat in Kafka for over an hour can legitimately 404
// on an honest peer, so such a 404 must not feed the peer-health breaker.
const DefaultStale404Grace = time.Hour

// PeerHealth tracks consecutive-failure state per DataHub peer so call
// sites (block-processor metadata fetch, /reprocess probe loop) can skip
// hosts that are persistently failing.
//
// State is in-memory per process: there is no cross-pod sharing. A peer
// is keyed on its URL host (lowercased, including port) so two URLs
// pointing at the same host share a counter and a "this host is dead"
// signal applies to every block/subtree path on that host.
//
// Cooldown is enforced lazily: once a peer crosses Threshold consecutive
// failures it is marked unhealthy until Now+Cooldown, after which the
// next RecordSuccess or IsHealthy-after-cooldown clears the unhealthy
// flag. RecordSuccess always resets the consecutive-failure counter so a
// transient blip does not snowball into an unhealthy mark.
//
// A nil *PeerHealth is treated as "no tracking": IsHealthy returns true
// for any URL and RecordFailure/RecordSuccess are no-ops, so callers can
// skip a nil check.
type PeerHealth struct {
	threshold int
	cooldown  time.Duration
	now       func() time.Time

	mu    sync.Mutex
	state map[string]*peerState
}

type peerState struct {
	consecutiveFailures int
	unhealthyUntil      time.Time
}

// NewPeerHealth constructs a PeerHealth tracker. threshold <= 0 selects
// DefaultPeerHealthFailureThreshold; cooldown <= 0 selects
// DefaultPeerHealthCooldown.
func NewPeerHealth(threshold int, cooldown time.Duration) *PeerHealth {
	if threshold <= 0 {
		threshold = DefaultPeerHealthFailureThreshold
	}
	if cooldown <= 0 {
		cooldown = DefaultPeerHealthCooldown
	}
	return &PeerHealth{
		threshold: threshold,
		cooldown:  cooldown,
		now:       time.Now,
		state:     make(map[string]*peerState),
	}
}

// Threshold returns the consecutive-failure threshold. Zero for a nil
// receiver. Exposed so trip-logging call sites can include the breaker
// parameters in their WARN line.
func (p *PeerHealth) Threshold() int {
	if p == nil {
		return 0
	}
	return p.threshold
}

// Cooldown returns how long a peer stays unhealthy once tripped. Zero for
// a nil receiver.
func (p *PeerHealth) Cooldown() time.Duration {
	if p == nil {
		return 0
	}
	return p.cooldown
}

// IsHealthy reports whether rawURL's host is currently considered healthy.
// An unknown peer is healthy by default. An unhealthy entry whose cooldown
// has elapsed is auto-cleared and reported healthy. The per-peer health
// gauge is refreshed on every call so first sight and lazy cooldown-expiry
// recovery are both visible in metrics.
func (p *PeerHealth) IsHealthy(rawURL string) bool {
	if p == nil {
		return true
	}
	key := peerKey(rawURL)
	if key == "" {
		return true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	st, ok := p.state[key]
	if !ok {
		metrics.SetPeerHealthy(rawURL, true)
		return true
	}
	if st.unhealthyUntil.IsZero() {
		metrics.SetPeerHealthy(rawURL, true)
		return true
	}
	if !p.now().Before(st.unhealthyUntil) {
		st.unhealthyUntil = time.Time{}
		st.consecutiveFailures = 0
		metrics.SetPeerHealthy(rawURL, true)
		return true
	}
	metrics.SetPeerHealthy(rawURL, false)
	return false
}

// RecordFailure increments rawURL's consecutive-failure counter and marks
// the peer unhealthy once Threshold is reached. It reports whether THIS
// call transitioned the peer from healthy to unhealthy — true exactly once
// per breaker opening (a peer whose cooldown has lapsed counts as healthy
// again, so re-tripping after expiry reports a new transition). Callers
// use the signal to WARN-log breaker openings; the transition counter and
// health gauge are updated here so every caller is covered.
func (p *PeerHealth) RecordFailure(rawURL string) bool {
	if p == nil {
		return false
	}
	key := peerKey(rawURL)
	if key == "" {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	st, ok := p.state[key]
	if !ok {
		st = &peerState{}
		p.state[key] = st
	}
	now := p.now()
	wasUnhealthy := !st.unhealthyUntil.IsZero() && now.Before(st.unhealthyUntil)
	st.consecutiveFailures++
	tripped := false
	if st.consecutiveFailures >= p.threshold {
		st.unhealthyUntil = now.Add(p.cooldown)
		tripped = !wasUnhealthy
	}
	if tripped {
		metrics.IncPeerUnhealthyTransition(rawURL)
	}
	stillHealthy := st.unhealthyUntil.IsZero() || !now.Before(st.unhealthyUntil)
	metrics.SetPeerHealthy(rawURL, stillHealthy)
	return tripped
}

// RecordSuccess clears any unhealthy flag and resets the consecutive
// failure counter for rawURL, and refreshes the health gauge (first sight
// of a peer via a successful fetch also registers it as healthy).
func (p *PeerHealth) RecordSuccess(rawURL string) {
	if p == nil {
		return
	}
	key := peerKey(rawURL)
	if key == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	metrics.SetPeerHealthy(rawURL, true)
	st, ok := p.state[key]
	if !ok {
		return
	}
	st.consecutiveFailures = 0
	st.unhealthyUntil = time.Time{}
}

// peerKey extracts the host (including port if present) from rawURL,
// lowercased. Returns "" if rawURL is not a parseable URL with a host —
// in that case callers treat the URL as un-trackable rather than
// bucketing every unparseable URL into a single shared key.
func peerKey(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return ""
	}
	return strings.ToLower(u.Host)
}
