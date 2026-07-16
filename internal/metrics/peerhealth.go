package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// PeerUnhealthyTransitionsTotal counts healthy→unhealthy transitions of the
// DataHub peer-health breaker, per peer host. This is the visibility the
// 2026-07-15 dev-ovh-1 incident lacked: the breaker silently opened on
// mis-attributed failures (caller cancellations, stale-announcement 404s)
// and, in a single-peer topology, ack-and-dropped 100% of subtree
// announcements with nothing on a dashboard to say why.
var PeerUnhealthyTransitionsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "merkle_datahub_peer_unhealthy_transitions_total",
		Help: "Healthy-to-unhealthy transitions of the DataHub peer-health breaker, per peer host.",
	},
	[]string{labelPeerHost},
)

// PeerHealthyGauge reports the current DataHub peer-health breaker state per
// peer host: 1 healthy, 0 unhealthy (in its cooldown window). Set on first
// sight of a peer and on every transition, including lazy cooldown-expiry
// recovery.
var PeerHealthyGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "merkle_datahub_peer_healthy",
		Help: "Whether a DataHub peer is currently considered healthy (1) or in its unhealthy cooldown (0).",
	},
	[]string{labelPeerHost},
)

func init() {
	Registry.MustRegister(
		PeerUnhealthyTransitionsTotal,
		PeerHealthyGauge,
	)
}

// IncPeerUnhealthyTransition records one healthy→unhealthy breaker
// transition for the peer identified by peerURL (labeled by host only, per
// the registry cardinality policy).
func IncPeerUnhealthyTransition(peerURL string) {
	PeerUnhealthyTransitionsTotal.WithLabelValues(HostLabel(peerURL)).Inc()
}

// SetPeerHealthy sets the per-peer health gauge for the peer identified by
// peerURL (labeled by host only, per the registry cardinality policy).
func SetPeerHealthy(peerURL string, healthy bool) {
	v := 0.0
	if healthy {
		v = 1.0
	}
	PeerHealthyGauge.WithLabelValues(HostLabel(peerURL)).Set(v)
}
