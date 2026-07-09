package metrics

import "github.com/prometheus/client_golang/prometheus"

// Announcement kind label values (bounded enum: the two teranode
// announcement types merkle-service forwards into Kafka).
const (
	AnnouncementKindSubtree = "subtree"
	AnnouncementKindBlock   = "block"
)

// P2PAnnouncementsTotal counts teranode announcements at p2p intake,
// classified by kind (subtree|block) and outcome (published|rejected_url).
// rejected_url means the announcing peer advertised a DataHub URL that
// fails SSRF/DNS validation (e.g. its cluster-internal service name) —
// the announcement is dropped before it can pollute the Kafka pipelines.
var P2PAnnouncementsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "merkle_p2p_announcements_total",
		Help: "Teranode announcements at p2p intake, by kind and outcome.",
	},
	[]string{labelKind, labelOutcome},
)

func init() {
	Registry.MustRegister(P2PAnnouncementsTotal)
}
