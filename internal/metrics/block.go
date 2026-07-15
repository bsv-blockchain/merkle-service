package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// SubtreeWorkMessagesTotal counts subtree work items flowing through the
// subtree-worker's retry pipeline, classified by outcome (OutcomeRetried,
// OutcomeDLQ, OutcomeParkedDiskFull). This is the per-outcome visibility the
// 2026-07-15 scale-ovh incident lacked: an immediate-republish storm was only
// diagnosable from raw consumer lag, because retries, DLQ hand-offs, and
// their causes were invisible in metrics.
var SubtreeWorkMessagesTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "merkle_subtree_work_messages_total",
		Help: "Subtree work items handled by the subtree-worker retry pipeline, classified by outcome.",
	},
	[]string{labelOutcome},
)

func init() {
	Registry.MustRegister(
		SubtreeWorkMessagesTotal,
	)
}

// IncSubtreeWork records one subtree-work retry-pipeline outcome.
func IncSubtreeWork(outcome string) {
	SubtreeWorkMessagesTotal.WithLabelValues(outcome).Inc()
}
