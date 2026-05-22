package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// SEEN callback kind label values.
const (
	SeenKindOnNetwork     = "seen_on_network"
	SeenKindMultipleNodes = "seen_multiple_nodes"
)

var (
	SubtreeMessagesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "merkle_subtree_messages_total",
			Help: "Subtree messages handled by the subtree-fetcher, classified by outcome.",
		},
		[]string{labelOutcome},
	)

	subtreeProcessingDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_subtree_processing_duration_seconds",
			Help:    "End-to-end handleMessage duration for the subtree-fetcher.",
			Buckets: DataHubBuckets,
		},
		[]string{labelOutcome},
	)

	subtreeDataHubFetchDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_subtree_datahub_fetch_duration_seconds",
			Help:    "Duration of DataHub subtree fetch operations.",
			Buckets: DataHubBuckets,
		},
		[]string{labelPeerHost, labelOutcome},
	)

	subtreeDataHubFetchBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_subtree_datahub_fetch_bytes",
			Help:    "Size of subtree payload fetched from DataHub.",
			Buckets: MsgSizeBuckets,
		},
		[]string{labelPeerHost},
	)

	subtreeTxidCount = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "merkle_subtree_txid_count",
			Help:    "Number of txids parsed per subtree.",
			Buckets: CountBuckets,
		},
	)

	subtreeRegisteredTxidCount = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "merkle_subtree_registered_txid_count",
			Help:    "Number of registered txids matched per subtree.",
			Buckets: CountBuckets,
		},
	)

	subtreeEmitSeenDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_subtree_emit_seen_callbacks_duration_seconds",
			Help:    "Duration of SEEN callback encode+publish per (callback_host, kind).",
			Buckets: DBBuckets,
		},
		[]string{labelCallbackHost, labelKind},
	)

	subtreeAttemptCount = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "merkle_subtree_attempt_count",
			Help:    "Retry attempt count at the moment a subtree message was successfully processed.",
			Buckets: prometheus.LinearBuckets(0, 1, 11),
		},
	)
)

func init() {
	Registry.MustRegister(
		SubtreeMessagesTotal,
		subtreeProcessingDuration,
		subtreeDataHubFetchDuration,
		subtreeDataHubFetchBytes,
		subtreeTxidCount,
		subtreeRegisteredTxidCount,
		subtreeEmitSeenDuration,
		subtreeAttemptCount,
	)
}

// ObserveSubtreeProcessing records the duration + outcome of handleMessage.
func ObserveSubtreeProcessing(outcome string, d time.Duration) {
	subtreeProcessingDuration.WithLabelValues(outcome).Observe(d.Seconds())
	SubtreeMessagesTotal.WithLabelValues(outcome).Inc()
}

// ObserveSubtreeDataHubFetch records a DataHub fetch's duration + outcome,
// and the payload size on success (size <= 0 on failure paths).
func ObserveSubtreeDataHubFetch(peerURL string, d time.Duration, size int, err error) {
	host := HostLabel(peerURL)
	outcome := OutcomeSuccess
	if err != nil {
		if isTimeoutErr(err) {
			outcome = OutcomeTimeout
		} else {
			outcome = OutcomeError
		}
	}
	subtreeDataHubFetchDuration.WithLabelValues(host, outcome).Observe(d.Seconds())
	if err == nil && size > 0 {
		subtreeDataHubFetchBytes.WithLabelValues(host).Observe(float64(size))
	}
}

// ObserveSubtreeCounts records txid and registered-txid counts per subtree.
func ObserveSubtreeCounts(txids, registered int) {
	if txids > 0 {
		subtreeTxidCount.Observe(float64(txids))
	}
	if registered >= 0 {
		subtreeRegisteredTxidCount.Observe(float64(registered))
	}
}

// ObserveSubtreeEmitSeen records duration of a SEEN callback encode+publish
// against a single callback host + kind.
func ObserveSubtreeEmitSeen(callbackURL, kind string, d time.Duration) {
	subtreeEmitSeenDuration.WithLabelValues(HostLabel(callbackURL), kind).Observe(d.Seconds())
}

// ObserveSubtreeAttemptCount records the AttemptCount of a subtree message
// at the moment it successfully completed.
func ObserveSubtreeAttemptCount(n int) {
	if n < 0 {
		n = 0
	}
	subtreeAttemptCount.Observe(float64(n))
}
