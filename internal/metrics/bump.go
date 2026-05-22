package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	bumpBuildDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_bump_build_duration_seconds",
			Help:    "Duration of STUMP build operations.",
			Buckets: BumpBuckets,
		},
		[]string{labelOutcome},
	)

	bumpEncodeDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "merkle_bump_encode_duration_seconds",
			Help:    "Duration of STUMP encode operations.",
			Buckets: BumpBuckets,
		},
	)

	bumpEncodedSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "merkle_bump_encoded_size_bytes",
			Help:    "Size of encoded STUMP payloads.",
			Buckets: MsgSizeBuckets,
		},
	)

	bumpTreeHeight = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "merkle_bump_tree_height",
			Help:    "STUMP merkle tree height.",
			Buckets: prometheus.LinearBuckets(0, 1, 33),
		},
	)

	bumpRegisteredIndices = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "merkle_bump_registered_indices_count",
			Help:    "Number of registered leaf indices included in each STUMP.",
			Buckets: CountBuckets,
		},
	)

	bumpLeavesCount = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "merkle_bump_leaves_count",
			Help:    "Number of leaves in the merkle tree per STUMP build.",
			Buckets: CountBuckets,
		},
	)
)

func init() {
	Registry.MustRegister(
		bumpBuildDuration,
		bumpEncodeDuration,
		bumpEncodedSize,
		bumpTreeHeight,
		bumpRegisteredIndices,
		bumpLeavesCount,
	)
}

// ObserveBumpBuild records a STUMP build's duration plus the input/output
// shape. Pass empty=true when Build returned nil (no registered indices)
// so the outcome label reflects the no-op fast path separately from a
// real build.
func ObserveBumpBuild(d time.Duration, leaves, registered, treeHeight int, empty bool) {
	outcome := OutcomeSuccess
	if empty {
		outcome = OutcomeEmpty
	}
	bumpBuildDuration.WithLabelValues(outcome).Observe(d.Seconds())
	if leaves > 0 {
		bumpLeavesCount.Observe(float64(leaves))
	}
	if registered > 0 {
		bumpRegisteredIndices.Observe(float64(registered))
	}
	if treeHeight >= 0 {
		bumpTreeHeight.Observe(float64(treeHeight))
	}
}

// ObserveBumpEncode records a STUMP encode operation's duration and the
// size of the resulting payload.
func ObserveBumpEncode(d time.Duration, size int) {
	bumpEncodeDuration.Observe(d.Seconds())
	if size > 0 {
		bumpEncodedSize.Observe(float64(size))
	}
}
