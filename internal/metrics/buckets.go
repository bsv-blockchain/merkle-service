package metrics

import "github.com/prometheus/client_golang/prometheus"

// Histogram buckets tuned to the workloads they observe. ExponentialBucketsRange
// panics if its arguments are malformed — that's a programming error, surfaced
// at startup, not a runtime concern.
var (
	// DBBuckets covers 100µs..30s. Aerospike fast-path hits land in the low
	// buckets; the upper end captures pathological backpressure.
	DBBuckets = prometheus.ExponentialBucketsRange(0.0001, 30, 12)

	// BumpBuckets mirrors DBBuckets. STUMP builds are CPU-bound; tiny
	// blocks finish in sub-ms, while a 33M-txid subtree can take seconds.
	BumpBuckets = prometheus.ExponentialBucketsRange(0.0001, 30, 12)

	// HTTPBuckets is the Prometheus default (caps at 10s) extended to 30s
	// to catch slow callback receivers and /reprocess probe chains. The
	// 30s upper bound is strictly greater than DefBuckets' top so the
	// histogram bucket list stays sorted.
	HTTPBuckets = append(append([]float64{}, prometheus.DefBuckets...), 30)

	// MsgSizeBuckets covers 64B..16MB — small callback envelopes through
	// the 10MB Sarama producer cap.
	MsgSizeBuckets = prometheus.ExponentialBucketsRange(64, 16*1024*1024, 12)

	// DataHubBuckets covers 1ms..60s. Large subtree fetches with retry
	// overhead can occupy the upper end.
	DataHubBuckets = prometheus.ExponentialBucketsRange(0.001, 60, 14)

	// CountBuckets covers 1..1M — txid counts, leaf counts, batch sizes.
	CountBuckets = prometheus.ExponentialBucketsRange(1, 1_000_000, 12)
)
