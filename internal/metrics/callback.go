package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	CallbackMessagesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "merkle_callback_messages_total",
			Help: "Callback delivery messages classified by outcome.",
		},
		[]string{"outcome"},
	)

	callbackDeliveryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_callback_delivery_duration_seconds",
			Help:    "HTTP POST duration for callback deliveries, by callback host and status class.",
			Buckets: HTTPBuckets,
		},
		[]string{"callback_host", "status_class"},
	)

	callbackPayloadSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_callback_payload_size_bytes",
			Help:    "Size of the JSON body POSTed to a callback URL.",
			Buckets: MsgSizeBuckets,
		},
		[]string{"callback_host"},
	)

	callbackRetryAttempt = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "merkle_callback_retry_attempt",
			Help:    "RetryCount observed at the moment a callback delivery succeeded.",
			Buckets: prometheus.LinearBuckets(0, 1, 11),
		},
	)

	callbackDedupCheckDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_callback_dedup_check_duration_seconds",
			Help:    "Duration of the dedup Exists check.",
			Buckets: DBBuckets,
		},
		[]string{"outcome"},
	)

	callbackDedupRecordDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_callback_dedup_record_duration_seconds",
			Help:    "Duration of the dedup Record write after a successful delivery.",
			Buckets: DBBuckets,
		},
		[]string{"outcome"},
	)

	callbackStumpFetchDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_callback_stump_fetch_duration_seconds",
			Help:    "Duration of STUMP blob fetches from the claim-check store.",
			Buckets: DBBuckets,
		},
		[]string{"outcome"},
	)
)

func init() {
	Registry.MustRegister(
		CallbackMessagesTotal,
		callbackDeliveryDuration,
		callbackPayloadSize,
		callbackRetryAttempt,
		callbackDedupCheckDuration,
		callbackDedupRecordDuration,
		callbackStumpFetchDuration,
	)
}

// ObserveCallbackDelivery records the HTTP POST duration + status class for
// a callback delivery, plus the request payload size. Pass the raw status
// code from the HTTP response (or 0 with err non-nil for transport
// failures).
func ObserveCallbackDelivery(callbackURL string, statusCode int, payloadSize int, d time.Duration, err error) {
	host := HostLabel(callbackURL)
	statusClass := StatusClass(statusCode, err)
	callbackDeliveryDuration.WithLabelValues(host, statusClass).Observe(d.Seconds())
	if payloadSize > 0 {
		callbackPayloadSize.WithLabelValues(host).Observe(float64(payloadSize))
	}
}

// ObserveCallbackRetryAttempt records the RetryCount distribution at the
// moment a delivery succeeded.
func ObserveCallbackRetryAttempt(retryCount int) {
	if retryCount < 0 {
		retryCount = 0
	}
	callbackRetryAttempt.Observe(float64(retryCount))
}

// ObserveCallbackDedupCheck records the duration + outcome of a dedup
// Exists check. outcome ∈ {hit, miss, error}.
func ObserveCallbackDedupCheck(outcome string, d time.Duration) {
	callbackDedupCheckDuration.WithLabelValues(outcome).Observe(d.Seconds())
}

// ObserveCallbackDedupRecord records the duration + outcome of a dedup
// Record write. outcome ∈ {success, error}.
func ObserveCallbackDedupRecord(outcome string, d time.Duration) {
	callbackDedupRecordDuration.WithLabelValues(outcome).Observe(d.Seconds())
}

// ObserveCallbackStumpFetch records the duration + outcome of a STUMP blob
// fetch. outcome ∈ {success, not_found, error}.
func ObserveCallbackStumpFetch(outcome string, d time.Duration) {
	callbackStumpFetchDuration.WithLabelValues(outcome).Observe(d.Seconds())
}
