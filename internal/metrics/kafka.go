package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Kafka direction labels for the shared message-size histogram.
const (
	KafkaDirectionIn  = "in"
	KafkaDirectionOut = "out"
)

// Kafka error kind labels.
const (
	KafkaErrorDecode       = "decode"
	KafkaErrorHandler      = "handler_error"
	KafkaErrorRebalance    = "rebalance"
	KafkaErrorBroker       = "broker"
)

var (
	kafkaMessagesProduced = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "merkle_kafka_messages_produced_total",
			Help: "Messages produced to Kafka, by topic and outcome.",
		},
		[]string{"topic", "outcome"},
	)

	kafkaProduceDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_kafka_produce_duration_seconds",
			Help:    "Producer SendMessage duration by topic and outcome.",
			Buckets: DBBuckets,
		},
		[]string{"topic", "outcome"},
	)

	kafkaProducedMessageSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_kafka_produced_message_size_bytes",
			Help:    "Size of produced Kafka messages by topic.",
			Buckets: MsgSizeBuckets,
		},
		[]string{"topic"},
	)

	kafkaMessagesConsumed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "merkle_kafka_messages_consumed_total",
			Help: "Messages consumed from Kafka, by topic and consumer group.",
		},
		[]string{"topic", "group"},
	)

	kafkaConsumeHandleDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_kafka_consume_handle_duration_seconds",
			Help:    "Duration of consumer message handler by topic and outcome.",
			Buckets: DataHubBuckets,
		},
		[]string{"topic", "outcome"},
	)

	kafkaConsumerErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "merkle_kafka_consumer_errors_total",
			Help: "Consumer-side error events by topic and error kind.",
		},
		[]string{"topic", "kind"},
	)

	kafkaMessageSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_kafka_message_size_bytes",
			Help:    "Kafka message payload size by topic and direction.",
			Buckets: MsgSizeBuckets,
		},
		[]string{"topic", "direction"},
	)

	kafkaInFlightMessages = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "merkle_kafka_in_flight_messages",
			Help: "Messages currently being handled per topic + group.",
		},
		[]string{"topic", "group"},
	)
)

func init() {
	Registry.MustRegister(
		kafkaMessagesProduced,
		kafkaProduceDuration,
		kafkaProducedMessageSize,
		kafkaMessagesConsumed,
		kafkaConsumeHandleDuration,
		kafkaConsumerErrors,
		kafkaMessageSize,
		kafkaInFlightMessages,
	)
}

// ObserveKafkaProduce records the outcome, duration, and payload size of
// a single Kafka publish.
func ObserveKafkaProduce(topic string, size int, d time.Duration, err error) {
	outcome := OutcomeSuccess
	if err != nil {
		outcome = OutcomeError
	}
	kafkaMessagesProduced.WithLabelValues(topic, outcome).Inc()
	kafkaProduceDuration.WithLabelValues(topic, outcome).Observe(d.Seconds())
	if err == nil && size > 0 {
		kafkaProducedMessageSize.WithLabelValues(topic).Observe(float64(size))
		kafkaMessageSize.WithLabelValues(topic, KafkaDirectionOut).Observe(float64(size))
	}
}

// ObserveKafkaConsumed records a message consumed from a topic + group
// and its payload size.
func ObserveKafkaConsumed(topic, group string, size int) {
	kafkaMessagesConsumed.WithLabelValues(topic, group).Inc()
	if size > 0 {
		kafkaMessageSize.WithLabelValues(topic, KafkaDirectionIn).Observe(float64(size))
	}
}

// ObserveKafkaHandle records the duration + outcome of a consumer
// message-handler invocation.
func ObserveKafkaHandle(topic, outcome string, d time.Duration) {
	kafkaConsumeHandleDuration.WithLabelValues(topic, outcome).Observe(d.Seconds())
}

// IncKafkaConsumerError increments the consumer-side error counter for the
// given topic and error kind.
func IncKafkaConsumerError(topic, kind string) {
	if topic == "" {
		topic = Unknown
	}
	kafkaConsumerErrors.WithLabelValues(topic, kind).Inc()
}

// KafkaInFlight returns the in-flight gauge for the given topic + group so
// callers can Inc / Dec around handler execution.
func KafkaInFlight(topic, group string) prometheus.Gauge {
	return kafkaInFlightMessages.WithLabelValues(topic, group)
}
