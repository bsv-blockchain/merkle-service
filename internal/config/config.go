package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all configuration for the merkle-service.
type Config struct {
	Mode      string          `yaml:"mode"      mapstructure:"mode"`
	LogLevel  string          `yaml:"logLevel"  mapstructure:"loglevel"`
	API       APIConfig       `yaml:"api"       mapstructure:"api"`
	Store     StoreConfig     `yaml:"store"     mapstructure:"store"`
	Aerospike AerospikeConfig `yaml:"aerospike" mapstructure:"aerospike"`
	Kafka     KafkaConfig     `yaml:"kafka"     mapstructure:"kafka"`
	P2P       P2PConfig       `yaml:"p2p"       mapstructure:"p2p"`
	Subtree   SubtreeConfig   `yaml:"subtree"   mapstructure:"subtree"`
	Block     BlockConfig     `yaml:"block"     mapstructure:"block"`
	Callback  CallbackConfig  `yaml:"callback"  mapstructure:"callback"`
	BlobStore BlobStoreConfig `yaml:"blobStore" mapstructure:"blobstore"`
	DataHub   DataHubConfig   `yaml:"datahub"   mapstructure:"datahub"`
	Registry  RegistryConfig  `yaml:"registry"  mapstructure:"registry"`
	Metrics   MetricsConfig   `yaml:"metrics"   mapstructure:"metrics"`
	Telemetry TelemetryConfig `yaml:"telemetry" mapstructure:"telemetry"`
}

// MetricsConfig controls the Prometheus /metrics exporter. Each binary that
// runs services (api-server, block-processor, callback-delivery,
// subtree-fetcher, subtree-worker, p2p-client, all-in-one) starts a dedicated
// metrics HTTP server on Port. In all-in-one mode a single server is shared
// across the in-process services via the package-level metrics.Registry.
//
// Operators running multiple sub-binaries on the same host MUST override Port
// per-binary to avoid bind conflicts.
type MetricsConfig struct {
	Enabled bool   `yaml:"enabled" mapstructure:"enabled"`
	Port    int    `yaml:"port"    mapstructure:"port"`
	Path    string `yaml:"path"    mapstructure:"path"`
}

// TelemetryConfig configures OpenTelemetry export of traces and metrics to
// an external OTLP collector. Disabled by default: with Enabled=false,
// telemetry.Init installs no providers and opens no network connections, so
// runtime behavior matches a build with no OTEL support at all.
//
// There is intentionally NO log export here — stdout JSON (via
// service.NewLogger) remains the only log path. OTLP log export is not
// implemented.
//
// When Enabled, Endpoint may be left empty to fall back to the standard
// OTEL_EXPORTER_OTLP_ENDPOINT / OTEL_EXPORTER_OTLP_{TRACES,METRICS}_ENDPOINT
// environment variables — that fallback is resolved inside telemetry.Init,
// not here.
type TelemetryConfig struct {
	Enabled bool `yaml:"enabled" mapstructure:"enabled"`
	// Endpoint is the host:port of the OTLP collector. May be left empty
	// when Enabled=true — the exporter then falls back to the standard
	// OTEL_EXPORTER_OTLP_ENDPOINT / OTEL_EXPORTER_OTLP_{TRACES,METRICS}_ENDPOINT
	// env vars.
	Endpoint string `yaml:"endpoint" mapstructure:"endpoint"`
	// Protocol selects the OTLP transport: "grpc" (default) or "http".
	Protocol string `yaml:"protocol" mapstructure:"protocol"`
	// Insecure skips TLS when dialing the collector (e.g. an in-cluster
	// collector reachable without TLS).
	Insecure bool `yaml:"insecure" mapstructure:"insecure"`
	// ServiceName sets the resource's service.name attribute. Default
	// "merkle-service".
	ServiceName string `yaml:"serviceName" mapstructure:"servicename"`
	// Namespace sets the resource's service.namespace attribute (e.g. the
	// downstream solution's name). Omitted from the resource when empty.
	Namespace string `yaml:"namespace" mapstructure:"namespace"`
	// Traces enables the OTLP trace pipeline. Default true.
	Traces bool `yaml:"traces" mapstructure:"traces"`
	// Metrics enables the OTLP metric pipeline (Prometheus bridge, reading
	// from metrics.Registry). Default true.
	Metrics bool `yaml:"metrics" mapstructure:"metrics"`
	// SampleRatio is the ratio (0.0-1.0) fed to a ParentBased(TraceIDRatioBased)
	// sampler. Default 1.0 (sample everything).
	SampleRatio float64 `yaml:"sampleRatio" mapstructure:"sampleratio"`
	// ExportTimeoutMs bounds Shutdown's ForceFlush+Shutdown of every
	// provider. Default 10000 (10s).
	ExportTimeoutMs int `yaml:"exportTimeoutMs" mapstructure:"exporttimeoutms"`
}

// RegistryConfig holds policy for the txid -> callback URL registration store.
// Limits and quotas live here regardless of whether the backend is Aerospike
// or SQL, so behavior stays identical across deployments.
type RegistryConfig struct {
	// MaxCallbacksPerTxID caps the number of distinct callback URLs that may
	// be registered for a single txid. Once reached, further Add calls fail
	// with store.ErrMaxCallbacksPerTxIDExceeded. Idempotent re-registration
	// of an already-known URL is unaffected by the limit. 0 disables the cap
	// (matches the legacy unbounded behavior — not recommended in
	// production; F-050 documents the resource-exhaustion risk).
	MaxCallbacksPerTxID int `yaml:"maxCallbacksPerTxID" mapstructure:"maxcallbackspertxid"`
}

// Backend names. Kept as package-level consts so callers don't stringly-type.
const (
	BackendAerospike = "aerospike"
	BackendSQL       = "sql"
)

// StoreConfig selects the persistence backend and carries backend-specific
// nested config blocks. When Backend == "aerospike" the top-level Aerospike
// block is used; when Backend == "sql" the Store.SQL block is used.
type StoreConfig struct {
	Backend string         `yaml:"backend" mapstructure:"backend"`
	SQL     StoreSQLConfig `yaml:"sql"     mapstructure:"sql"`
}

// StoreSQLConfig configures the SQL backend.
type StoreSQLConfig struct {
	Driver          string `yaml:"driver"          mapstructure:"driver"`
	DSN             string `yaml:"dsn"             mapstructure:"dsn"`
	Schema          string `yaml:"schema"          mapstructure:"schema"`
	SweeperInterval string `yaml:"sweeperInterval" mapstructure:"sweeperinterval"`
	MaxOpenConns    int    `yaml:"maxOpenConns"    mapstructure:"maxopenconns"`
	MaxIdleConns    int    `yaml:"maxIdleConns"    mapstructure:"maxidleconns"`
	// ConnMaxIdleTimeSec, if >0, calls db.SetConnMaxIdleTime so idle
	// connections are closed after this many seconds even when within the
	// MaxIdleConns budget. The Go database/sql default is 0 (no idle reaping),
	// which lets pods retain dead connections across long quiet periods and
	// pile up against PostgreSQL's max_connections under fan-out. Recommended
	// 60 in production. 0 disables.
	ConnMaxIdleTimeSec int `yaml:"connMaxIdleTimeSec" mapstructure:"connmaxidletimesec"`
	// ConnMaxLifetimeSec, if >0, calls db.SetConnMaxLifetime so any
	// connection — idle or in use — is recycled after this many seconds.
	// Useful for environments where the server reaps idle backends or where
	// load-balanced backends rotate. 0 disables. Recommended 900 (15m).
	ConnMaxLifetimeSec int `yaml:"connMaxLifetimeSec" mapstructure:"connmaxlifetimesec"`
	// CallbackURLRegistryRetention bounds the SQL callback URL registry. URLs
	// whose last `Add` is older than this are dropped by the TTL sweeper.
	// Format: any time.ParseDuration string ("168h", "7d" is NOT supported).
	// Default 168h (7 days). See F-037 / issue #23.
	CallbackURLRegistryRetention string `yaml:"callbackUrlRegistryRetention" mapstructure:"callbackurlregistryretention"`
}

// APIConfig holds HTTP API configuration.
type APIConfig struct {
	Port int `yaml:"port" mapstructure:"port"`

	// AuthToken is the shared bearer secret required on POST /reprocess (and,
	// when RequireWatchAuth is true, POST /watch). Empty (the default) disables
	// inbound auth entirely — the api-server logs a warning once at startup and
	// serves /reprocess unauthenticated, preserving today's behavior. This
	// fail-open default lets operators roll auth out in two phases: deploy the
	// binary that *can* enforce, then set the token to *start* enforcing, so an
	// arcade client that hasn't provisioned merkle_service.auth_token isn't
	// broken by the upgrade itself. Source from a Secret via env API_AUTH_TOKEN;
	// never commit it to config.yaml / the ConfigMap.
	AuthToken string `yaml:"authToken" mapstructure:"authtoken"`

	// RequireWatchAuth additionally gates POST /watch behind AuthToken. Default
	// false: /watch is a bounded, idempotent registration upsert with no block
	// fan-out, so its abuse ceiling is low, and gating it would halt an
	// un-tokened arcade's transaction propagation (F-024 register-before-
	// broadcast). Operators wanting defense-in-depth can opt in once every
	// client has a token. Has no effect while AuthToken is empty.
	RequireWatchAuth bool `yaml:"requireWatchAuth" mapstructure:"requirewatchauth"`

	// ReprocessRateLimitRps is the sustained token-bucket rate (requests/sec)
	// admitted on POST /reprocess. <= 0 disables rate limiting. Default 20 — far
	// above legitimate watchdog volume (~4 concurrent, ~100/tick every 30s), so
	// this is defense-in-depth behind AuthToken, not a throttle on normal use.
	ReprocessRateLimitRps float64 `yaml:"reprocessRateLimitRps" mapstructure:"reprocessratelimitrps"`

	// ReprocessBurst is the token-bucket burst size for the /reprocess rate
	// limiter. Default 100. Ignored when ReprocessRateLimitRps <= 0.
	ReprocessBurst int `yaml:"reprocessBurst" mapstructure:"reprocessburst"`

	// MaxInflightReprocess caps concurrently-executing POST /reprocess requests
	// (each probes DataHubs synchronously before returning). <= 0 disables the
	// cap. Default 16. Excess requests get 429 rather than queueing.
	MaxInflightReprocess int `yaml:"maxInflightReprocess" mapstructure:"maxinflightreprocess"`
}

// AerospikeConfig holds Aerospike connection configuration.
type AerospikeConfig struct {
	// Host is a single seed node address. Kept for backward compatibility;
	// prefer Seeds for production (multiple seeds protect against the seed
	// node's connection pool becoming the bottleneck).
	Host string `yaml:"host" mapstructure:"host"`
	// Seeds is a list of seed node addresses. When non-empty, Host is ignored.
	Seeds               []string `yaml:"seeds"                mapstructure:"seeds"`
	Port                int      `yaml:"port"                 mapstructure:"port"`
	Namespace           string   `yaml:"namespace"            mapstructure:"namespace"`
	SetName             string   `yaml:"setName"              mapstructure:"setname"`
	SeenSet             string   `yaml:"seenSet"              mapstructure:"seenset"`
	CallbackDedupSet    string   `yaml:"callbackDedupSet"     mapstructure:"callbackdedupset"`
	CallbackURLRegistry string   `yaml:"callbackUrlRegistry"  mapstructure:"callbackurlregistry"`
	// CallbackURLRegistryTTLSec is the per-URL eviction window applied by the
	// Aerospike callback URL registry (and the SQL sibling). URLs whose last
	// `Add` is older than this are evicted, bounding the registry's growth so
	// BLOCK_PROCESSED fan-out and the underlying record(s) never grow without
	// limit. Default 7 days. See F-037 / issue #23.
	CallbackURLRegistryTTLSec int    `yaml:"callbackUrlRegistryTTLSec" mapstructure:"callbackurlregistryttlsec"`
	SubtreeCounterSet         string `yaml:"subtreeCounterSet"    mapstructure:"subtreecounterset"`
	SubtreeCounterTTLSec      int    `yaml:"subtreeCounterTTLSec" mapstructure:"subtreecounterttlsec"`
	// ExpectedStumpSet stores, per (block, callbackURL), the set of subtree
	// indices that produced a STUMP — surfaced on BLOCK_PROCESSED so the receiver
	// can detect a missing STUMP. Shares the subtree counter's TTL (same
	// lifetime: both live until the block is fully processed).
	ExpectedStumpSet          string `yaml:"expectedStumpSet" mapstructure:"expectedstumpset"`
	CallbackAccumulatorSet    string `yaml:"callbackAccumulatorSet"    mapstructure:"callbackaccumulatorset"`
	CallbackAccumulatorTTLSec int    `yaml:"callbackAccumulatorTTLSec" mapstructure:"callbackaccumulatorttlsec"`
	// DataHubRegistry is the Aerospike set name for the on-demand /reprocess
	// DataHub URL pool. Block-processor adds an entry whenever it successfully
	// fetches block metadata, so the pool reflects DataHubs known to be
	// healthy for this network.
	DataHubRegistry string `yaml:"dataHubRegistry" mapstructure:"datahubregistry"`
	// DataHubRegistryTTLSec bounds the DataHub URL registry — entries older
	// than this are evicted by the per-record TTL. Default 7 days; tune down
	// for clusters where DataHub fleet membership rotates more aggressively.
	DataHubRegistryTTLSec int `yaml:"dataHubRegistryTTLSec" mapstructure:"datahubregistryttlsec"`
	MaxRetries            int `yaml:"maxRetries"  mapstructure:"maxretries"`
	RetryBaseMs           int `yaml:"retryBaseMs" mapstructure:"retrybasems"`
	// ConnectionQueueSize is the per-node connection pool size. The Aerospike
	// Go client default is 100; under bursty BatchGet load (e.g. 14+ subtrees
	// processed in parallel during block-time, each fanning out thousands of
	// txid lookups) the pool is trivially exhausted and we see
	// "connection pool is empty" timeouts.
	ConnectionQueueSize int `yaml:"connectionQueueSize" mapstructure:"connectionqueuesize"`
	// MinConnectionsPerNode keeps a warm baseline of connections so the first
	// burst after idle doesn't pay the dial cost on every request.
	MinConnectionsPerNode int `yaml:"minConnectionsPerNode" mapstructure:"minconnectionspernode"`
	// LimitConnectionsToQueueSize, when false, allows transient bursts above
	// ConnectionQueueSize (extra connections are closed when returned). Set to
	// true to enforce a hard cap (fails fast under sustained overload).
	LimitConnectionsToQueueSize bool `yaml:"limitConnectionsToQueueSize" mapstructure:"limitconnectionstoqueuesize"`
	// SocketTimeoutMs caps the time spent on a single transport-level
	// read/write attempt before retrying. 0 = client default.
	SocketTimeoutMs int `yaml:"socketTimeoutMs" mapstructure:"sockettimeoutms"`
	// TotalTimeoutMs is retained for back-compat; new code should set the
	// per-operation timeouts below. When the more specific timeouts are zero,
	// this value is no longer used.
	TotalTimeoutMs int `yaml:"totalTimeoutMs" mapstructure:"totaltimeoutms"`
	// ReadTimeoutMs is the TotalTimeout applied to single-record Get/Exists.
	// Single-record reads should be sub-second under healthy load; default
	// 3000 ms.
	ReadTimeoutMs int `yaml:"readTimeoutMs" mapstructure:"readtimeoutms"`
	// WriteTimeoutMs is the TotalTimeout applied to Put/Operate. Default 5000.
	WriteTimeoutMs int `yaml:"writeTimeoutMs" mapstructure:"writetimeoutms"`
	// BatchTimeoutMs is the TotalTimeout applied to BatchGet/BatchOperate.
	// Default 15000.
	BatchTimeoutMs int `yaml:"batchTimeoutMs" mapstructure:"batchtimeoutms"`
	// IdleTimeoutSec is the client-side connection idle reap. Set a few
	// seconds below the server's proto-fd-idle-ms so the client closes idle
	// sockets before the server reaps them. Mandatory when the server runs
	// with proto-fd-idle-ms=0 (otherwise idle conns accumulate forever and
	// starve the pool). Default 55.
	IdleTimeoutSec int `yaml:"idleTimeoutSec" mapstructure:"idletimeoutsec"`
	// MaxErrorRate is the threshold for the client's per-node circuit
	// breaker — once a node returns this many errors within an
	// ErrorRateWindow, subsequent requests fast-fail with MAX_ERROR_RATE
	// instead of timing out and holding pool slots. Default 5 — Aerospike's
	// stock 100 is too lenient for multi-tenant clusters where one slow node
	// would otherwise drain the pool before tripping.
	MaxErrorRate int `yaml:"maxErrorRate" mapstructure:"maxerrorrate"`
	// ErrorRateWindow is the number of cluster-tend iterations over which
	// MaxErrorRate accumulates. Default 1 (~1 second of accumulation).
	ErrorRateWindow int `yaml:"errorRateWindow" mapstructure:"errorratewindow"`
	// BatchConcurrentNodes controls how many cluster nodes a single
	// BatchGet/BatchOperate fans out to concurrently. The Aerospike Go client
	// defaults this to 1 — each owning node's sub-batch is issued serially on
	// the calling goroutine, so a batch whose keys span M nodes costs M
	// sequential round-trips. On the SEEN/MINED read path (registration
	// BatchGet, seen-counter BatchIncrement) that serialization is the dominant
	// per-subtree database cost.
	//
	//	0  = all owning nodes concurrently — one parallel fan-out (default)
	//	1  = serial, one node at a time (the Aerospike library default)
	//	>1 = up to N nodes concurrently
	//
	// Default 0 (all concurrent). On a large cluster, where a wide fan-out from
	// many partition workers at once could push the per-node pool
	// (ConnectionQueueSize) under pressure, set a bound (e.g. 8) to trade some
	// parallelism for steadier connection usage.
	BatchConcurrentNodes int `yaml:"batchConcurrentNodes" mapstructure:"batchconcurrentnodes"`
	// BatchChunkConcurrency caps how many of a single batch operation's 5000-key
	// chunks are issued concurrently. A teranode-default subtree (~1M txids) is
	// split into ~200 chunks; processed serially (the prior behaviour) that is
	// ~200 sequential round-trips on one goroutine, the dominant SEEN/MINED
	// read-path latency. Applies to registration BatchGet and seen-counter
	// BatchIncrement. 0/1 = serial (one chunk at a time); >1 = up to N chunks
	// in flight at once. Default 8.
	//
	// Connection-budget note: this multiplies with BatchConcurrentNodes and with
	// the caller's own concurrency (block.batchGetConcurrency, or the number of
	// active consumer partitions on the SEEN fetcher). Peak in-flight node
	// connections ≈ callerConcurrency × BatchChunkConcurrency × nodes — keep that
	// under ConnectionQueueSize, or bound this on large clusters.
	BatchChunkConcurrency int `yaml:"batchChunkConcurrency" mapstructure:"batchchunkconcurrency"`
}

// SeedHosts returns the list of seed hosts to use when constructing the
// Aerospike client. Falls back to the single Host when Seeds is empty.
func (a AerospikeConfig) SeedHosts() []string {
	if len(a.Seeds) > 0 {
		return a.Seeds
	}
	if a.Host == "" {
		return nil
	}
	return []string{a.Host}
}

// KafkaConfig holds Kafka connection configuration.
// Default partition counts for the two safe-to-widen topics. subtree-work gets
// the most because a block fans out into hundreds–thousands of independent
// subtree build units; subtree (the SEEN path) gets a smaller spread.
const (
	defaultSubtreePartitions     = 12
	defaultSubtreeWorkPartitions = 24
	// defaultCallbackSeenPartitions is the width of the DEDICATED SEEN callback
	// topic (CallbackSeenTopic). It only applies when that topic is configured;
	// it never widens the shared 'callback' topic. SEEN callbacks carry no
	// cross-message ordering requirement that a partition split could break —
	// arcade's status lattice refuses to supersede a higher status with a lower
	// one, so SEEN_MULTIPLE_NODES arriving before SEEN_ON_NETWORK is a no-op
	// rather than a regression.
	defaultCallbackSeenPartitions = 12
)

// Default retention.ms applied when THIS service creates a topic (see
// TopicRetention). Work topics get 6h — comfortably above any realistic
// processing backlog; DLQ topics get 7d so dead letters survive long enough
// for a human to notice and act. Without an explicit value a fresh cluster's
// broker default applies silently — on dev-ovh-1 that default was 15
// MINUTES, short enough to expire parked work and dead letters unseen.
const (
	defaultTopicRetentionMs int64 = 21_600_000  // 6h
	defaultDLQRetentionMs   int64 = 604_800_000 // 7d
)

type KafkaConfig struct {
	Brokers             []string `yaml:"brokers"             mapstructure:"brokers"`
	SubtreeTopic        string   `yaml:"subtreeTopic"        mapstructure:"subtreetopic"`
	BlockTopic          string   `yaml:"blockTopic"          mapstructure:"blocktopic"`
	CallbackTopic       string   `yaml:"callbackTopic"       mapstructure:"callbacktopic"`
	CallbackSeenTopic   string   `yaml:"callbackSeenTopic"   mapstructure:"callbackseentopic"`
	CallbackDLQTopic    string   `yaml:"callbackDlqTopic"    mapstructure:"callbackdlqtopic"`
	SubtreeDLQTopic     string   `yaml:"subtreeDlqTopic"     mapstructure:"subtreedlqtopic"`
	SubtreeWorkTopic    string   `yaml:"subtreeWorkTopic"    mapstructure:"subtreeworktopic"`
	SubtreeWorkDLQTopic string   `yaml:"subtreeWorkDlqTopic" mapstructure:"subtreeworkdlqtopic"`
	ConsumerGroup       string   `yaml:"consumerGroup"       mapstructure:"consumergroup"`

	// SubtreePartitions / SubtreeWorkPartitions set how many partitions the
	// 'subtree' and 'subtree-work' topics are created with (and grown to on
	// startup). These two topics carry independent, idempotent, per-subtree work
	// units with NO ordering requirement between them, so partitioning them is
	// purely a parallelism lever: a consumer group can run at most one in-order
	// worker goroutine per partition, so partitions cap how many subtrees a stage
	// can fetch/build concurrently. Defaults: subtree-work 24, subtree 12.
	//
	// The 'block' and 'callback' topics are deliberately NOT configurable here and
	// stay at 1 partition: 'block' is low-rate, and 'callback' — which after the
	// SEEN split carries ONLY the STUMP / BLOCK_PROCESSED traffic — relies on
	// single-partition ordering to keep BLOCK_PROCESSED behind its STUMPs.
	// Widening 'callback' stays deferred until the expectedSubtreeIndices
	// completeness check is verified end-to-end in arcade — rollout step 4 of
	// docs/block-processed-completeness.md (which supersedes the earlier
	// callback-topic-partition-design.md, no longer in the repo).
	//
	// CallbackSeenPartitions is a SEPARATE knob for the dedicated SEEN topic
	// (CallbackSeenTopic) and never touches 'callback': SEEN callbacks carry no
	// STUMP → BLOCK_PROCESSED barrier, so they are free to fan out.
	SubtreePartitions      int `yaml:"subtreePartitions"      mapstructure:"subtreepartitions"`
	SubtreeWorkPartitions  int `yaml:"subtreeWorkPartitions"  mapstructure:"subtreeworkpartitions"`
	CallbackSeenPartitions int `yaml:"callbackSeenPartitions" mapstructure:"callbackseenpartitions"`

	// TopicRetentionMs / DLQRetentionMs set the retention.ms config applied
	// when this service CREATES a topic (work topics and DLQ topics
	// respectively — see TopicRetention). Applied at create time only:
	// existing topics are never altered, so GitOps-managed Topic CRDs remain
	// authoritative wherever they exist. Non-positive values fall back to the
	// built-in defaults (6h / 7d).
	TopicRetentionMs int64 `yaml:"topicRetentionMs" mapstructure:"topicretentionms"`
	DLQRetentionMs   int64 `yaml:"dlqRetentionMs"   mapstructure:"dlqretentionms"`
}

// SeenCallbackTopic returns the topic SEEN_ON_NETWORK / SEEN_MULTIPLE_NODES
// callbacks travel on: the dedicated CallbackSeenTopic when configured,
// otherwise the shared CallbackTopic (pre-split behavior).
//
// BOTH the producer (internal/subtree/processor.go) and the consumer
// (internal/callback/delivery.go) MUST route through this one helper — if
// either side hard-codes a topic while the other reads config, SEEN callbacks
// are published to a topic nobody consumes and every watched transaction
// silently strands at ACCEPTED_BY_NETWORK.
func (k KafkaConfig) SeenCallbackTopic() string {
	if k.CallbackSeenTopic != "" {
		return k.CallbackSeenTopic
	}
	return k.CallbackTopic
}

// TopicPartitions maps each topic name to the partition count it should be
// created with (and grown to). Only the safe-to-widen topics carry a count
// above 1; every other topic is absent, which callers/EnsureTopics treat as the
// default of 1. Counts <= 0 fall back to the built-in default.
func (k KafkaConfig) TopicPartitions() map[string]int32 {
	subtree := k.SubtreePartitions
	if subtree <= 0 {
		subtree = defaultSubtreePartitions
	}
	work := k.SubtreeWorkPartitions
	if work <= 0 {
		work = defaultSubtreeWorkPartitions
	}
	seen := k.CallbackSeenPartitions
	if seen <= 0 {
		seen = defaultCallbackSeenPartitions
	}
	m := make(map[string]int32, 3)
	if k.SubtreeTopic != "" {
		m[k.SubtreeTopic] = int32(subtree)
	}
	if k.SubtreeWorkTopic != "" {
		m[k.SubtreeWorkTopic] = int32(work)
	}
	// Deliberately keyed off CallbackSeenTopic, NOT SeenCallbackTopic(): in
	// fallback mode the helper returns CallbackTopic, so using it here would
	// silently widen the shared 'callback' topic to defaultCallbackSeenPartitions
	// and shatter the STUMP → BLOCK_PROCESSED ordering barrier. 'callback' must
	// stay absent from this map (see the topic_partitions_test assertion).
	if k.CallbackSeenTopic != "" {
		m[k.CallbackSeenTopic] = int32(seen)
	}
	return m
}

// TopicRetention maps every configured topic name to the retention.ms it
// should be CREATED with (EnsureTopics never alters existing topics): work
// topics get TopicRetentionMs, DLQ topics get DLQRetentionMs, non-positive
// values fall back to the built-in defaults (6h / 7d). Empty topic names are
// skipped.
func (k KafkaConfig) TopicRetention() map[string]int64 {
	topicMs := k.TopicRetentionMs
	if topicMs <= 0 {
		topicMs = defaultTopicRetentionMs
	}
	dlqMs := k.DLQRetentionMs
	if dlqMs <= 0 {
		dlqMs = defaultDLQRetentionMs
	}
	m := make(map[string]int64, 8)
	for _, topic := range []string{k.SubtreeTopic, k.BlockTopic, k.CallbackTopic, k.CallbackSeenTopic, k.SubtreeWorkTopic} {
		if topic != "" {
			m[topic] = topicMs
		}
	}
	for _, topic := range []string{k.CallbackDLQTopic, k.SubtreeDLQTopic, k.SubtreeWorkDLQTopic} {
		if topic != "" {
			m[topic] = dlqMs
		}
	}
	return m
}

// P2PMsgBusConfig holds configuration for the underlying libp2p message bus.
type P2PMsgBusConfig struct {
	DHTMode        string   `yaml:"dhtMode"        mapstructure:"dhtmode"`
	Port           int      `yaml:"port"           mapstructure:"port"`
	AnnounceAddrs  []string `yaml:"announceAddrs"  mapstructure:"announceaddrs"`
	BootstrapPeers []string `yaml:"bootstrapPeers" mapstructure:"bootstrappeers"`
	MaxConnections int      `yaml:"maxConnections" mapstructure:"maxconnections"`
	MinConnections int      `yaml:"minConnections" mapstructure:"minconnections"`
	EnableNAT      bool     `yaml:"enableNAT"      mapstructure:"enablenat"`
	EnableMDNS     bool     `yaml:"enableMDNS"     mapstructure:"enablemdns"`
}

// P2PConfig holds peer-to-peer network configuration.
type P2PConfig struct {
	Network     string          `yaml:"network"     mapstructure:"network"`
	StoragePath string          `yaml:"storagePath" mapstructure:"storagepath"`
	MsgBus      P2PMsgBusConfig `yaml:"msgbus"      mapstructure:"msgbus"`
}

// SubtreeConfig holds subtree processing configuration.
type SubtreeConfig struct {
	StorageMode    string `yaml:"storageMode"    mapstructure:"storagemode"`
	DAHOffset      int    `yaml:"dahOffset"      mapstructure:"dahoffset"`
	StumpDAHOffset int    `yaml:"stumpDahOffset" mapstructure:"stumpdahoffset"`
	CacheMaxMB     int    `yaml:"cacheMaxMB"     mapstructure:"cachemaxmb"`
	DedupCacheSize int    `yaml:"dedupCacheSize" mapstructure:"dedupcachesize"`
	// MaxAttempts caps how many times a subtree message is re-driven through
	// the `subtree` topic before it is parked on `subtree-dlq`. Previously a
	// permanently-failing subtree (e.g. DataHub 404) stalled the partition
	// forever because the consumer doesn't MarkMessage on handler error.
	MaxAttempts int `yaml:"maxAttempts" mapstructure:"maxattempts"`
	// RetryBackoffBaseMs is the base delay before a transient subtree failure
	// is re-published for retry; it doubles per attempt and is capped at 30s.
	// Without it the whole MaxAttempts budget burned in ~300ms on dev-ovh-1
	// (blob-store ENOSPC), turning a 15-minute disk incident into 1,406
	// permanent dead letters. Default 1000. 0 disables the backoff (unit
	// tests; not recommended in production).
	RetryBackoffBaseMs int `yaml:"retryBackoffBaseMs" mapstructure:"retrybackoffbasems"`
	// SeenTxidLogMax caps how many matched txids are included (as logfields.TxIDs)
	// on the Info log emitted for each SEEN_ON_NETWORK / SEEN_MULTIPLE_NODES
	// batch published to a callback URL. 0 disables the txids array entirely
	// and logs only the count — useful when a batch can carry thousands of
	// txids and the full list would dominate log volume.
	SeenTxidLogMax int `yaml:"seenTxidLogMax" mapstructure:"seentxidlogmax"`
}

// BlockConfig holds block processing configuration.
type BlockConfig struct {
	WorkerPoolSize int `yaml:"workerPoolSize" mapstructure:"workerpoolsize"`
	PostMineTTLSec int `yaml:"postMineTTLSec" mapstructure:"postminettlsec"`
	DedupCacheSize int `yaml:"dedupCacheSize" mapstructure:"dedupcachesize"`
	// MaxAttempts caps how many times a subtree work item is re-driven through
	// the subtree-work topic before it is parked on subtree-work-dlq. The DLQ
	// hand-off still decrements the per-block subtree counter so BLOCK_PROCESSED
	// can fire — at that point arcade will see a missing STUMP for that subtree
	// and surface it as a BUMP build error rather than silently going stale.
	MaxAttempts int `yaml:"maxAttempts" mapstructure:"maxattempts"`
	// RetryBackoffBaseMs is the base delay before a transiently-failed subtree
	// work item is re-published to subtree-work; it doubles per attempt and is
	// capped at 5s (deliberately below the fetcher's 30s — the worker's
	// 10-attempt budget would live at a higher cap and a failing 50-record
	// consumer chunk would sleep past the group's 5m rebalance timeout).
	// Without it the whole MaxAttempts budget burned in milliseconds per item,
	// which is how the 2026-07-15 scale-ovh DataHub-404 storm was amplified
	// ~10x by immediate republishes. Default 1000. 0 disables the backoff
	// (unit tests; not recommended in production).
	RetryBackoffBaseMs int `yaml:"retryBackoffBaseMs" mapstructure:"retrybackoffbasems"`
	// NotFoundMaxAttempts caps the retry budget for work items whose failure
	// is a DataHub 404 (datahub.ErrNotFound). Worker subtrees provably existed
	// at announcement time, but teranode's asset cache prunes subtree data
	// after ~2h — a late 404 is effectively permanent, and burning the full
	// MaxAttempts budget on it only amplified the scale-ovh 404 storm.
	// MaxAttempts remains a hard ceiling regardless of this value. Default 3.
	// <= 0 selects the default.
	NotFoundMaxAttempts int `yaml:"notFoundMaxAttempts" mapstructure:"notfoundmaxattempts"`
	// RegCacheMaxMB sizes the in-process registration deduplication cache used
	// by block-time subtree processing. Same shape as Subtree.CacheMaxMB but
	// independent so all-in-one and microservice topologies behave identically.
	RegCacheMaxMB int `yaml:"regCacheMaxMB" mapstructure:"regcachemaxmb"`
	// BatchGetConcurrency caps the number of concurrent registration BatchGet
	// calls per subtree-worker process. Without this limit a single block
	// announcement can fire one BatchGet per subtree (up to 30+) in parallel
	// and exhaust the Aerospike connection pool.
	BatchGetConcurrency int `yaml:"batchGetConcurrency" mapstructure:"batchgetconcurrency"`
	// EmitExpectedStumpSet controls whether the subtree worker records the
	// per-(block, callbackURL) set of subtree indices that produced a STUMP and
	// attaches it to BLOCK_PROCESSED as expectedSubtreeIndices, so the receiver
	// can detect a missing STUMP (docs/block-processed-completeness.md).
	//
	// Default TRUE — deliberately diverging from that doc's original "default
	// off" rollout plan: the feature shipped unconditionally in v0.4.5 (#162)
	// before this flag existed, so its Aerospike write load on the MINED hot
	// path is already absorbed in production. The flag is an emergency
	// off-switch, not a rollout gate.
	//
	// A *bool so that nil (unset) means enabled: zero-valued BlockConfigs built
	// directly in tests keep production semantics; only an explicit false
	// disables. Use EmitExpectedStumpSetEnabled to read it.
	//
	// Disabling turns off BOTH the recording (write) and the attach (read)
	// together. Gating only the write while still attaching would ship an
	// EMPTY expected set, telling the receiver to expect zero STUMPs — which
	// reintroduces the exact silent-loss bug the feature exists to fix.
	EmitExpectedStumpSet *bool `yaml:"emitExpectedStumpSet" mapstructure:"emitexpectedstumpset"`
}

// EmitExpectedStumpSetEnabled reports whether the expected-STUMP set is
// recorded and attached to BLOCK_PROCESSED. nil (unset) means enabled: the
// default is true, so a zero-valued BlockConfig behaves like production. Only
// an explicit false disables. See BlockConfig.EmitExpectedStumpSet for the
// default-true rationale and the write+attach-gated-together requirement.
func (b BlockConfig) EmitExpectedStumpSetEnabled() bool {
	return b.EmitExpectedStumpSet == nil || *b.EmitExpectedStumpSet
}

// CallbackConfig holds callback delivery configuration.
type CallbackConfig struct {
	MaxRetries          int `yaml:"maxRetries"          mapstructure:"maxretries"`
	BackoffBaseSec      int `yaml:"backoffBaseSec"      mapstructure:"backoffbasesec"`
	TimeoutSec          int `yaml:"timeoutSec"          mapstructure:"timeoutsec"`
	SeenThreshold       int `yaml:"seenThreshold"       mapstructure:"seenthreshold"`
	DedupTTLSec         int `yaml:"dedupTTLSec"         mapstructure:"dedupttlsec"`
	MaxConnsPerHost     int `yaml:"maxConnsPerHost"     mapstructure:"maxconnsperhost"`
	MaxIdleConnsPerHost int `yaml:"maxIdleConnsPerHost" mapstructure:"maxidleconnsperhost"`
	// BreakerThreshold is the number of DLQ'd callbacks to a single URL that
	// trips its circuit breaker. Once tripped the URL is disabled in the
	// registry and BLOCK_PROCESSED / STUMP fan-out stops targeting it (a fresh
	// /watch re-enables it). Default 20; set <= 0 to disable the breaker.
	BreakerThreshold int `yaml:"breakerThreshold" mapstructure:"breakerthreshold"`
	// AllowPrivateIPs disables the SSRF guard that normally rejects
	// callback URLs (or dial addresses) pointing at loopback,
	// link-local, or RFC1918 destinations. Default false. Operators
	// who legitimately need to deliver to internal services (testing,
	// in-cluster sidecars) can set this to true. The guard against
	// 0.0.0.0/multicast destinations remains in force regardless.
	AllowPrivateIPs bool `yaml:"allowPrivateIPs" mapstructure:"allowprivateips"`
}

// BlobStoreConfig holds blob store configuration.
type BlobStoreConfig struct {
	URL string `yaml:"url" mapstructure:"url"`
	// SweepIntervalSec is how often the age sweeper walks a file:// store.
	// The sweep runs in the block-processor only (single replica — a
	// per-replica sweep would stampede the shared volume). 0 disables the
	// sweeper. Default 300.
	SweepIntervalSec int `yaml:"sweepIntervalSec" mapstructure:"sweepintervalsec"`
	// SweepMaxAgeSec is the age after which a top-level subtree blob (a
	// 64-lowercase-hex file at the store root) is removed by the sweeper —
	// the backstop for blobs whose subtree-work item never completes
	// (trimmed queues, long parking, crashes), which the delete-at-height
	// path can never reach. Subtree blobs are a re-fetchable cache (DataHub
	// re-serves them), so an aggressive age is safe; STUMP blobs (under
	// stump/) and .dah/ bookkeeping are never age-swept. Default 1800
	// (~3 blocks at ~10min cadence). Nonzero values below 600 are rejected
	// at startup: a sweep age under one block interval could delete the
	// in-flight block's blobs mid-processing. 0 disables age sweeping.
	// Rationale: 2026-07-15 dev-ovh-1 incident — orphaned subtree blobs
	// filled a 1TiB volume in ~3h while steady state is ~2 blocks ≈ 26GB.
	SweepMaxAgeSec int `yaml:"sweepMaxAgeSec" mapstructure:"sweepmaxagesec"`
}

// DataHubConfig holds DataHub HTTP client configuration.
type DataHubConfig struct {
	TimeoutSec int `yaml:"timeoutSec" mapstructure:"timeoutsec"`
	MaxRetries int `yaml:"maxRetries" mapstructure:"maxretries"`

	// MaxBlockBytes caps a single /block/<hash> response body. Block metadata
	// is small (header + subtree-hash list) so the default 16 MiB is two
	// orders of magnitude of headroom. A value <= 0 selects the default.
	// Mitigates F-027 (DataHub responses read into memory without a cap).
	MaxBlockBytes int64 `yaml:"maxBlockBytes" mapstructure:"maxblockbytes"`

	// MaxSubtreeBytes caps a single /subtree/<hash> binary response body.
	// DataHub subtrees are concatenated 32-byte hashes; the default 1 GiB
	// (~33.5M txids) accommodates Teranode's largest realistic subtrees.
	// Operators with a smaller subtree size limit should tune this down.
	// A value <= 0 selects the default.
	MaxSubtreeBytes int64 `yaml:"maxSubtreeBytes" mapstructure:"maxsubtreebytes"`

	// AllowPrivateIPs disables the SSRF guard that normally rejects
	// DataHub URLs (or dial addresses) pointing at loopback,
	// link-local, or RFC1918 destinations. Default false. Because the
	// dataHubURL flows from peer-controlled P2P announcements, by
	// default a forged announcement cannot turn the block/subtree
	// processors into an SSRF primitive against cluster-internal
	// services or cloud metadata endpoints. Operators running an
	// in-cluster Teranode peer (testing, sidecar topologies) can set
	// this to true. The guard against 0.0.0.0/multicast destinations
	// remains in force regardless. See finding F-028.
	AllowPrivateIPs bool `yaml:"allowPrivateIPs" mapstructure:"allowprivateips"`

	// FallbackURLs lists operator-trusted DataHub endpoints used by the
	// /reprocess flow. The API server probes these (alongside any URLs
	// observed via P2P and persisted in the DataHubRegistry) to find one
	// that serves the requested block. The first entry to return metadata
	// for the block hash wins and is used for the entire reprocess. Order
	// matters: configured entries are tried before discovered ones.
	FallbackURLs []string `yaml:"fallbackUrls" mapstructure:"fallbackurls"`

	// PeerHealth controls the in-memory tracker that marks DataHub peers
	// unhealthy after consecutive failures so they are skipped for a
	// cooldown window in /reprocess probes and block-processor failover.
	PeerHealth PeerHealthConfig `yaml:"peerHealth" mapstructure:"peerhealth"`
}

// PeerHealthConfig configures the in-memory peer-health tracker. State
// is per-process and resets on restart. A zero or negative value selects
// the corresponding default.
type PeerHealthConfig struct {
	// FailureThreshold is the number of consecutive failures against a
	// peer's host before it is marked unhealthy. Any successful fetch
	// against the same host resets the counter.
	FailureThreshold int `yaml:"failureThreshold" mapstructure:"failurethreshold"`

	// CooldownSec is how long a peer stays marked unhealthy after the
	// threshold is reached. Expiry is checked lazily on the next
	// IsHealthy call.
	CooldownSec int `yaml:"cooldownSec" mapstructure:"cooldownsec"`

	// Stale404GraceSec is the announcement age (seconds, measured from the
	// subtree message's AnnouncedAtUnixMs stamp) beyond which a DataHub 404
	// is attributed to our own consumer lag instead of the peer: teranode's
	// asset cache retains subtree data for a bounded window (~2h), so a 404
	// on an announcement that sat in Kafka past this grace says nothing
	// about the peer's honesty and is not counted against its health. The
	// message itself still routes to the DLQ as a permanent failure — only
	// the peer attribution changes. Messages without a stamp are treated
	// as fresh (404 counted). Default 3600 (see the 2026-07-15 dev-ovh-1
	// incident: lag-aged 404s re-tripped the breaker after every cooldown,
	// ack-and-dropping 100% of announcements in a single-peer topology).
	Stale404GraceSec int `yaml:"stale404GraceSec" mapstructure:"stale404gracesec"`
}

// registerDefaults sets all default values in the Viper instance.
func registerDefaults(v *viper.Viper) {
	// General
	v.SetDefault("mode", "all-in-one")
	v.SetDefault("loglevel", "info")

	// API
	v.SetDefault("api.port", 8080)
	v.SetDefault("api.authtoken", "")
	v.SetDefault("api.requirewatchauth", false)
	v.SetDefault("api.reprocessratelimitrps", 20.0)
	v.SetDefault("api.reprocessburst", 100)
	v.SetDefault("api.maxinflightreprocess", 16)

	// Store
	v.SetDefault("store.backend", BackendAerospike)
	v.SetDefault("store.sql.driver", "postgres")
	v.SetDefault("store.sql.schema", "")
	v.SetDefault("store.sql.sweeperinterval", "60s")
	v.SetDefault("store.sql.maxopenconns", 25)
	v.SetDefault("store.sql.maxidleconns", 5)
	v.SetDefault("store.sql.callbackurlregistryretention", "168h")

	// Aerospike
	v.SetDefault("aerospike.host", "localhost")
	v.SetDefault("aerospike.seeds", []string{})
	v.SetDefault("aerospike.port", 3000)
	v.SetDefault("aerospike.namespace", "merkle")
	v.SetDefault("aerospike.setname", "merkle_registrations")
	v.SetDefault("aerospike.seenset", "merkle_seen_counters")
	v.SetDefault("aerospike.callbackdedupset", "merkle_callback_dedup")
	v.SetDefault("aerospike.callbackurlregistry", "merkle_callback_urls")
	v.SetDefault("aerospike.callbackurlregistryttlsec", 7*24*60*60)
	v.SetDefault("aerospike.subtreecounterset", "merkle_subtree_counters")
	v.SetDefault("aerospike.expectedstumpset", "merkle_expected_stumps")
	// 1h. With Decrement re-stamping the TTL on every subtree, this is an
	// inactivity window, not a hard block-processing deadline — it only fires
	// if a block makes zero subtree progress for a full hour. The former 600s
	// default expired mid-flight on large (tens-of-thousands-of-subtree)
	// blocks even while they were actively progressing.
	v.SetDefault("aerospike.subtreecounterttlsec", 3600)
	v.SetDefault("aerospike.callbackaccumulatorset", "merkle_callback_accum")
	v.SetDefault("aerospike.callbackaccumulatorttlsec", 600)
	v.SetDefault("aerospike.datahubregistry", "merkle_datahub_urls")
	v.SetDefault("aerospike.datahubregistryttlsec", 7*24*60*60)
	v.SetDefault("aerospike.maxretries", 3)
	v.SetDefault("aerospike.retrybasems", 100)
	v.SetDefault("aerospike.connectionqueuesize", 256)
	v.SetDefault("aerospike.minconnectionspernode", 16)
	v.SetDefault("aerospike.limitconnectionstoqueuesize", false)
	v.SetDefault("aerospike.sockettimeoutms", 5000)
	v.SetDefault("aerospike.totaltimeoutms", 15000)
	v.SetDefault("aerospike.readtimeoutms", 3000)
	v.SetDefault("aerospike.writetimeoutms", 5000)
	v.SetDefault("aerospike.batchtimeoutms", 15000)
	v.SetDefault("aerospike.idletimeoutsec", 55)
	v.SetDefault("aerospike.maxerrorrate", 5)
	v.SetDefault("aerospike.errorratewindow", 1)
	v.SetDefault("aerospike.batchconcurrentnodes", 0)
	v.SetDefault("aerospike.batchchunkconcurrency", 8)

	// Kafka
	v.SetDefault("kafka.brokers", []string{"localhost:9092"})
	v.SetDefault("kafka.subtreetopic", "subtree")
	v.SetDefault("kafka.blocktopic", "block")
	v.SetDefault("kafka.callbacktopic", "callback")
	// Empty by default: the SEEN split is INERT until an operator sets
	// kafka.callbackSeenTopic (or KAFKA_CALLBACK_SEEN_TOPIC). Empty ⇒
	// SeenCallbackTopic() falls back to 'callback' and behavior is unchanged.
	v.SetDefault("kafka.callbackseentopic", "")
	v.SetDefault("kafka.callbackdlqtopic", "callback-dlq")
	v.SetDefault("kafka.subtreedlqtopic", "subtree-dlq")
	v.SetDefault("kafka.subtreeworktopic", "subtree-work")
	v.SetDefault("kafka.subtreeworkdlqtopic", "subtree-work-dlq")
	v.SetDefault("kafka.consumergroup", "merkle-service")
	v.SetDefault("kafka.subtreepartitions", defaultSubtreePartitions)
	v.SetDefault("kafka.subtreeworkpartitions", defaultSubtreeWorkPartitions)
	v.SetDefault("kafka.callbackseenpartitions", defaultCallbackSeenPartitions)
	v.SetDefault("kafka.topicretentionms", defaultTopicRetentionMs)
	v.SetDefault("kafka.dlqretentionms", defaultDLQRetentionMs)

	// P2P
	v.SetDefault("p2p.network", "main")
	v.SetDefault("p2p.storagepath", "~/.merkle-service/p2p")
	v.SetDefault("p2p.msgbus.dhtmode", "off")
	v.SetDefault("p2p.msgbus.port", 9905)
	v.SetDefault("p2p.msgbus.enablenat", false)
	v.SetDefault("p2p.msgbus.enablemdns", false)

	// Subtree
	v.SetDefault("subtree.storagemode", "realtime")
	v.SetDefault("subtree.dahoffset", 1)
	v.SetDefault("subtree.stumpdahoffset", 6)
	v.SetDefault("subtree.cachemaxmb", 64)
	v.SetDefault("subtree.dedupcachesize", 100000)
	// 10 was too aggressive — when Aerospike has a slow node, every retry
	// re-fans-out a BatchGet that piles up on the bad node. 3 attempts
	// (initial + 2 retries) gives us recovery from transient blips without
	// turning into a self-inflicted DoS.
	v.SetDefault("subtree.maxattempts", 3)
	// 1s base, doubling per attempt (cap 30s): 3 attempts span ~3s of real
	// time instead of ~300ms, giving transient blob-store/DataHub conditions
	// a chance to clear before the DLQ.
	v.SetDefault("subtree.retrybackoffbasems", 1000)
	v.SetDefault("subtree.seentxidlogmax", 1000)

	// Block
	v.SetDefault("block.workerpoolsize", 16)
	v.SetDefault("block.postminettlsec", 1800)
	v.SetDefault("block.dedupcachesize", 10000)
	v.SetDefault("block.maxattempts", 10)
	// 1s base, doubling per attempt (cap 5s): a 10-attempt budget spans ~37s
	// of deliberate wait (plus per-republish backlog traversal) instead of
	// milliseconds — see BlockConfig.RetryBackoffBaseMs.
	v.SetDefault("block.retrybackoffbasems", 1000)
	// Reduced budget for DataHub 404s (see BlockConfig.NotFoundMaxAttempts).
	v.SetDefault("block.notfoundmaxattempts", 3)
	v.SetDefault("block.regcachemaxmb", 64)
	v.SetDefault("block.batchgetconcurrency", 4)
	// Off-switch, not a rollout gate — see BlockConfig.EmitExpectedStumpSet.
	v.SetDefault("block.emitexpectedstumpset", true)

	// Callback
	v.SetDefault("callback.maxretries", 5)
	v.SetDefault("callback.breakerthreshold", 20)
	v.SetDefault("callback.backoffbasesec", 30)
	v.SetDefault("callback.timeoutsec", 10)
	v.SetDefault("callback.seenthreshold", 3)
	v.SetDefault("callback.dedupttlsec", 86400)
	v.SetDefault("callback.maxconnsperhost", 32)
	v.SetDefault("callback.maxidleconnsperhost", 16)
	v.SetDefault("callback.allowprivateips", false)

	// BlobStore
	v.SetDefault("blobstore.url", "file:///tmp/merkle-subtrees")
	// Age sweeper: aggressive by design. Subtree blobs are a re-fetchable
	// cache, and the 2026-07-15 dev-ovh-1 incident filled a 1TiB volume in
	// ~3h with orphans the DAH path could never reach. 1800s ≈ 3 blocks at
	// ~10min cadence; a 300s interval bounds orphan buildup between sweeps.
	v.SetDefault("blobstore.sweepintervalsec", 300)
	v.SetDefault("blobstore.sweepmaxagesec", 1800)

	// DataHub
	// Defaults tuned so a known-bad peer is dropped in seconds, not minutes.
	// Worst case per peer: 10s + 500ms backoff + 10s ≈ 20.5s before failover.
	v.SetDefault("datahub.timeoutsec", 10)
	v.SetDefault("datahub.maxretries", 1)
	// 16 MiB — block metadata is small; this leaves ~100x headroom over the
	// largest realistic block-metadata payload while still bounding memory.
	v.SetDefault("datahub.maxblockbytes", int64(16*1024*1024))
	// 1 GiB — accommodates Teranode subtrees up to ~33.5M txids; operators
	// running with smaller per-subtree limits should tune this down.
	v.SetDefault("datahub.maxsubtreebytes", int64(1*1024*1024*1024))
	// SSRF guard default: deny private/loopback/link-local destinations.
	v.SetDefault("datahub.allowprivateips", false)
	v.SetDefault("datahub.fallbackurls", []string{})
	// Peer-health tracker defaults: 3 consecutive failures → 5m unhealthy.
	v.SetDefault("datahub.peerhealth.failurethreshold", 3)
	v.SetDefault("datahub.peerhealth.cooldownsec", 300)
	// 404s on announcements older than an hour are our consumer lag, not a
	// lying peer (teranode's asset cache holds ~2h) — don't count them.
	v.SetDefault("datahub.peerhealth.stale404gracesec", 3600)

	// Registry
	v.SetDefault("registry.maxcallbackspertxid", 10)

	// Metrics
	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.port", 9090)
	v.SetDefault("metrics.path", "/metrics")

	// Telemetry: off by default. See TelemetryConfig doc comment. Every
	// field gets an explicit SetDefault — including the zero-valued ones —
	// because viper's AutomaticEnv only recognizes TELEMETRY_* overrides for
	// keys it already knows about; without these, TELEMETRY_ENDPOINT /
	// TELEMETRY_NAMESPACE / TELEMETRY_INSECURE would silently be ignored.
	v.SetDefault("telemetry.enabled", false)
	v.SetDefault("telemetry.endpoint", "")
	v.SetDefault("telemetry.protocol", "grpc")
	v.SetDefault("telemetry.insecure", false)
	v.SetDefault("telemetry.servicename", "merkle-service")
	v.SetDefault("telemetry.namespace", "")
	v.SetDefault("telemetry.traces", true)
	v.SetDefault("telemetry.metrics", true)
	v.SetDefault("telemetry.sampleratio", 1.0)
	v.SetDefault("telemetry.exporttimeoutms", 10000)
}

// bindEnvVars explicitly binds environment variable names to Viper keys.
// This handles cases where the automatic dot-to-underscore mapping doesn't
// produce the expected env var name.
func bindEnvVars(v *viper.Viper) {
	// The general pattern is: key "section.field" → env "SECTION_FIELD"
	// Viper's AutomaticEnv with the replacer handles most of these,
	// but we bind explicitly for clarity and to ensure correct mapping.

	bindings := map[string]string{
		// General
		"mode":     "MODE",
		"loglevel": "LOG_LEVEL",

		// API
		"api.port":                  "API_PORT",
		"api.authtoken":             "API_AUTH_TOKEN", //#nosec G101 -- viper config key → env-var name mapping, not a hardcoded credential value
		"api.requirewatchauth":      "API_REQUIRE_WATCH_AUTH",
		"api.reprocessratelimitrps": "API_REPROCESS_RATE_LIMIT_RPS",
		"api.reprocessburst":        "API_REPROCESS_BURST",
		"api.maxinflightreprocess":  "API_MAX_INFLIGHT_REPROCESS",

		// Store
		"store.backend":                          "STORE_BACKEND",
		"store.sql.driver":                       "STORE_SQL_DRIVER",
		"store.sql.dsn":                          "STORE_SQL_DSN",
		"store.sql.schema":                       "STORE_SQL_SCHEMA",
		"store.sql.sweeperinterval":              "STORE_SQL_SWEEPER_INTERVAL",
		"store.sql.maxopenconns":                 "STORE_SQL_MAX_OPEN_CONNS",
		"store.sql.maxidleconns":                 "STORE_SQL_MAX_IDLE_CONNS",
		"store.sql.callbackurlregistryretention": "STORE_SQL_CALLBACK_URL_REGISTRY_RETENTION",

		// Aerospike
		"aerospike.host":                        "AEROSPIKE_HOST",
		"aerospike.seeds":                       "AEROSPIKE_SEEDS",
		"aerospike.port":                        "AEROSPIKE_PORT",
		"aerospike.namespace":                   "AEROSPIKE_NAMESPACE",
		"aerospike.setname":                     "AEROSPIKE_SET",
		"aerospike.seenset":                     "AEROSPIKE_SEEN_SET",
		"aerospike.callbackdedupset":            "AEROSPIKE_CALLBACK_DEDUP_SET",
		"aerospike.callbackurlregistry":         "AEROSPIKE_CALLBACK_URL_REGISTRY",
		"aerospike.callbackurlregistryttlsec":   "AEROSPIKE_CALLBACK_URL_REGISTRY_TTL_SEC",
		"aerospike.subtreecounterset":           "AEROSPIKE_SUBTREE_COUNTER_SET",
		"aerospike.subtreecounterttlsec":        "AEROSPIKE_SUBTREE_COUNTER_TTL_SEC",
		"aerospike.expectedstumpset":            "AEROSPIKE_EXPECTED_STUMP_SET",
		"aerospike.callbackaccumulatorset":      "AEROSPIKE_CALLBACK_ACCUMULATOR_SET",
		"aerospike.callbackaccumulatorttlsec":   "AEROSPIKE_CALLBACK_ACCUMULATOR_TTL_SEC",
		"aerospike.datahubregistry":             "AEROSPIKE_DATAHUB_REGISTRY",
		"aerospike.datahubregistryttlsec":       "AEROSPIKE_DATAHUB_REGISTRY_TTL_SEC",
		"aerospike.maxretries":                  "AEROSPIKE_MAX_RETRIES",
		"aerospike.retrybasems":                 "AEROSPIKE_RETRY_BASE_MS",
		"aerospike.connectionqueuesize":         "AEROSPIKE_CONNECTION_QUEUE_SIZE",
		"aerospike.minconnectionspernode":       "AEROSPIKE_MIN_CONNECTIONS_PER_NODE",
		"aerospike.limitconnectionstoqueuesize": "AEROSPIKE_LIMIT_CONNECTIONS_TO_QUEUE_SIZE",
		"aerospike.sockettimeoutms":             "AEROSPIKE_SOCKET_TIMEOUT_MS",
		"aerospike.totaltimeoutms":              "AEROSPIKE_TOTAL_TIMEOUT_MS",
		"aerospike.readtimeoutms":               "AEROSPIKE_READ_TIMEOUT_MS",
		"aerospike.writetimeoutms":              "AEROSPIKE_WRITE_TIMEOUT_MS",
		"aerospike.batchtimeoutms":              "AEROSPIKE_BATCH_TIMEOUT_MS",
		"aerospike.idletimeoutsec":              "AEROSPIKE_IDLE_TIMEOUT_SEC",
		"aerospike.maxerrorrate":                "AEROSPIKE_MAX_ERROR_RATE",
		"aerospike.errorratewindow":             "AEROSPIKE_ERROR_RATE_WINDOW",

		// Kafka
		"kafka.brokers":             "KAFKA_BROKERS",
		"kafka.subtreetopic":        "KAFKA_SUBTREE_TOPIC",
		"kafka.blocktopic":          "KAFKA_BLOCK_TOPIC",
		"kafka.callbacktopic":       "KAFKA_CALLBACK_TOPIC",
		"kafka.callbackseentopic":   "KAFKA_CALLBACK_SEEN_TOPIC",
		"kafka.callbackdlqtopic":    "KAFKA_CALLBACK_DLQ_TOPIC",
		"kafka.subtreedlqtopic":     "KAFKA_SUBTREE_DLQ_TOPIC",
		"kafka.subtreeworktopic":    "KAFKA_SUBTREE_WORK_TOPIC",
		"kafka.subtreeworkdlqtopic": "KAFKA_SUBTREE_WORK_DLQ_TOPIC",
		"kafka.consumergroup":       "KAFKA_CONSUMER_GROUP",
		"kafka.topicretentionms":    "KAFKA_TOPIC_RETENTION_MS",
		"kafka.dlqretentionms":      "KAFKA_DLQ_RETENTION_MS",

		"kafka.callbackseenpartitions": "KAFKA_CALLBACK_SEEN_PARTITIONS",

		// P2P
		"p2p.network":               "P2P_NETWORK",
		"p2p.storagepath":           "P2P_STORAGE_PATH",
		"p2p.msgbus.dhtmode":        "P2P_DHT_MODE",
		"p2p.msgbus.port":           "P2P_PORT",
		"p2p.msgbus.announceaddrs":  "P2P_ANNOUNCE_ADDRS",
		"p2p.msgbus.bootstrappeers": "P2P_BOOTSTRAP_PEERS",
		"p2p.msgbus.maxconnections": "P2P_MAX_CONNECTIONS",
		"p2p.msgbus.minconnections": "P2P_MIN_CONNECTIONS",
		"p2p.msgbus.enablenat":      "P2P_ENABLE_NAT",
		"p2p.msgbus.enablemdns":     "P2P_ENABLE_MDNS",

		// Subtree
		"subtree.storagemode":        "SUBTREE_STORAGE_MODE",
		"subtree.dahoffset":          "SUBTREE_DAH_OFFSET",
		"subtree.stumpdahoffset":     "SUBTREE_STUMP_DAH_OFFSET",
		"subtree.cachemaxmb":         "SUBTREE_CACHE_MAX_MB",
		"subtree.dedupcachesize":     "SUBTREE_DEDUP_CACHE_SIZE",
		"subtree.maxattempts":        "SUBTREE_MAX_ATTEMPTS",
		"subtree.retrybackoffbasems": "SUBTREE_RETRY_BACKOFF_BASE_MS",
		"subtree.seentxidlogmax":     "SUBTREE_SEEN_TXID_LOG_MAX",

		// Block
		"block.workerpoolsize":       "BLOCK_WORKER_POOL_SIZE",
		"block.postminettlsec":       "BLOCK_POST_MINE_TTL_SEC",
		"block.dedupcachesize":       "BLOCK_DEDUP_CACHE_SIZE",
		"block.maxattempts":          "BLOCK_MAX_ATTEMPTS",
		"block.retrybackoffbasems":   "BLOCK_RETRY_BACKOFF_BASE_MS",
		"block.notfoundmaxattempts":  "BLOCK_NOT_FOUND_MAX_ATTEMPTS",
		"block.regcachemaxmb":        "BLOCK_REG_CACHE_MAX_MB",
		"block.batchgetconcurrency":  "BLOCK_BATCH_GET_CONCURRENCY",
		"block.emitexpectedstumpset": "BLOCK_EMIT_EXPECTED_STUMP_SET",

		// Callback
		"callback.maxretries":          "CALLBACK_MAX_RETRIES",
		"callback.backoffbasesec":      "CALLBACK_BACKOFF_BASE_SEC",
		"callback.timeoutsec":          "CALLBACK_TIMEOUT_SEC",
		"callback.seenthreshold":       "CALLBACK_SEEN_THRESHOLD",
		"callback.dedupttlsec":         "CALLBACK_DEDUP_TTL_SEC",
		"callback.maxconnsperhost":     "CALLBACK_MAX_CONNS_PER_HOST",
		"callback.maxidleconnsperhost": "CALLBACK_MAX_IDLE_CONNS_PER_HOST",
		"callback.allowprivateips":     "CALLBACK_ALLOW_PRIVATE_IPS",

		// BlobStore
		"blobstore.url":              "BLOB_STORE_URL",
		"blobstore.sweepintervalsec": "BLOB_STORE_SWEEP_INTERVAL_SEC",
		"blobstore.sweepmaxagesec":   "BLOB_STORE_SWEEP_MAX_AGE_SEC",

		// DataHub
		"datahub.timeoutsec":                  "DATAHUB_TIMEOUT_SEC",
		"datahub.maxretries":                  "DATAHUB_MAX_RETRIES",
		"datahub.maxblockbytes":               "DATAHUB_MAX_BLOCK_BYTES",
		"datahub.maxsubtreebytes":             "DATAHUB_MAX_SUBTREE_BYTES",
		"datahub.allowprivateips":             "DATAHUB_ALLOW_PRIVATE_IPS",
		"datahub.fallbackurls":                "DATAHUB_FALLBACK_URLS",
		"datahub.peerhealth.failurethreshold": "DATAHUB_PEER_HEALTH_FAILURE_THRESHOLD",
		"datahub.peerhealth.cooldownsec":      "DATAHUB_PEER_HEALTH_COOLDOWN_SEC",
		"datahub.peerhealth.stale404gracesec": "DATAHUB_PEER_HEALTH_STALE404_GRACE_SEC",

		// Registry
		"registry.maxcallbackspertxid": "REGISTRY_MAX_CALLBACKS_PER_TXID",

		// Metrics
		"metrics.enabled": "METRICS_ENABLED",
		"metrics.port":    "METRICS_PORT",
		"metrics.path":    "METRICS_PATH",

		// Telemetry
		"telemetry.enabled":         "TELEMETRY_ENABLED",
		"telemetry.endpoint":        "TELEMETRY_ENDPOINT",
		"telemetry.protocol":        "TELEMETRY_PROTOCOL",
		"telemetry.insecure":        "TELEMETRY_INSECURE",
		"telemetry.servicename":     "TELEMETRY_SERVICE_NAME",
		"telemetry.namespace":       "TELEMETRY_NAMESPACE",
		"telemetry.traces":          "TELEMETRY_TRACES",
		"telemetry.metrics":         "TELEMETRY_METRICS",
		"telemetry.sampleratio":     "TELEMETRY_SAMPLE_RATIO",
		"telemetry.exporttimeoutms": "TELEMETRY_EXPORT_TIMEOUT_MS",
	}

	for key, env := range bindings {
		_ = v.BindEnv(key, env)
	}
}

// Load reads configuration from defaults, YAML file, and environment variables.
// Priority order: env vars > YAML file > defaults.
func Load() (*Config, error) {
	v := viper.New()

	// Register defaults.
	registerDefaults(v)

	// Bind environment variables before reading config file.
	bindEnvVars(v)

	// Configure config file path.
	// Check CONFIG_FILE env var directly (not via Viper since it's not a config key).
	if configFile := os.Getenv("CONFIG_FILE"); configFile != "" {
		v.SetConfigFile(configFile)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
	}

	// Read config file (ignore file-not-found).
	if err := v.ReadInConfig(); err != nil {
		var notFoundErr viper.ConfigFileNotFoundError
		if errors.As(err, &notFoundErr) {
			// No config file found — use defaults + env vars only.
		} else if os.IsNotExist(err) {
			// Explicit CONFIG_FILE path doesn't exist — use defaults + env vars only.
		} else {
			// File exists but is invalid (parse error).
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	// Unmarshal into Config struct.
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Normalize + validate store backend.
	if cfg.Store.Backend == "" {
		cfg.Store.Backend = BackendAerospike
	}
	switch cfg.Store.Backend {
	case BackendAerospike, BackendSQL:
	default:
		return nil, fmt.Errorf("invalid store.backend %q: must be %q or %q", cfg.Store.Backend, BackendAerospike, BackendSQL)
	}

	// SubtreeCounterTTLSec must be strictly positive. Zero or negative values
	// silently produce dangerous Aerospike record TTLs: 0 means "use namespace
	// default" (may be never-expire), and a negative int truncated to uint32
	// becomes the Aerospike never-expire sentinel — either way the counter
	// becomes a permanent record that the sweeper never reclaims.
	if cfg.Aerospike.SubtreeCounterTTLSec <= 0 {
		return nil, fmt.Errorf("invalid aerospike.subtreeCounterTTLSec %d: must be > 0", cfg.Aerospike.SubtreeCounterTTLSec)
	}

	// Blob-store sweeper validation. The age floor is the safety property:
	// the sweeper deletes by mtime, and a nonzero max age below one block
	// interval (~600s) could remove the in-flight block's subtree blobs
	// while the block-processor is still assembling STUMPs from them. 0 is
	// legal for both keys — it disables the sweeper outright.
	if cfg.BlobStore.SweepIntervalSec < 0 {
		return nil, fmt.Errorf("invalid blobStore.sweepIntervalSec %d: must be >= 0 (0 disables the sweeper)", cfg.BlobStore.SweepIntervalSec)
	}
	if cfg.BlobStore.SweepMaxAgeSec != 0 && cfg.BlobStore.SweepMaxAgeSec < 600 {
		return nil, fmt.Errorf("invalid blobStore.sweepMaxAgeSec %d: must be 0 (disabled) or >= 600 — a sweep age below one block interval could delete the in-flight block's subtree blobs", cfg.BlobStore.SweepMaxAgeSec)
	}

	// Telemetry validation. telemetry.endpoint is intentionally NOT required
	// here: an empty value falls back to the standard
	// OTEL_EXPORTER_OTLP_ENDPOINT / OTEL_EXPORTER_OTLP_{TRACES,METRICS}_ENDPOINT
	// env vars, resolved inside telemetry.Init (which errors if neither is set).
	if cfg.Telemetry.Enabled {
		switch cfg.Telemetry.Protocol {
		case "grpc", "http":
		default:
			return nil, fmt.Errorf("invalid telemetry.protocol %q: must be %q or %q", cfg.Telemetry.Protocol, "grpc", "http")
		}
		if cfg.Telemetry.SampleRatio < 0 || cfg.Telemetry.SampleRatio > 1 {
			return nil, fmt.Errorf("invalid telemetry.sampleratio %v: must be between 0 and 1", cfg.Telemetry.SampleRatio)
		}
		// Both the gRPC and HTTP exporters' WithEndpoint dial the value
		// verbatim as host:port; a scheme prefix makes the dial fail silently
		// in the background exporter goroutine (for HTTP the scheme is instead
		// controlled by telemetry.insecure), so fail fast here for either
		// protocol. Only the OTEL_EXPORTER_OTLP_ENDPOINT env var understands a
		// scheme-prefixed URL.
		if strings.HasPrefix(cfg.Telemetry.Endpoint, "http://") || strings.HasPrefix(cfg.Telemetry.Endpoint, "https://") {
			return nil, fmt.Errorf("invalid telemetry.endpoint %q: must be host:port without a scheme "+
				"(e.g. \"collector:4317\"); scheme-prefixed URLs are only understood by the OTEL_EXPORTER_OTLP_ENDPOINT env var",
				cfg.Telemetry.Endpoint)
		}
	}

	// API auth validation. Enabling /watch auth without a token is a
	// misconfiguration: the middleware fails open when the token is empty, so
	// the operator would believe /watch is protected while it silently is not.
	// Fail fast rather than present a false sense of security.
	if cfg.API.RequireWatchAuth && cfg.API.AuthToken == "" {
		return nil, fmt.Errorf("invalid api config: api.requireWatchAuth is true but api.authToken is empty " +
			"(auth fails open with no token, so /watch would be unprotected); set api.authToken or disable api.requireWatchAuth")
	}
	// A positive rate limit with a non-positive burst would reject every
	// request (token bucket can never fill), silently taking /reprocess down.
	if cfg.API.ReprocessRateLimitRps > 0 && cfg.API.ReprocessBurst <= 0 {
		return nil, fmt.Errorf("invalid api.reprocessBurst %d: must be > 0 when api.reprocessRateLimitRps > 0", cfg.API.ReprocessBurst)
	}

	return &cfg, nil
}

// ParseLogLevel converts a log level string to slog.Level.
// Supports "debug", "info", "warn", "error" (case-insensitive).
// Returns slog.LevelInfo for unrecognized values.
func ParseLogLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
