package config

import (
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
	Backend string          `yaml:"backend" mapstructure:"backend"`
	SQL     StoreSQLConfig  `yaml:"sql"     mapstructure:"sql"`
}

// StoreSQLConfig configures the SQL backend.
type StoreSQLConfig struct {
	Driver          string `yaml:"driver"          mapstructure:"driver"`
	DSN             string `yaml:"dsn"             mapstructure:"dsn"`
	Schema          string `yaml:"schema"          mapstructure:"schema"`
	SweeperInterval string `yaml:"sweeperInterval" mapstructure:"sweeperinterval"`
	MaxOpenConns    int    `yaml:"maxOpenConns"    mapstructure:"maxopenconns"`
	MaxIdleConns    int    `yaml:"maxIdleConns"    mapstructure:"maxidleconns"`
	// CallbackURLRegistryRetention bounds the SQL callback URL registry. URLs
	// whose last `Add` is older than this are dropped by the TTL sweeper.
	// Format: any time.ParseDuration string ("168h", "7d" is NOT supported).
	// Default 168h (7 days). See F-037 / issue #23.
	CallbackURLRegistryRetention string `yaml:"callbackUrlRegistryRetention" mapstructure:"callbackurlregistryretention"`
}

// APIConfig holds HTTP API configuration.
type APIConfig struct {
	Port int `yaml:"port" mapstructure:"port"`
}

// AerospikeConfig holds Aerospike connection configuration.
type AerospikeConfig struct {
	// Host is a single seed node address. Kept for backward compatibility;
	// prefer Seeds for production (multiple seeds protect against the seed
	// node's connection pool becoming the bottleneck).
	Host string `yaml:"host" mapstructure:"host"`
	// Seeds is a list of seed node addresses. When non-empty, Host is ignored.
	Seeds                []string `yaml:"seeds"                mapstructure:"seeds"`
	Port                 int      `yaml:"port"                 mapstructure:"port"`
	Namespace            string   `yaml:"namespace"            mapstructure:"namespace"`
	SetName              string   `yaml:"setName"              mapstructure:"setname"`
	SeenSet              string   `yaml:"seenSet"              mapstructure:"seenset"`
	CallbackDedupSet     string   `yaml:"callbackDedupSet"     mapstructure:"callbackdedupset"`
	CallbackURLRegistry  string   `yaml:"callbackUrlRegistry"  mapstructure:"callbackurlregistry"`
	// CallbackURLRegistryTTLSec is the per-URL eviction window applied by the
	// Aerospike callback URL registry (and the SQL sibling). URLs whose last
	// `Add` is older than this are evicted, bounding the registry's growth so
	// BLOCK_PROCESSED fan-out and the underlying record(s) never grow without
	// limit. Default 7 days. See F-037 / issue #23.
	CallbackURLRegistryTTLSec int `yaml:"callbackUrlRegistryTTLSec" mapstructure:"callbackurlregistryttlsec"`
	SubtreeCounterSet    string   `yaml:"subtreeCounterSet"    mapstructure:"subtreecounterset"`
	SubtreeCounterTTLSec int      `yaml:"subtreeCounterTTLSec" mapstructure:"subtreecounterttlsec"`
	CallbackAccumulatorSet    string `yaml:"callbackAccumulatorSet"    mapstructure:"callbackaccumulatorset"`
	CallbackAccumulatorTTLSec int    `yaml:"callbackAccumulatorTTLSec" mapstructure:"callbackaccumulatorttlsec"`
	MaxRetries  int `yaml:"maxRetries"  mapstructure:"maxretries"`
	RetryBaseMs int `yaml:"retryBaseMs" mapstructure:"retrybasems"`
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
type KafkaConfig struct {
	Brokers             []string `yaml:"brokers"             mapstructure:"brokers"`
	SubtreeTopic        string   `yaml:"subtreeTopic"        mapstructure:"subtreetopic"`
	BlockTopic          string   `yaml:"blockTopic"          mapstructure:"blocktopic"`
	CallbackTopic       string   `yaml:"callbackTopic"       mapstructure:"callbacktopic"`
	CallbackDLQTopic    string   `yaml:"callbackDlqTopic"    mapstructure:"callbackdlqtopic"`
	SubtreeDLQTopic     string   `yaml:"subtreeDlqTopic"     mapstructure:"subtreedlqtopic"`
	SubtreeWorkTopic    string   `yaml:"subtreeWorkTopic"    mapstructure:"subtreeworktopic"`
	SubtreeWorkDLQTopic string   `yaml:"subtreeWorkDlqTopic" mapstructure:"subtreeworkdlqtopic"`
	ConsumerGroup       string   `yaml:"consumerGroup"       mapstructure:"consumergroup"`
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
	// RegCacheMaxMB sizes the in-process registration deduplication cache used
	// by block-time subtree processing. Same shape as Subtree.CacheMaxMB but
	// independent so all-in-one and microservice topologies behave identically.
	RegCacheMaxMB int `yaml:"regCacheMaxMB" mapstructure:"regcachemaxmb"`
	// BatchGetConcurrency caps the number of concurrent registration BatchGet
	// calls per subtree-worker process. Without this limit a single block
	// announcement can fire one BatchGet per subtree (up to 30+) in parallel
	// and exhaust the Aerospike connection pool.
	BatchGetConcurrency int `yaml:"batchGetConcurrency" mapstructure:"batchgetconcurrency"`
}

// CallbackConfig holds callback delivery configuration.
type CallbackConfig struct {
	MaxRetries          int `yaml:"maxRetries"          mapstructure:"maxretries"`
	BackoffBaseSec      int `yaml:"backoffBaseSec"      mapstructure:"backoffbasesec"`
	TimeoutSec          int `yaml:"timeoutSec"          mapstructure:"timeoutsec"`
	SeenThreshold       int `yaml:"seenThreshold"       mapstructure:"seenthreshold"`
	DedupTTLSec         int `yaml:"dedupTTLSec"         mapstructure:"dedupttlsec"`
	DeliveryWorkers     int `yaml:"deliveryWorkers"     mapstructure:"deliveryworkers"`
	MaxConnsPerHost     int `yaml:"maxConnsPerHost"     mapstructure:"maxconnsperhost"`
	MaxIdleConnsPerHost int `yaml:"maxIdleConnsPerHost" mapstructure:"maxidleconnsperhost"`
}

// BlobStoreConfig holds blob store configuration.
type BlobStoreConfig struct {
	URL string `yaml:"url" mapstructure:"url"`
}

// DataHubConfig holds DataHub HTTP client configuration.
type DataHubConfig struct {
	TimeoutSec int `yaml:"timeoutSec" mapstructure:"timeoutsec"`
	MaxRetries int `yaml:"maxRetries" mapstructure:"maxretries"`
}

// registerDefaults sets all default values in the Viper instance.
func registerDefaults(v *viper.Viper) {
	// General
	v.SetDefault("mode", "all-in-one")
	v.SetDefault("loglevel", "info")

	// API
	v.SetDefault("api.port", 8080)

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
	v.SetDefault("aerospike.subtreecounterttlsec", 600)
	v.SetDefault("aerospike.callbackaccumulatorset", "merkle_callback_accum")
	v.SetDefault("aerospike.callbackaccumulatorttlsec", 600)
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

	// Kafka
	v.SetDefault("kafka.brokers", []string{"localhost:9092"})
	v.SetDefault("kafka.subtreetopic", "subtree")
	v.SetDefault("kafka.blocktopic", "block")
	v.SetDefault("kafka.callbacktopic", "callback")
	v.SetDefault("kafka.callbackdlqtopic", "callback-dlq")
	v.SetDefault("kafka.subtreedlqtopic", "subtree-dlq")
	v.SetDefault("kafka.subtreeworktopic", "subtree-work")
	v.SetDefault("kafka.subtreeworkdlqtopic", "subtree-work-dlq")
	v.SetDefault("kafka.consumergroup", "merkle-service")

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

	// Block
	v.SetDefault("block.workerpoolsize", 16)
	v.SetDefault("block.postminettlsec", 1800)
	v.SetDefault("block.dedupcachesize", 10000)
	v.SetDefault("block.maxattempts", 10)
	v.SetDefault("block.regcachemaxmb", 64)
	v.SetDefault("block.batchgetconcurrency", 4)

	// Callback
	v.SetDefault("callback.maxretries", 5)
	v.SetDefault("callback.backoffbasesec", 30)
	v.SetDefault("callback.timeoutsec", 10)
	v.SetDefault("callback.seenthreshold", 3)
	v.SetDefault("callback.dedupttlsec", 86400)
	v.SetDefault("callback.deliveryworkers", 64)
	v.SetDefault("callback.maxconnsperhost", 32)
	v.SetDefault("callback.maxidleconnsperhost", 16)

	// BlobStore
	v.SetDefault("blobstore.url", "file:///tmp/merkle-subtrees")

	// DataHub
	v.SetDefault("datahub.timeoutsec", 30)
	v.SetDefault("datahub.maxretries", 3)
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
		"api.port": "API_PORT",

		// Store
		"store.backend":             "STORE_BACKEND",
		"store.sql.driver":          "STORE_SQL_DRIVER",
		"store.sql.dsn":             "STORE_SQL_DSN",
		"store.sql.schema":          "STORE_SQL_SCHEMA",
		"store.sql.sweeperinterval": "STORE_SQL_SWEEPER_INTERVAL",
		"store.sql.maxopenconns":    "STORE_SQL_MAX_OPEN_CONNS",
		"store.sql.maxidleconns":    "STORE_SQL_MAX_IDLE_CONNS",
		"store.sql.callbackurlregistryretention": "STORE_SQL_CALLBACK_URL_REGISTRY_RETENTION",

		// Aerospike
		"aerospike.host":      "AEROSPIKE_HOST",
		"aerospike.seeds":     "AEROSPIKE_SEEDS",
		"aerospike.port":      "AEROSPIKE_PORT",
		"aerospike.namespace": "AEROSPIKE_NAMESPACE",
		"aerospike.setname":   "AEROSPIKE_SET",
		"aerospike.seenset":             "AEROSPIKE_SEEN_SET",
		"aerospike.callbackdedupset":    "AEROSPIKE_CALLBACK_DEDUP_SET",
		"aerospike.callbackurlregistry":         "AEROSPIKE_CALLBACK_URL_REGISTRY",
		"aerospike.callbackurlregistryttlsec":   "AEROSPIKE_CALLBACK_URL_REGISTRY_TTL_SEC",
		"aerospike.subtreecounterset":         "AEROSPIKE_SUBTREE_COUNTER_SET",
		"aerospike.subtreecounterttlsec":      "AEROSPIKE_SUBTREE_COUNTER_TTL_SEC",
		"aerospike.callbackaccumulatorset":    "AEROSPIKE_CALLBACK_ACCUMULATOR_SET",
		"aerospike.callbackaccumulatorttlsec": "AEROSPIKE_CALLBACK_ACCUMULATOR_TTL_SEC",
		"aerospike.maxretries":                "AEROSPIKE_MAX_RETRIES",
		"aerospike.retrybasems":               "AEROSPIKE_RETRY_BASE_MS",
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
		"kafka.brokers":        "KAFKA_BROKERS",
		"kafka.subtreetopic":   "KAFKA_SUBTREE_TOPIC",
		"kafka.blocktopic":     "KAFKA_BLOCK_TOPIC",
		"kafka.callbacktopic":    "KAFKA_CALLBACK_TOPIC",
		"kafka.callbackdlqtopic": "KAFKA_CALLBACK_DLQ_TOPIC",
		"kafka.subtreedlqtopic":  "KAFKA_SUBTREE_DLQ_TOPIC",
		"kafka.subtreeworktopic":    "KAFKA_SUBTREE_WORK_TOPIC",
		"kafka.subtreeworkdlqtopic": "KAFKA_SUBTREE_WORK_DLQ_TOPIC",
		"kafka.consumergroup":       "KAFKA_CONSUMER_GROUP",

		// P2P
		"p2p.network":     "P2P_NETWORK",
		"p2p.storagepath": "P2P_STORAGE_PATH",
		"p2p.msgbus.dhtmode":        "P2P_DHT_MODE",
		"p2p.msgbus.port":           "P2P_PORT",
		"p2p.msgbus.announceaddrs":  "P2P_ANNOUNCE_ADDRS",
		"p2p.msgbus.bootstrappeers": "P2P_BOOTSTRAP_PEERS",
		"p2p.msgbus.maxconnections": "P2P_MAX_CONNECTIONS",
		"p2p.msgbus.minconnections": "P2P_MIN_CONNECTIONS",
		"p2p.msgbus.enablenat":      "P2P_ENABLE_NAT",
		"p2p.msgbus.enablemdns":     "P2P_ENABLE_MDNS",

		// Subtree
		"subtree.storagemode":    "SUBTREE_STORAGE_MODE",
		"subtree.dahoffset":      "SUBTREE_DAH_OFFSET",
		"subtree.stumpdahoffset": "SUBTREE_STUMP_DAH_OFFSET",
		"subtree.cachemaxmb":     "SUBTREE_CACHE_MAX_MB",
		"subtree.dedupcachesize": "SUBTREE_DEDUP_CACHE_SIZE",
		"subtree.maxattempts":    "SUBTREE_MAX_ATTEMPTS",

		// Block
		"block.workerpoolsize":      "BLOCK_WORKER_POOL_SIZE",
		"block.postminettlsec":      "BLOCK_POST_MINE_TTL_SEC",
		"block.dedupcachesize":      "BLOCK_DEDUP_CACHE_SIZE",
		"block.maxattempts":         "BLOCK_MAX_ATTEMPTS",
		"block.regcachemaxmb":       "BLOCK_REG_CACHE_MAX_MB",
		"block.batchgetconcurrency": "BLOCK_BATCH_GET_CONCURRENCY",

		// Callback
		"callback.maxretries":     "CALLBACK_MAX_RETRIES",
		"callback.backoffbasesec": "CALLBACK_BACKOFF_BASE_SEC",
		"callback.timeoutsec":     "CALLBACK_TIMEOUT_SEC",
		"callback.seenthreshold":  "CALLBACK_SEEN_THRESHOLD",
		"callback.dedupttlsec":         "CALLBACK_DEDUP_TTL_SEC",
		"callback.deliveryworkers":     "CALLBACK_DELIVERY_WORKERS",
		"callback.maxconnsperhost":     "CALLBACK_MAX_CONNS_PER_HOST",
		"callback.maxidleconnsperhost": "CALLBACK_MAX_IDLE_CONNS_PER_HOST",

		// BlobStore
		"blobstore.url": "BLOB_STORE_URL",

		// DataHub
		"datahub.timeoutsec": "DATAHUB_TIMEOUT_SEC",
		"datahub.maxretries": "DATAHUB_MAX_RETRIES",
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
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
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
