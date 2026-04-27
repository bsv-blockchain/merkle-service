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
}

// APIConfig holds HTTP API configuration.
type APIConfig struct {
	Port int `yaml:"port" mapstructure:"port"`
}

// AerospikeConfig holds Aerospike connection configuration.
type AerospikeConfig struct {
	Host             string `yaml:"host"             mapstructure:"host"`
	Port             int    `yaml:"port"             mapstructure:"port"`
	Namespace        string `yaml:"namespace"        mapstructure:"namespace"`
	SetName          string `yaml:"setName"          mapstructure:"setname"`
	SeenSet          string `yaml:"seenSet"          mapstructure:"seenset"`
	CallbackDedupSet      string `yaml:"callbackDedupSet"      mapstructure:"callbackdedupset"`
	CallbackURLRegistry   string `yaml:"callbackUrlRegistry"   mapstructure:"callbackurlregistry"`
	SubtreeCounterSet     string `yaml:"subtreeCounterSet"     mapstructure:"subtreecounterset"`
	SubtreeCounterTTLSec  int    `yaml:"subtreeCounterTTLSec"  mapstructure:"subtreecounterttlsec"`
	CallbackAccumulatorSet    string `yaml:"callbackAccumulatorSet"    mapstructure:"callbackaccumulatorset"`
	CallbackAccumulatorTTLSec int    `yaml:"callbackAccumulatorTTLSec" mapstructure:"callbackaccumulatorttlsec"`
	MaxRetries                int    `yaml:"maxRetries"                mapstructure:"maxretries"`
	RetryBaseMs           int    `yaml:"retryBaseMs"           mapstructure:"retrybasems"`
}

// KafkaConfig holds Kafka connection configuration.
type KafkaConfig struct {
	Brokers          []string `yaml:"brokers"          mapstructure:"brokers"`
	SubtreeTopic     string   `yaml:"subtreeTopic"     mapstructure:"subtreetopic"`
	BlockTopic       string   `yaml:"blockTopic"       mapstructure:"blocktopic"`
	CallbackTopic    string   `yaml:"callbackTopic"    mapstructure:"callbacktopic"`
	CallbackDLQTopic string   `yaml:"callbackDlqTopic" mapstructure:"callbackdlqtopic"`
	SubtreeDLQTopic  string   `yaml:"subtreeDlqTopic"  mapstructure:"subtreedlqtopic"`
	SubtreeWorkTopic string   `yaml:"subtreeWorkTopic" mapstructure:"subtreeworktopic"`
	ConsumerGroup    string   `yaml:"consumerGroup"    mapstructure:"consumergroup"`
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

	// Aerospike
	v.SetDefault("aerospike.host", "localhost")
	v.SetDefault("aerospike.port", 3000)
	v.SetDefault("aerospike.namespace", "merkle")
	v.SetDefault("aerospike.setname", "registrations")
	v.SetDefault("aerospike.seenset", "seen_counters")
	v.SetDefault("aerospike.callbackdedupset", "callback_dedup")
	v.SetDefault("aerospike.callbackurlregistry", "callback_urls")
	v.SetDefault("aerospike.subtreecounterset", "subtree_counters")
	v.SetDefault("aerospike.subtreecounterttlsec", 600)
	v.SetDefault("aerospike.callbackaccumulatorset", "callback_accum")
	v.SetDefault("aerospike.callbackaccumulatorttlsec", 600)
	v.SetDefault("aerospike.maxretries", 3)
	v.SetDefault("aerospike.retrybasems", 100)

	// Kafka
	v.SetDefault("kafka.brokers", []string{"localhost:9092"})
	v.SetDefault("kafka.subtreetopic", "subtree")
	v.SetDefault("kafka.blocktopic", "block")
	v.SetDefault("kafka.callbacktopic", "callback")
	v.SetDefault("kafka.callbackdlqtopic", "callback-dlq")
	v.SetDefault("kafka.subtreedlqtopic", "subtree-dlq")
	v.SetDefault("kafka.subtreeworktopic", "subtree-work")
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
	v.SetDefault("subtree.maxattempts", 10)

	// Block
	v.SetDefault("block.workerpoolsize", 16)
	v.SetDefault("block.postminettlsec", 1800)
	v.SetDefault("block.dedupcachesize", 10000)

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

		// Aerospike
		"aerospike.host":        "AEROSPIKE_HOST",
		"aerospike.port":        "AEROSPIKE_PORT",
		"aerospike.namespace":   "AEROSPIKE_NAMESPACE",
		"aerospike.setname":     "AEROSPIKE_SET",
		"aerospike.seenset":          "AEROSPIKE_SEEN_SET",
		"aerospike.callbackdedupset":    "AEROSPIKE_CALLBACK_DEDUP_SET",
		"aerospike.callbackurlregistry": "AEROSPIKE_CALLBACK_URL_REGISTRY",
		"aerospike.subtreecounterset":    "AEROSPIKE_SUBTREE_COUNTER_SET",
		"aerospike.subtreecounterttlsec": "AEROSPIKE_SUBTREE_COUNTER_TTL_SEC",
		"aerospike.callbackaccumulatorset":    "AEROSPIKE_CALLBACK_ACCUMULATOR_SET",
		"aerospike.callbackaccumulatorttlsec": "AEROSPIKE_CALLBACK_ACCUMULATOR_TTL_SEC",
		"aerospike.maxretries":           "AEROSPIKE_MAX_RETRIES",
		"aerospike.retrybasems":          "AEROSPIKE_RETRY_BASE_MS",

		// Kafka
		"kafka.brokers":        "KAFKA_BROKERS",
		"kafka.subtreetopic":   "KAFKA_SUBTREE_TOPIC",
		"kafka.blocktopic":     "KAFKA_BLOCK_TOPIC",
		"kafka.callbacktopic":    "KAFKA_CALLBACK_TOPIC",
		"kafka.callbackdlqtopic": "KAFKA_CALLBACK_DLQ_TOPIC",
		"kafka.subtreedlqtopic":  "KAFKA_SUBTREE_DLQ_TOPIC",
		"kafka.subtreeworktopic": "KAFKA_SUBTREE_WORK_TOPIC",
		"kafka.consumergroup":    "KAFKA_CONSUMER_GROUP",

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
		"block.workerpoolsize": "BLOCK_WORKER_POOL_SIZE",
		"block.postminettlsec":  "BLOCK_POST_MINE_TTL_SEC",
		"block.dedupcachesize":  "BLOCK_DEDUP_CACHE_SIZE",

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
