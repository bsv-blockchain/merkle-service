package config

import (
	"log/slog"
	"os"
	"testing"
)

// clearConfigEnv unsets all environment variables that affect config loading,
// so tests start from a clean slate.
func clearConfigEnv(t *testing.T) {
	t.Helper()
	envVars := []string{
		"CONFIG_FILE",
		"MODE", "LOG_LEVEL",
		"API_PORT",
		"AEROSPIKE_HOST", "AEROSPIKE_PORT", "AEROSPIKE_NAMESPACE",
		"AEROSPIKE_SET", "AEROSPIKE_SEEN_SET",
		"AEROSPIKE_MAX_RETRIES", "AEROSPIKE_RETRY_BASE_MS",
		"KAFKA_BROKERS", "KAFKA_SUBTREE_TOPIC", "KAFKA_BLOCK_TOPIC",
		"KAFKA_CALLBACK_TOPIC", "KAFKA_CALLBACK_DLQ_TOPIC", "KAFKA_SUBTREE_DLQ_TOPIC",
		"KAFKA_CONSUMER_GROUP",
		"P2P_NETWORK", "P2P_STORAGE_PATH",
		"P2P_DHT_MODE", "P2P_PORT", "P2P_ANNOUNCE_ADDRS", "P2P_BOOTSTRAP_PEERS",
		"P2P_MAX_CONNECTIONS", "P2P_MIN_CONNECTIONS", "P2P_ENABLE_NAT", "P2P_ENABLE_MDNS",
		"SUBTREE_STORAGE_MODE", "SUBTREE_DAH_OFFSET", "SUBTREE_CACHE_MAX_MB",
		"SUBTREE_MAX_ATTEMPTS",
		"BLOCK_WORKER_POOL_SIZE", "BLOCK_POST_MINE_TTL_SEC",
		"CALLBACK_MAX_RETRIES", "CALLBACK_BACKOFF_BASE_SEC",
		"CALLBACK_TIMEOUT_SEC", "CALLBACK_SEEN_THRESHOLD",
		"BLOB_STORE_URL",
	}
	for _, v := range envVars {
		_ = os.Unsetenv(v)
	}
}

func TestLoad_Defaults(t *testing.T) {
	clearConfigEnv(t)
	_ = os.Setenv("CONFIG_FILE", "/tmp/nonexistent-config-file.yaml")
	defer func() { _ = os.Unsetenv("CONFIG_FILE") }()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.Mode != "all-in-one" {
		t.Errorf("Mode: expected %q, got %q", "all-in-one", cfg.Mode)
	}
	if cfg.API.Port != 8080 {
		t.Errorf("API.Port: expected 8080, got %d", cfg.API.Port)
	}

	// Aerospike defaults
	if cfg.Aerospike.Host != "localhost" {
		t.Errorf("Aerospike.Host: expected %q, got %q", "localhost", cfg.Aerospike.Host)
	}
	if cfg.Aerospike.Port != 3000 {
		t.Errorf("Aerospike.Port: expected 3000, got %d", cfg.Aerospike.Port)
	}
	if cfg.Aerospike.Namespace != "merkle" {
		t.Errorf("Aerospike.Namespace: expected %q, got %q", "merkle", cfg.Aerospike.Namespace)
	}
	if cfg.Aerospike.SetName != "merkle_registrations" {
		t.Errorf("Aerospike.SetName: expected %q, got %q", "merkle_registrations", cfg.Aerospike.SetName)
	}
	if cfg.Aerospike.SeenSet != "merkle_seen_counters" {
		t.Errorf("Aerospike.SeenSet: expected %q, got %q", "merkle_seen_counters", cfg.Aerospike.SeenSet)
	}
	if cfg.Aerospike.MaxRetries != 3 {
		t.Errorf("Aerospike.MaxRetries: expected 3, got %d", cfg.Aerospike.MaxRetries)
	}
	if cfg.Aerospike.RetryBaseMs != 100 {
		t.Errorf("Aerospike.RetryBaseMs: expected 100, got %d", cfg.Aerospike.RetryBaseMs)
	}

	// Kafka defaults
	if len(cfg.Kafka.Brokers) != 1 || cfg.Kafka.Brokers[0] != "localhost:9092" {
		t.Errorf("Kafka.Brokers: expected [localhost:9092], got %v", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.SubtreeTopic != "subtree" {
		t.Errorf("Kafka.SubtreeTopic: expected %q, got %q", "subtree", cfg.Kafka.SubtreeTopic)
	}
	if cfg.Kafka.CallbackDLQTopic != "callback-dlq" {
		t.Errorf("Kafka.CallbackDLQTopic: expected %q, got %q", "callback-dlq", cfg.Kafka.CallbackDLQTopic)
	}
	if cfg.Kafka.SubtreeDLQTopic != "subtree-dlq" {
		t.Errorf("Kafka.SubtreeDLQTopic: expected %q, got %q", "subtree-dlq", cfg.Kafka.SubtreeDLQTopic)
	}
	if cfg.Kafka.ConsumerGroup != "merkle-service" {
		t.Errorf("Kafka.ConsumerGroup: expected %q, got %q", "merkle-service", cfg.Kafka.ConsumerGroup)
	}

	// P2P defaults
	if cfg.P2P.Network != "main" {
		t.Errorf("P2P.Network: expected %q, got %q", "main", cfg.P2P.Network)
	}
	if cfg.P2P.StoragePath != "~/.merkle-service/p2p" {
		t.Errorf("P2P.StoragePath: expected %q, got %q", "~/.merkle-service/p2p", cfg.P2P.StoragePath)
	}

	// Subtree defaults
	if cfg.Subtree.StorageMode != "realtime" {
		t.Errorf("Subtree.StorageMode: expected %q, got %q", "realtime", cfg.Subtree.StorageMode)
	}
	if cfg.Subtree.DAHOffset != 1 {
		t.Errorf("Subtree.DAHOffset: expected 1, got %d", cfg.Subtree.DAHOffset)
	}
	if cfg.Subtree.CacheMaxMB != 64 {
		t.Errorf("Subtree.CacheMaxMB: expected 64, got %d", cfg.Subtree.CacheMaxMB)
	}
	if cfg.Subtree.MaxAttempts != 3 {
		t.Errorf("Subtree.MaxAttempts: expected 3, got %d", cfg.Subtree.MaxAttempts)
	}

	// Block defaults
	if cfg.Block.WorkerPoolSize != 16 {
		t.Errorf("Block.WorkerPoolSize: expected 16, got %d", cfg.Block.WorkerPoolSize)
	}
	if cfg.Block.PostMineTTLSec != 1800 {
		t.Errorf("Block.PostMineTTLSec: expected 1800, got %d", cfg.Block.PostMineTTLSec)
	}

	// Callback defaults
	if cfg.Callback.MaxRetries != 5 {
		t.Errorf("Callback.MaxRetries: expected 5, got %d", cfg.Callback.MaxRetries)
	}
	if cfg.Callback.SeenThreshold != 3 {
		t.Errorf("Callback.SeenThreshold: expected 3, got %d", cfg.Callback.SeenThreshold)
	}

	// BlobStore default
	if cfg.BlobStore.URL != "file:///tmp/merkle-subtrees" {
		t.Errorf("BlobStore.URL: expected %q, got %q", "file:///tmp/merkle-subtrees", cfg.BlobStore.URL)
	}
}

func TestLoad_EnvOverrides(t *testing.T) {
	clearConfigEnv(t)
	_ = os.Setenv("CONFIG_FILE", "/tmp/nonexistent-config-file.yaml")
	defer func() { _ = os.Unsetenv("CONFIG_FILE") }()

	_ = os.Setenv("MODE", "microservice")
	_ = os.Setenv("API_PORT", "9090")
	_ = os.Setenv("AEROSPIKE_HOST", "aerospike.example.com")
	_ = os.Setenv("AEROSPIKE_PORT", "3001")
	_ = os.Setenv("AEROSPIKE_NAMESPACE", "testns")
	_ = os.Setenv("AEROSPIKE_SET", "testregs")
	_ = os.Setenv("AEROSPIKE_SEEN_SET", "testseen")
	_ = os.Setenv("AEROSPIKE_MAX_RETRIES", "7")
	_ = os.Setenv("AEROSPIKE_RETRY_BASE_MS", "200")
	_ = os.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")
	_ = os.Setenv("KAFKA_SUBTREE_TOPIC", "my-subtree")
	_ = os.Setenv("KAFKA_BLOCK_TOPIC", "my-block")
	_ = os.Setenv("KAFKA_CALLBACK_TOPIC", "my-callback")
	_ = os.Setenv("KAFKA_CALLBACK_DLQ_TOPIC", "my-callback-dlq")
	_ = os.Setenv("KAFKA_SUBTREE_DLQ_TOPIC", "my-subtree-dlq")
	_ = os.Setenv("KAFKA_CONSUMER_GROUP", "my-group")
	_ = os.Setenv("P2P_NETWORK", "testnet")
	_ = os.Setenv("P2P_STORAGE_PATH", "/tmp/p2p-test")
	_ = os.Setenv("SUBTREE_STORAGE_MODE", "deferred")
	_ = os.Setenv("SUBTREE_DAH_OFFSET", "3")
	_ = os.Setenv("SUBTREE_CACHE_MAX_MB", "128")
	_ = os.Setenv("SUBTREE_MAX_ATTEMPTS", "7")
	_ = os.Setenv("BLOCK_WORKER_POOL_SIZE", "32")
	_ = os.Setenv("BLOCK_POST_MINE_TTL_SEC", "3600")
	_ = os.Setenv("CALLBACK_MAX_RETRIES", "10")
	_ = os.Setenv("CALLBACK_BACKOFF_BASE_SEC", "60")
	_ = os.Setenv("CALLBACK_TIMEOUT_SEC", "20")
	_ = os.Setenv("CALLBACK_SEEN_THRESHOLD", "5")
	_ = os.Setenv("BLOB_STORE_URL", "s3://my-bucket")

	defer clearConfigEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.Mode != "microservice" {
		t.Errorf("Mode: expected %q, got %q", "microservice", cfg.Mode)
	}
	if cfg.API.Port != 9090 {
		t.Errorf("API.Port: expected 9090, got %d", cfg.API.Port)
	}
	if cfg.Aerospike.Host != "aerospike.example.com" {
		t.Errorf("Aerospike.Host: expected %q, got %q", "aerospike.example.com", cfg.Aerospike.Host)
	}
	if cfg.Aerospike.Port != 3001 {
		t.Errorf("Aerospike.Port: expected 3001, got %d", cfg.Aerospike.Port)
	}
	if cfg.Aerospike.SetName != "testregs" {
		t.Errorf("Aerospike.SetName: expected %q, got %q", "testregs", cfg.Aerospike.SetName)
	}
	if cfg.Aerospike.MaxRetries != 7 {
		t.Errorf("Aerospike.MaxRetries: expected 7, got %d", cfg.Aerospike.MaxRetries)
	}
	if len(cfg.Kafka.Brokers) != 2 || cfg.Kafka.Brokers[0] != "broker1:9092" {
		t.Errorf("Kafka.Brokers: expected [broker1:9092 broker2:9092], got %v", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.CallbackDLQTopic != "my-callback-dlq" {
		t.Errorf("Kafka.CallbackDLQTopic: expected %q, got %q", "my-callback-dlq", cfg.Kafka.CallbackDLQTopic)
	}
	if cfg.Kafka.SubtreeDLQTopic != "my-subtree-dlq" {
		t.Errorf("Kafka.SubtreeDLQTopic: expected %q, got %q", "my-subtree-dlq", cfg.Kafka.SubtreeDLQTopic)
	}
	if cfg.P2P.Network != "testnet" {
		t.Errorf("P2P.Network: expected %q, got %q", "testnet", cfg.P2P.Network)
	}
	if cfg.P2P.StoragePath != "/tmp/p2p-test" {
		t.Errorf("P2P.StoragePath: expected %q, got %q", "/tmp/p2p-test", cfg.P2P.StoragePath)
	}
	if cfg.Subtree.StorageMode != "deferred" {
		t.Errorf("Subtree.StorageMode: expected %q, got %q", "deferred", cfg.Subtree.StorageMode)
	}
	if cfg.Subtree.CacheMaxMB != 128 {
		t.Errorf("Subtree.CacheMaxMB: expected 128, got %d", cfg.Subtree.CacheMaxMB)
	}
	if cfg.Subtree.MaxAttempts != 7 {
		t.Errorf("Subtree.MaxAttempts: expected 7, got %d", cfg.Subtree.MaxAttempts)
	}
	if cfg.Block.WorkerPoolSize != 32 {
		t.Errorf("Block.WorkerPoolSize: expected 32, got %d", cfg.Block.WorkerPoolSize)
	}
	if cfg.Callback.MaxRetries != 10 {
		t.Errorf("Callback.MaxRetries: expected 10, got %d", cfg.Callback.MaxRetries)
	}
	if cfg.BlobStore.URL != "s3://my-bucket" {
		t.Errorf("BlobStore.URL: expected %q, got %q", "s3://my-bucket", cfg.BlobStore.URL)
	}
}

func TestLoad_YAMLFile(t *testing.T) {
	clearConfigEnv(t)

	yamlContent := []byte(`
mode: yaml-mode
api:
  port: 7777
callback:
  maxRetries: 99
`)
	tmpFile := t.TempDir() + "/test-config.yaml"
	if err := os.WriteFile(tmpFile, yamlContent, 0o644); err != nil { //nolint:gosec // test temp file, 0644 is fine
		t.Fatalf("failed to write temp yaml: %v", err)
	}
	_ = os.Setenv("CONFIG_FILE", tmpFile)
	defer func() { _ = os.Unsetenv("CONFIG_FILE") }()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.Mode != "yaml-mode" {
		t.Errorf("Mode: expected %q, got %q", "yaml-mode", cfg.Mode)
	}
	if cfg.API.Port != 7777 {
		t.Errorf("API.Port: expected 7777, got %d", cfg.API.Port)
	}
	if cfg.Callback.MaxRetries != 99 {
		t.Errorf("Callback.MaxRetries: expected 99, got %d", cfg.Callback.MaxRetries)
	}
	// Fields not set in YAML should retain defaults.
	if cfg.Aerospike.Host != "localhost" {
		t.Errorf("Aerospike.Host: expected default %q, got %q", "localhost", cfg.Aerospike.Host)
	}
}

func TestLoad_EnvOverridesYAML(t *testing.T) {
	clearConfigEnv(t)

	yamlContent := []byte(`mode: from-yaml`)
	tmpFile := t.TempDir() + "/test-config.yaml"
	if err := os.WriteFile(tmpFile, yamlContent, 0o644); err != nil { //nolint:gosec // test temp file, 0644 is fine
		t.Fatalf("failed to write temp yaml: %v", err)
	}
	_ = os.Setenv("CONFIG_FILE", tmpFile)
	_ = os.Setenv("MODE", "from-env")
	defer func() {
		_ = os.Unsetenv("CONFIG_FILE")
		_ = os.Unsetenv("MODE")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}
	if cfg.Mode != "from-env" {
		t.Errorf("Mode: env should override YAML; expected %q, got %q", "from-env", cfg.Mode)
	}
}

func TestLoad_InvalidYAMLReturnsError(t *testing.T) {
	clearConfigEnv(t)

	yamlContent := []byte(`mode: [invalid yaml`)
	tmpFile := t.TempDir() + "/bad-config.yaml"
	if err := os.WriteFile(tmpFile, yamlContent, 0o644); err != nil { //nolint:gosec // test temp file, 0644 is fine
		t.Fatalf("failed to write temp yaml: %v", err)
	}
	_ = os.Setenv("CONFIG_FILE", tmpFile)
	defer func() { _ = os.Unsetenv("CONFIG_FILE") }()

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for invalid YAML, got nil")
	}
}

func TestLoad_P2PMsgBusDefaults(t *testing.T) {
	clearConfigEnv(t)
	_ = os.Setenv("CONFIG_FILE", "/tmp/nonexistent-config-file.yaml")
	defer func() { _ = os.Unsetenv("CONFIG_FILE") }()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.P2P.MsgBus.DHTMode != "off" {
		t.Errorf("P2P.MsgBus.DHTMode: expected %q, got %q", "off", cfg.P2P.MsgBus.DHTMode)
	}
	if cfg.P2P.MsgBus.Port != 9905 {
		t.Errorf("P2P.MsgBus.Port: expected 9905, got %d", cfg.P2P.MsgBus.Port)
	}
	if cfg.P2P.MsgBus.EnableNAT {
		t.Error("P2P.MsgBus.EnableNAT: expected false")
	}
	if cfg.P2P.MsgBus.EnableMDNS {
		t.Error("P2P.MsgBus.EnableMDNS: expected false")
	}
}

func TestLoad_P2PDHTModeEnvOverride(t *testing.T) {
	clearConfigEnv(t)
	_ = os.Setenv("CONFIG_FILE", "/tmp/nonexistent-config-file.yaml")
	_ = os.Setenv("P2P_DHT_MODE", "server")
	defer func() {
		_ = os.Unsetenv("CONFIG_FILE")
		_ = os.Unsetenv("P2P_DHT_MODE")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.P2P.MsgBus.DHTMode != "server" {
		t.Errorf("P2P.MsgBus.DHTMode: expected %q via env, got %q", "server", cfg.P2P.MsgBus.DHTMode)
	}
}

func TestLoad_LogLevelDefault(t *testing.T) {
	clearConfigEnv(t)
	_ = os.Setenv("CONFIG_FILE", "/tmp/nonexistent-config-file.yaml")
	defer func() { _ = os.Unsetenv("CONFIG_FILE") }()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel: expected %q, got %q", "info", cfg.LogLevel)
	}
}

func TestLoad_LogLevelEnvOverride(t *testing.T) {
	clearConfigEnv(t)
	_ = os.Setenv("CONFIG_FILE", "/tmp/nonexistent-config-file.yaml")
	_ = os.Setenv("LOG_LEVEL", "debug")
	defer func() {
		_ = os.Unsetenv("CONFIG_FILE")
		_ = os.Unsetenv("LOG_LEVEL")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel: expected %q, got %q", "debug", cfg.LogLevel)
	}
}

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"INFO", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"ERROR", slog.LevelError},
		{"", slog.LevelInfo},
		{"invalid", slog.LevelInfo},
		{"  debug  ", slog.LevelDebug},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ParseLogLevel(tt.input)
			if got != tt.expected {
				t.Errorf("ParseLogLevel(%q): expected %v, got %v", tt.input, tt.expected, got)
			}
		})
	}
}
