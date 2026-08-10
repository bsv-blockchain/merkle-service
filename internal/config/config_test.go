package config

import (
	"log/slog"
	"os"
	"testing"
)

// Env var name constants for the telemetry env-binding/validation table
// tests below. Pulled out as constants (rather than repeated literals)
// because TestLoad_TelemetryValidation's table drives both env vars across
// many cases, which would otherwise trip the goconst linter.
const (
	envTelemetryEnabled  = "TELEMETRY_ENABLED"
	envTelemetryProtocol = "TELEMETRY_PROTOCOL"
	// envTrue is the string value set for boolean env vars in the telemetry
	// validation table; pulled out so the repeated literal doesn't trip the
	// goconst linter.
	envTrue = "true"
)

// clearConfigEnv unsets all environment variables that affect config loading,
// so tests start from a clean slate.
func clearConfigEnv(t *testing.T) {
	t.Helper()
	envVars := []string{
		"CONFIG_FILE",
		"MODE", "LOG_LEVEL",
		"API_PORT", "API_AUTH_TOKEN", "API_REQUIRE_WATCH_AUTH",
		"API_REPROCESS_RATE_LIMIT_RPS", "API_REPROCESS_BURST", "API_MAX_INFLIGHT_REPROCESS",
		"AEROSPIKE_HOST", "AEROSPIKE_PORT", "AEROSPIKE_NAMESPACE",
		"AEROSPIKE_SET", "AEROSPIKE_SEEN_SET",
		"AEROSPIKE_MAX_RETRIES", "AEROSPIKE_RETRY_BASE_MS",
		"KAFKA_BROKERS", "KAFKA_SUBTREE_TOPIC", "KAFKA_BLOCK_TOPIC",
		"KAFKA_CALLBACK_TOPIC", "KAFKA_CALLBACK_DLQ_TOPIC", "KAFKA_SUBTREE_DLQ_TOPIC",
		"KAFKA_CALLBACK_SEEN_TOPIC", "KAFKA_CALLBACK_SEEN_PARTITIONS",
		"KAFKA_CONSUMER_GROUP",
		"P2P_NETWORK", "P2P_STORAGE_PATH",
		"P2P_DHT_MODE", "P2P_PORT", "P2P_ANNOUNCE_ADDRS", "P2P_BOOTSTRAP_PEERS",
		"P2P_MAX_CONNECTIONS", "P2P_MIN_CONNECTIONS", "P2P_ENABLE_NAT", "P2P_ENABLE_MDNS",
		"SUBTREE_STORAGE_MODE", "SUBTREE_DAH_OFFSET", "SUBTREE_CACHE_MAX_MB",
		"SUBTREE_MAX_ATTEMPTS",
		"BLOCK_WORKER_POOL_SIZE", "BLOCK_POST_MINE_TTL_SEC",
		"BLOCK_RETRY_BACKOFF_BASE_MS", "BLOCK_NOT_FOUND_MAX_ATTEMPTS",
		"BLOCK_EMIT_EXPECTED_STUMP_SET",
		"CALLBACK_MAX_RETRIES", "CALLBACK_BACKOFF_BASE_SEC",
		"CALLBACK_TIMEOUT_SEC", "CALLBACK_SEEN_THRESHOLD",
		"BLOB_STORE_URL", "BLOB_STORE_SWEEP_INTERVAL_SEC", "BLOB_STORE_SWEEP_MAX_AGE_SEC",
		envTelemetryEnabled, "TELEMETRY_ENDPOINT", envTelemetryProtocol,
		"TELEMETRY_INSECURE", "TELEMETRY_SERVICE_NAME", "TELEMETRY_NAMESPACE",
		"TELEMETRY_TRACES", "TELEMETRY_METRICS", "TELEMETRY_SAMPLE_RATIO",
		"TELEMETRY_EXPORT_TIMEOUT_MS",
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
	if cfg.API.AuthToken != "" {
		t.Errorf("API.AuthToken: expected empty default, got %q", cfg.API.AuthToken)
	}
	if cfg.API.RequireWatchAuth {
		t.Errorf("API.RequireWatchAuth: expected false default, got true")
	}
	if cfg.API.ReprocessRateLimitRps != 20 {
		t.Errorf("API.ReprocessRateLimitRps: expected 20, got %v", cfg.API.ReprocessRateLimitRps)
	}
	if cfg.API.ReprocessBurst != 100 {
		t.Errorf("API.ReprocessBurst: expected 100, got %d", cfg.API.ReprocessBurst)
	}
	if cfg.API.MaxInflightReprocess != 16 {
		t.Errorf("API.MaxInflightReprocess: expected 16, got %d", cfg.API.MaxInflightReprocess)
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
	if cfg.Subtree.SeenTxidLogMax != 1000 {
		t.Errorf("Subtree.SeenTxidLogMax: expected 1000, got %d", cfg.Subtree.SeenTxidLogMax)
	}

	// Block defaults
	if cfg.Block.WorkerPoolSize != 16 {
		t.Errorf("Block.WorkerPoolSize: expected 16, got %d", cfg.Block.WorkerPoolSize)
	}
	if cfg.Block.PostMineTTLSec != 1800 {
		t.Errorf("Block.PostMineTTLSec: expected 1800, got %d", cfg.Block.PostMineTTLSec)
	}
	if cfg.Block.RetryBackoffBaseMs != 1000 {
		t.Errorf("Block.RetryBackoffBaseMs: expected 1000, got %d", cfg.Block.RetryBackoffBaseMs)
	}
	if cfg.Block.NotFoundMaxAttempts != 3 {
		t.Errorf("Block.NotFoundMaxAttempts: expected 3, got %d", cfg.Block.NotFoundMaxAttempts)
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
	// Age sweeper: on by default and aggressive — subtree blobs are a
	// re-fetchable cache (DataHub re-serves them), and the 2026-07-15
	// dev-ovh-1 incident showed orphans filling a 1TiB volume in ~3h.
	// 1800s ≈ 3 blocks at ~10min cadence; 300s interval bounds orphan
	// buildup to a small fraction of steady state.
	if cfg.BlobStore.SweepIntervalSec != 300 {
		t.Errorf("BlobStore.SweepIntervalSec: expected 300, got %d", cfg.BlobStore.SweepIntervalSec)
	}
	if cfg.BlobStore.SweepMaxAgeSec != 1800 {
		t.Errorf("BlobStore.SweepMaxAgeSec: expected 1800, got %d", cfg.BlobStore.SweepMaxAgeSec)
	}
}

func TestLoad_EnvOverrides(t *testing.T) {
	clearConfigEnv(t)
	_ = os.Setenv("CONFIG_FILE", "/tmp/nonexistent-config-file.yaml")
	defer func() { _ = os.Unsetenv("CONFIG_FILE") }()

	_ = os.Setenv("MODE", "microservice")
	_ = os.Setenv("API_PORT", "9090")
	_ = os.Setenv("API_AUTH_TOKEN", "s3cret-token")
	_ = os.Setenv("API_REQUIRE_WATCH_AUTH", "true")
	_ = os.Setenv("API_REPROCESS_RATE_LIMIT_RPS", "5")
	_ = os.Setenv("API_REPROCESS_BURST", "42")
	_ = os.Setenv("API_MAX_INFLIGHT_REPROCESS", "8")
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
	_ = os.Setenv("BLOCK_RETRY_BACKOFF_BASE_MS", "250")
	_ = os.Setenv("BLOCK_NOT_FOUND_MAX_ATTEMPTS", "5")
	_ = os.Setenv("CALLBACK_MAX_RETRIES", "10")
	_ = os.Setenv("CALLBACK_BACKOFF_BASE_SEC", "60")
	_ = os.Setenv("CALLBACK_TIMEOUT_SEC", "20")
	_ = os.Setenv("CALLBACK_SEEN_THRESHOLD", "5")
	_ = os.Setenv("BLOB_STORE_URL", "s3://my-bucket")
	_ = os.Setenv("BLOB_STORE_SWEEP_INTERVAL_SEC", "120")
	_ = os.Setenv("BLOB_STORE_SWEEP_MAX_AGE_SEC", "900")

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
	if cfg.API.AuthToken != "s3cret-token" {
		t.Errorf("API.AuthToken: expected %q, got %q", "s3cret-token", cfg.API.AuthToken)
	}
	if !cfg.API.RequireWatchAuth {
		t.Errorf("API.RequireWatchAuth: expected true, got false")
	}
	if cfg.API.ReprocessRateLimitRps != 5 {
		t.Errorf("API.ReprocessRateLimitRps: expected 5, got %v", cfg.API.ReprocessRateLimitRps)
	}
	if cfg.API.ReprocessBurst != 42 {
		t.Errorf("API.ReprocessBurst: expected 42, got %d", cfg.API.ReprocessBurst)
	}
	if cfg.API.MaxInflightReprocess != 8 {
		t.Errorf("API.MaxInflightReprocess: expected 8, got %d", cfg.API.MaxInflightReprocess)
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
	if cfg.Block.RetryBackoffBaseMs != 250 {
		t.Errorf("Block.RetryBackoffBaseMs: expected 250, got %d", cfg.Block.RetryBackoffBaseMs)
	}
	if cfg.Block.NotFoundMaxAttempts != 5 {
		t.Errorf("Block.NotFoundMaxAttempts: expected 5, got %d", cfg.Block.NotFoundMaxAttempts)
	}
	if cfg.Callback.MaxRetries != 10 {
		t.Errorf("Callback.MaxRetries: expected 10, got %d", cfg.Callback.MaxRetries)
	}
	if cfg.BlobStore.URL != "s3://my-bucket" {
		t.Errorf("BlobStore.URL: expected %q, got %q", "s3://my-bucket", cfg.BlobStore.URL)
	}
	if cfg.BlobStore.SweepIntervalSec != 120 {
		t.Errorf("BlobStore.SweepIntervalSec: expected 120, got %d", cfg.BlobStore.SweepIntervalSec)
	}
	if cfg.BlobStore.SweepMaxAgeSec != 900 {
		t.Errorf("BlobStore.SweepMaxAgeSec: expected 900, got %d", cfg.BlobStore.SweepMaxAgeSec)
	}
}

// TestLoad_BlobStoreSweepValidation pins the sweep-age floor: a nonzero
// blobStore.sweepMaxAgeSec below 600s (~1 block interval) could delete the
// in-flight block's subtree blobs mid-processing, so Load must reject it.
// 0 stays legal — it disables age-based sweeping entirely.
func TestLoad_BlobStoreSweepValidation(t *testing.T) {
	tests := []struct {
		name      string
		env       map[string]string
		wantError bool
	}{
		{
			name:      "defaults are valid",
			env:       nil,
			wantError: false,
		},
		{
			name:      "max age at the 600s floor is accepted",
			env:       map[string]string{"BLOB_STORE_SWEEP_MAX_AGE_SEC": "600"},
			wantError: false,
		},
		{
			name:      "max age below the floor is rejected",
			env:       map[string]string{"BLOB_STORE_SWEEP_MAX_AGE_SEC": "599"},
			wantError: true,
		},
		{
			name:      "max age 0 disables the sweeper and is accepted",
			env:       map[string]string{"BLOB_STORE_SWEEP_MAX_AGE_SEC": "0"},
			wantError: false,
		},
		{
			name:      "negative max age is rejected",
			env:       map[string]string{"BLOB_STORE_SWEEP_MAX_AGE_SEC": "-1"},
			wantError: true,
		},
		{
			name:      "interval 0 disables the sweeper and is accepted",
			env:       map[string]string{"BLOB_STORE_SWEEP_INTERVAL_SEC": "0"},
			wantError: false,
		},
		{
			name:      "negative interval is rejected",
			env:       map[string]string{"BLOB_STORE_SWEEP_INTERVAL_SEC": "-1"},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv("CONFIG_FILE", "/tmp/nonexistent-config-file.yaml")
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			_, err := Load()
			if tt.wantError && err == nil {
				t.Fatal("expected Load() to return an error, got nil")
			}
			if !tt.wantError && err != nil {
				t.Fatalf("expected Load() to succeed, got error: %v", err)
			}
		})
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

// TestLoad_DataHubPeerHealthDefaults pins the peer-health breaker defaults,
// including the stale-404 grace added after the 2026-07-15 dev-ovh-1
// incident (consumer lag aged announcements past teranode's ~2h asset-cache
// retention; every resulting 404 re-tripped the breaker after each
// cooldown even though the peer was fine).
func TestLoad_DataHubPeerHealthDefaults(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("CONFIG_FILE", "/tmp/nonexistent-config-file.yaml")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.DataHub.PeerHealth.FailureThreshold != 3 {
		t.Errorf("DataHub.PeerHealth.FailureThreshold: expected 3, got %d", cfg.DataHub.PeerHealth.FailureThreshold)
	}
	if cfg.DataHub.PeerHealth.CooldownSec != 300 {
		t.Errorf("DataHub.PeerHealth.CooldownSec: expected 300, got %d", cfg.DataHub.PeerHealth.CooldownSec)
	}
	if cfg.DataHub.PeerHealth.Stale404GraceSec != 3600 {
		t.Errorf("DataHub.PeerHealth.Stale404GraceSec: expected 3600, got %d", cfg.DataHub.PeerHealth.Stale404GraceSec)
	}
}

// TestLoad_DataHubPeerHealthEnvBinding proves the bindEnvVars entries for
// the datahub.peerhealth keys — a missing binding would silently leave the
// env var ignored.
func TestLoad_DataHubPeerHealthEnvBinding(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("CONFIG_FILE", "/tmp/nonexistent-config-file.yaml")
	t.Setenv("DATAHUB_PEER_HEALTH_FAILURE_THRESHOLD", "5")
	t.Setenv("DATAHUB_PEER_HEALTH_COOLDOWN_SEC", "120")
	t.Setenv("DATAHUB_PEER_HEALTH_STALE404_GRACE_SEC", "900")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.DataHub.PeerHealth.FailureThreshold != 5 {
		t.Errorf("DataHub.PeerHealth.FailureThreshold: expected 5, got %d", cfg.DataHub.PeerHealth.FailureThreshold)
	}
	if cfg.DataHub.PeerHealth.CooldownSec != 120 {
		t.Errorf("DataHub.PeerHealth.CooldownSec: expected 120, got %d", cfg.DataHub.PeerHealth.CooldownSec)
	}
	if cfg.DataHub.PeerHealth.Stale404GraceSec != 900 {
		t.Errorf("DataHub.PeerHealth.Stale404GraceSec: expected 900, got %d", cfg.DataHub.PeerHealth.Stale404GraceSec)
	}
}

func TestLoad_TelemetryDefaults(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("CONFIG_FILE", "/tmp/nonexistent-config-file.yaml")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.Telemetry.Enabled {
		t.Error("Telemetry.Enabled: expected false")
	}
	if cfg.Telemetry.Endpoint != "" {
		t.Errorf("Telemetry.Endpoint: expected empty, got %q", cfg.Telemetry.Endpoint)
	}
	if cfg.Telemetry.Protocol != "grpc" {
		t.Errorf("Telemetry.Protocol: expected %q, got %q", "grpc", cfg.Telemetry.Protocol)
	}
	if cfg.Telemetry.Insecure {
		t.Error("Telemetry.Insecure: expected false")
	}
	if cfg.Telemetry.ServiceName != "merkle-service" {
		t.Errorf("Telemetry.ServiceName: expected %q, got %q", "merkle-service", cfg.Telemetry.ServiceName)
	}
	if cfg.Telemetry.Namespace != "" {
		t.Errorf("Telemetry.Namespace: expected empty, got %q", cfg.Telemetry.Namespace)
	}
	if !cfg.Telemetry.Traces {
		t.Error("Telemetry.Traces: expected true")
	}
	if !cfg.Telemetry.Metrics {
		t.Error("Telemetry.Metrics: expected true")
	}
	if cfg.Telemetry.SampleRatio != 1.0 {
		t.Errorf("Telemetry.SampleRatio: expected 1.0, got %v", cfg.Telemetry.SampleRatio)
	}
	if cfg.Telemetry.ExportTimeoutMs != 10000 {
		t.Errorf("Telemetry.ExportTimeoutMs: expected 10000, got %d", cfg.Telemetry.ExportTimeoutMs)
	}
}

// TestLoad_TelemetryEnvBinding proves the bindEnvVars map wires every
// TELEMETRY_* environment variable to its viper key — a typo'd or missing
// binding entry would silently leave the env var ignored.
func TestLoad_TelemetryEnvBinding(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("CONFIG_FILE", "/tmp/nonexistent-config-file.yaml")
	t.Setenv(envTelemetryEnabled, "true")
	t.Setenv("TELEMETRY_ENDPOINT", "collector:4317")
	t.Setenv(envTelemetryProtocol, "http")
	t.Setenv("TELEMETRY_INSECURE", "true")
	t.Setenv("TELEMETRY_SERVICE_NAME", "custom-service")
	t.Setenv("TELEMETRY_NAMESPACE", "custom-namespace")
	t.Setenv("TELEMETRY_TRACES", "false")
	t.Setenv("TELEMETRY_METRICS", "false")
	t.Setenv("TELEMETRY_SAMPLE_RATIO", "0.5")
	t.Setenv("TELEMETRY_EXPORT_TIMEOUT_MS", "2500")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if !cfg.Telemetry.Enabled {
		t.Error("Telemetry.Enabled: expected true via TELEMETRY_ENABLED")
	}
	if cfg.Telemetry.Endpoint != "collector:4317" {
		t.Errorf("Telemetry.Endpoint: expected %q, got %q", "collector:4317", cfg.Telemetry.Endpoint)
	}
	if cfg.Telemetry.Protocol != "http" {
		t.Errorf("Telemetry.Protocol: expected %q, got %q", "http", cfg.Telemetry.Protocol)
	}
	if !cfg.Telemetry.Insecure {
		t.Error("Telemetry.Insecure: expected true via TELEMETRY_INSECURE")
	}
	if cfg.Telemetry.ServiceName != "custom-service" {
		t.Errorf("Telemetry.ServiceName: expected %q, got %q", "custom-service", cfg.Telemetry.ServiceName)
	}
	if cfg.Telemetry.Namespace != "custom-namespace" {
		t.Errorf("Telemetry.Namespace: expected %q, got %q", "custom-namespace", cfg.Telemetry.Namespace)
	}
	if cfg.Telemetry.Traces {
		t.Error("Telemetry.Traces: expected false via TELEMETRY_TRACES")
	}
	if cfg.Telemetry.Metrics {
		t.Error("Telemetry.Metrics: expected false via TELEMETRY_METRICS")
	}
	if cfg.Telemetry.SampleRatio != 0.5 {
		t.Errorf("Telemetry.SampleRatio: expected 0.5, got %v", cfg.Telemetry.SampleRatio)
	}
	if cfg.Telemetry.ExportTimeoutMs != 2500 {
		t.Errorf("Telemetry.ExportTimeoutMs: expected 2500, got %d", cfg.Telemetry.ExportTimeoutMs)
	}
}

func TestLoad_TelemetryValidation(t *testing.T) {
	tests := []struct {
		name      string
		env       map[string]string
		wantError bool
	}{
		{
			name:      "disabled skips validation entirely",
			env:       map[string]string{envTelemetryEnabled: "false", envTelemetryProtocol: "bogus"},
			wantError: false,
		},
		{
			name:      "enabled with valid grpc config",
			env:       map[string]string{envTelemetryEnabled: envTrue, envTelemetryProtocol: "grpc"},
			wantError: false,
		},
		{
			name:      "enabled with valid http config",
			env:       map[string]string{envTelemetryEnabled: envTrue, envTelemetryProtocol: "http"},
			wantError: false,
		},
		{
			name:      "enabled with empty endpoint is allowed (env fallback resolved in Init)",
			env:       map[string]string{envTelemetryEnabled: envTrue, envTelemetryProtocol: "grpc", "TELEMETRY_ENDPOINT": ""},
			wantError: false,
		},
		{
			name:      "invalid protocol",
			env:       map[string]string{envTelemetryEnabled: envTrue, envTelemetryProtocol: "webtransport"},
			wantError: true,
		},
		{
			name:      "sample ratio below 0",
			env:       map[string]string{envTelemetryEnabled: envTrue, "TELEMETRY_SAMPLE_RATIO": "-0.1"},
			wantError: true,
		},
		{
			name:      "sample ratio above 1",
			env:       map[string]string{envTelemetryEnabled: envTrue, "TELEMETRY_SAMPLE_RATIO": "1.1"},
			wantError: true,
		},
		{
			name:      "grpc protocol rejects http:// scheme endpoint",
			env:       map[string]string{envTelemetryEnabled: envTrue, envTelemetryProtocol: "grpc", "TELEMETRY_ENDPOINT": "http://collector:4317"},
			wantError: true,
		},
		{
			name:      "grpc protocol rejects https:// scheme endpoint",
			env:       map[string]string{envTelemetryEnabled: envTrue, envTelemetryProtocol: "grpc", "TELEMETRY_ENDPOINT": "https://collector:4317"},
			wantError: true,
		},
		{
			name:      "http protocol allows a bare host:port endpoint",
			env:       map[string]string{envTelemetryEnabled: envTrue, envTelemetryProtocol: "http", "TELEMETRY_ENDPOINT": "collector:4318"},
			wantError: false,
		},
		{
			name:      "http protocol rejects http:// scheme endpoint",
			env:       map[string]string{envTelemetryEnabled: envTrue, envTelemetryProtocol: "http", "TELEMETRY_ENDPOINT": "http://collector:4318"},
			wantError: true,
		},
		{
			name:      "http protocol rejects https:// scheme endpoint",
			env:       map[string]string{envTelemetryEnabled: envTrue, envTelemetryProtocol: "http", "TELEMETRY_ENDPOINT": "https://collector:4318"},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv("CONFIG_FILE", "/tmp/nonexistent-config-file.yaml")
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			_, err := Load()
			if tt.wantError && err == nil {
				t.Fatal("expected Load() to return an error, got nil")
			}
			if !tt.wantError && err != nil {
				t.Fatalf("expected Load() to succeed, got error: %v", err)
			}
		})
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

// TestLoad_EmitExpectedStumpSet_DefaultTrue pins the default-on semantics of
// the expected-STUMP set flag: a clean Load() yields a non-nil pointer (the
// registered default) and the feature reports enabled. Default TRUE is a
// deliberate divergence from docs/block-processed-completeness.md's original
// "default off" rollout plan — the feature shipped always-on in v0.4.5, so the
// flag is an emergency off-switch, not a rollout gate.
func TestLoad_EmitExpectedStumpSet_DefaultTrue(t *testing.T) {
	clearConfigEnv(t)
	_ = os.Setenv("CONFIG_FILE", "/tmp/nonexistent-config-file.yaml")
	defer func() { _ = os.Unsetenv("CONFIG_FILE") }()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}
	if cfg.Block.EmitExpectedStumpSet == nil {
		t.Fatal("Block.EmitExpectedStumpSet: expected non-nil after Load() (default registered), got nil")
	}
	if !cfg.Block.EmitExpectedStumpSetEnabled() {
		t.Error("Block.EmitExpectedStumpSetEnabled(): expected true by default, got false")
	}
}

// TestLoad_EmitExpectedStumpSet_EnvOverride proves the *bool field decodes
// from the environment through viper's WeaklyTypedInput pipeline: an explicit
// "false" is the only way to disable, and an explicit "true" round-trips.
func TestLoad_EmitExpectedStumpSet_EnvOverride(t *testing.T) {
	tests := []struct {
		name    string
		envVal  string
		enabled bool
	}{
		{"explicit false disables", "false", false},
		{"explicit true enables", envTrue, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearConfigEnv(t)
			_ = os.Setenv("CONFIG_FILE", "/tmp/nonexistent-config-file.yaml")
			_ = os.Setenv("BLOCK_EMIT_EXPECTED_STUMP_SET", tt.envVal)
			defer clearConfigEnv(t)

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() failed: %v", err)
			}
			if cfg.Block.EmitExpectedStumpSet == nil {
				t.Fatal("Block.EmitExpectedStumpSet: expected non-nil when env var is set, got nil")
			}
			if got := cfg.Block.EmitExpectedStumpSetEnabled(); got != tt.enabled {
				t.Errorf("EmitExpectedStumpSetEnabled() with env=%q: expected %v, got %v", tt.envVal, tt.enabled, got)
			}
		})
	}
}

// TestBlockConfig_EmitExpectedStumpSetEnabled_ZeroValue locks in the
// nil-means-enabled contract: a zero-valued BlockConfig (direct struct
// construction, no Load()) behaves like production, so services built in
// tests keep recording and attaching the expected-STUMP set unless a test
// explicitly opts out with a false pointer.
func TestBlockConfig_EmitExpectedStumpSetEnabled_ZeroValue(t *testing.T) {
	var cfg BlockConfig
	if !cfg.EmitExpectedStumpSetEnabled() {
		t.Error("zero-valued BlockConfig: EmitExpectedStumpSetEnabled() expected true (nil means enabled), got false")
	}
}
