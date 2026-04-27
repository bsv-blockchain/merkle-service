## 1. Dependencies

- [x] 1.1 Add `github.com/spf13/viper` to `go.mod` and run `go mod tidy`

## 2. Config Struct Tags

- [x] 2.1 Add `mapstructure` tags to all fields in `Config`, `APIConfig`, `AerospikeConfig`, `KafkaConfig`, `P2PConfig`, `SubtreeConfig`, `BlockConfig`, `CallbackConfig`, `BlobStoreConfig` structs (matching the yaml tag names)

## 3. Rewrite Config Loading

- [x] 3.1 Rewrite `Load()` to use `viper.New()`: set config name/type/paths, call `ReadInConfig()`, handle file-not-found gracefully
- [x] 3.2 Register all defaults via `viper.SetDefault()` calls replacing `setDefaults()`
- [x] 3.3 Configure env var binding: `SetEnvKeyReplacer` for dot-to-underscore mapping, and explicit `BindEnv()` calls for keys whose env name doesn't follow the simple pattern (e.g., `kafka.stumpsDlqTopic` → `KAFKA_STUMPS_DLQ_TOPIC`)
- [x] 3.4 Unmarshal Viper config into the `Config` struct via `viper.Unmarshal()`
- [x] 3.5 Remove the `overrideFromEnv()` function and `setDefaults()` function
- [x] 3.6 Remove direct `gopkg.in/yaml.v3` import (now transitive via Viper)

## 4. Tests

- [x] 4.1 Update `internal/config/config_test.go`: verify defaults, YAML file loading, env var overrides, slice/bool env vars, and invalid YAML error
- [x] 4.2 Verify all existing unit tests pass (`go test ./...`)

## 5. Cleanup

- [x] 5.1 Run `go mod tidy` to remove unused direct dependencies
- [x] 5.2 Verify full build succeeds (`go build ./...`)
