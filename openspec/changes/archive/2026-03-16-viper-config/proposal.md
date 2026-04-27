## Why

The current configuration system in `internal/config/config.go` uses ~160 lines of hand-written `os.Getenv`/`strconv.Atoi` boilerplate to map environment variables to config fields. Every new config field requires adding a manual env override with type conversion, string splitting for slices, and boolean parsing. This is error-prone and scales poorly as the config surface grows. Viper provides automatic env binding, YAML unmarshalling, and type-safe defaults in a single library, replacing all the manual wiring.

## What Changes

- Replace the manual `setDefaults()` + `yaml.Unmarshal()` + `overrideFromEnv()` pattern with Viper's `SetDefault()`, `ReadInConfig()`, and `AutomaticEnv()` workflow
- Remove the `gopkg.in/yaml.v3` dependency (Viper handles YAML internally)
- Remove the entire `overrideFromEnv()` function (~160 lines) — Viper binds env vars automatically using a configurable key-to-env mapping
- Use Viper's `mapstructure` tags on config structs for proper unmarshalling (or keep `yaml` tags since Viper supports them)
- Keep the `Config` struct and sub-structs unchanged — only the loading mechanism changes
- Keep the `Load() (*Config, error)` function signature unchanged so all callers remain unaffected

## Capabilities

### New Capabilities

(none — this is a refactoring of existing configuration loading)

### Modified Capabilities

- `service-lifecycle`: Configuration loading mechanism changes from manual YAML+env parsing to Viper, but the `Config` struct and `Load()` signature remain identical

## Impact

- **Modified code**: `internal/config/config.go` (major rewrite of loading logic, struct tags stay)
- **Modified tests**: `internal/config/config_test.go` (tests must verify Viper-based loading)
- **Dependencies**: Add `github.com/spf13/viper`; remove `gopkg.in/yaml.v3` (becomes transitive via Viper)
- **No API changes**: `Load() (*Config, error)` signature preserved, all callers unaffected
- **Env var naming**: Current `AEROSPIKE_HOST` style preserved via Viper env key replacer (`.` → `_`, nested keys use `_` separator)
