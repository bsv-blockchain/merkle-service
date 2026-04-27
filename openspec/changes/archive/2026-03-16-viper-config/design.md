## Context

The config package (`internal/config/config.go`) currently has three loading phases — `setDefaults()`, YAML unmarshal, and a 160-line `overrideFromEnv()` function that manually maps 30+ environment variables to config fields with per-type parsing. Viper handles all three phases natively: `SetDefault()` for defaults, `ReadInConfig()` for YAML, and `AutomaticEnv()` for env binding.

The `Config` struct and `Load() (*Config, error)` signature are the public API — every entry point and service depends on them. The refactoring must keep these unchanged.

## Goals / Non-Goals

**Goals:**
- Replace manual env var parsing with Viper's `AutomaticEnv()` + env key replacer
- Preserve exact same env var names (e.g., `AEROSPIKE_HOST` maps to `aerospike.host`)
- Preserve `Load() (*Config, error)` signature and `Config` struct
- Preserve the priority order: env vars > YAML file > defaults
- Support config file path override via `CONFIG_FILE` env var or `--config` flag

**Non-Goals:**
- Adding new config fields (done in other changes)
- Changing the Config struct shape
- Supporting config formats beyond YAML (Viper supports them but we don't need to advertise it)

## Decisions

### 1. Use `mapstructure` tags alongside `yaml` tags

**Decision**: Add `mapstructure` tags to config structs so Viper's `Unmarshal()` maps keys correctly. Keep `yaml` tags for backward compatibility and documentation.

**Rationale**: Viper uses `mapstructure` internally for unmarshalling. Without these tags, nested struct fields with camelCase names (e.g., `setName`) won't map correctly from Viper's dot-notation keys.

**Alternatives considered**:
- Use Viper's `DecoderConfigOption` to use yaml tags: works but fragile; explicit mapstructure tags are the idiomatic approach

### 2. Env var binding via key replacer

**Decision**: Use `viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))` combined with an env prefix-free approach and explicit `viper.BindEnv()` calls for keys whose env name doesn't follow the dot-to-underscore pattern exactly.

**Rationale**: The current env vars use patterns like `AEROSPIKE_HOST` which maps to Viper key `aerospike.host` naturally with the dot-to-underscore replacer. But some keys like `KAFKA_STUMPS_DLQ_TOPIC` need explicit binding to `kafka.stumpsdlqtopic` since the mapstructure key has no underscores.

### 3. Register all defaults via `viper.SetDefault()`

**Decision**: Call `viper.SetDefault()` for every config key instead of building a default struct. This lets Viper's merging work correctly with env vars and YAML.

**Rationale**: If we set defaults via a struct, Viper doesn't know about individual key defaults and env var overrides for nested keys fail silently. Registering each key ensures proper merging.

## Risks / Trade-offs

**[mapstructure tag maintenance]** — Adding `mapstructure` tags to every struct field is a one-time cost but adds visual noise.
→ Mitigation: Tags are co-located with yaml tags, making the mapping explicit and greppable.

**[Viper singleton]** — Viper's default global instance is convenient but not test-friendly.
→ Mitigation: Use `viper.New()` to create an isolated instance in `Load()`, avoiding global state.
