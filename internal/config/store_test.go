package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTempConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

func loadWith(t *testing.T, env map[string]string, yaml string) (*Config, error) {
	t.Helper()
	for k, v := range env {
		t.Setenv(k, v)
	}
	if yaml != "" {
		t.Setenv("CONFIG_FILE", writeTempConfig(t, yaml))
	} else {
		// Ensure we don't accidentally pick up the repo config.yaml.
		t.Setenv("CONFIG_FILE", writeTempConfig(t, "mode: all-in-one\n"))
	}
	return Load()
}

func TestStoreBackend_DefaultsToAerospike(t *testing.T) {
	cfg, err := loadWith(t, nil, "")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Store.Backend != BackendAerospike {
		t.Fatalf("default backend = %q, want %q", cfg.Store.Backend, BackendAerospike)
	}
}

func TestStoreBackend_ExplicitAerospike(t *testing.T) {
	cfg, err := loadWith(t, nil, "store:\n  backend: aerospike\n")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Store.Backend != BackendAerospike {
		t.Fatalf("backend = %q, want %q", cfg.Store.Backend, BackendAerospike)
	}
}

func TestStoreBackend_ExplicitSQL(t *testing.T) {
	yaml := "" +
		"store:\n" +
		"  backend: sql\n" +
		"  sql:\n" +
		"    driver: sqlite\n" +
		"    dsn: \":memory:\"\n" +
		"    maxOpenConns: 5\n" +
		"    maxIdleConns: 2\n" +
		"    connMaxIdleTimeSec: 60\n" +
		"    connMaxLifetimeSec: 900\n"
	cfg, err := loadWith(t, nil, yaml)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Store.Backend != BackendSQL {
		t.Fatalf("backend = %q, want %q", cfg.Store.Backend, BackendSQL)
	}
	if cfg.Store.SQL.Driver != "sqlite" {
		t.Fatalf("driver = %q, want sqlite", cfg.Store.SQL.Driver)
	}
	if cfg.Store.SQL.DSN != ":memory:" {
		t.Fatalf("dsn = %q, want :memory:", cfg.Store.SQL.DSN)
	}
	if cfg.Store.SQL.MaxOpenConns != 5 {
		t.Fatalf("maxOpenConns = %d, want 5", cfg.Store.SQL.MaxOpenConns)
	}
	if cfg.Store.SQL.MaxIdleConns != 2 {
		t.Fatalf("maxIdleConns = %d, want 2", cfg.Store.SQL.MaxIdleConns)
	}
	if cfg.Store.SQL.ConnMaxIdleTimeSec != 60 {
		t.Fatalf("connMaxIdleTimeSec = %d, want 60", cfg.Store.SQL.ConnMaxIdleTimeSec)
	}
	if cfg.Store.SQL.ConnMaxLifetimeSec != 900 {
		t.Fatalf("connMaxLifetimeSec = %d, want 900", cfg.Store.SQL.ConnMaxLifetimeSec)
	}
}

func TestStoreBackend_InvalidFailsFast(t *testing.T) {
	_, err := loadWith(t, nil, "store:\n  backend: cassandra\n")
	if err == nil {
		t.Fatal("expected error for invalid backend, got nil")
	}
}

func TestStoreBackend_EnvOverride(t *testing.T) {
	cfg, err := loadWith(t, map[string]string{"STORE_BACKEND": "sql", "STORE_SQL_DSN": "postgres://x"}, "")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Store.Backend != BackendSQL {
		t.Fatalf("backend = %q, want sql", cfg.Store.Backend)
	}
	if cfg.Store.SQL.DSN != "postgres://x" {
		t.Fatalf("dsn = %q, want postgres://x", cfg.Store.SQL.DSN)
	}
}
