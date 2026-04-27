-- Initial schema for the merkle-service SQL backend.
-- Dialect placeholders (${BYTEA}, ${TIMESTAMPTZ}, ${IF_NOT_EXISTS_INDEX}) are
-- rewritten per-driver by dialect.rewrite before execution.

CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INTEGER PRIMARY KEY,
    applied_at  ${TIMESTAMPTZ} NOT NULL
);

-- registrations: one row per txid. URLs live in registration_urls for set semantics.
CREATE TABLE IF NOT EXISTS registrations (
    txid        TEXT PRIMARY KEY,
    expires_at  ${TIMESTAMPTZ}
);

CREATE TABLE IF NOT EXISTS registration_urls (
    txid         TEXT NOT NULL,
    callback_url TEXT NOT NULL,
    PRIMARY KEY (txid, callback_url)
);

CREATE INDEX ${IF_NOT_EXISTS_INDEX} idx_registrations_expires_at ON registrations (expires_at);
CREATE INDEX ${IF_NOT_EXISTS_INDEX} idx_registration_urls_txid ON registration_urls (txid);

-- callback_dedup: one row per (txid,url,status_type) hash. Ephemeral.
CREATE TABLE IF NOT EXISTS callback_dedup (
    dedup_key   TEXT PRIMARY KEY,
    expires_at  ${TIMESTAMPTZ}
);

CREATE INDEX ${IF_NOT_EXISTS_INDEX} idx_callback_dedup_expires_at ON callback_dedup (expires_at);

-- callback_urls: set of all known callback URLs (for broadcast).
CREATE TABLE IF NOT EXISTS callback_urls (
    callback_url TEXT PRIMARY KEY
);

-- seen_counters: one row per txid. Threshold-fired tracked separately.
CREATE TABLE IF NOT EXISTS seen_counters (
    txid            TEXT PRIMARY KEY,
    threshold_fired INTEGER NOT NULL DEFAULT 0
);

-- seen_counter_subtrees: child table holding distinct subtreeIDs per txid.
CREATE TABLE IF NOT EXISTS seen_counter_subtrees (
    txid        TEXT NOT NULL,
    subtree_id  TEXT NOT NULL,
    PRIMARY KEY (txid, subtree_id)
);

CREATE INDEX ${IF_NOT_EXISTS_INDEX} idx_seen_counter_subtrees_txid ON seen_counter_subtrees (txid);

-- subtree_counters: atomic per-block remaining count for BLOCK_PROCESSED.
CREATE TABLE IF NOT EXISTS subtree_counters (
    block_hash  TEXT PRIMARY KEY,
    remaining   INTEGER NOT NULL,
    expires_at  ${TIMESTAMPTZ}
);

CREATE INDEX ${IF_NOT_EXISTS_INDEX} idx_subtree_counters_expires_at ON subtree_counters (expires_at);

-- callback_accumulator: per-block aggregation of callback entries across subtrees.
CREATE TABLE IF NOT EXISTS callback_accumulator (
    block_hash  TEXT PRIMARY KEY,
    expires_at  ${TIMESTAMPTZ}
);

CREATE TABLE IF NOT EXISTS callback_accumulator_entries (
    id             ${AUTOINC},
    block_hash     TEXT NOT NULL,
    callback_url   TEXT NOT NULL,
    subtree_index  INTEGER NOT NULL,
    txids_json     TEXT NOT NULL,
    stump_data     ${BYTEA} NOT NULL
);

CREATE INDEX ${IF_NOT_EXISTS_INDEX} idx_callback_accumulator_expires_at ON callback_accumulator (expires_at);
CREATE INDEX ${IF_NOT_EXISTS_INDEX} idx_callback_accumulator_entries_block ON callback_accumulator_entries (block_hash);
