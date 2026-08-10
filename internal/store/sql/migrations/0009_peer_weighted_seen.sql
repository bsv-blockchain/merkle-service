-- Peer-weighted SEEN_MULTIPLE_NODES scoring + first-seen block/subtree attributions.

CREATE TABLE IF NOT EXISTS block_attributions (
    block_hash  TEXT PRIMARY KEY,
    prev_hash   TEXT NOT NULL DEFAULT '',
    height      INTEGER NOT NULL,
    peer_id     TEXT NOT NULL,
    created_at  ${TIMESTAMPTZ} NOT NULL
);

CREATE INDEX ${IF_NOT_EXISTS_INDEX} idx_block_attributions_height ON block_attributions (height);

CREATE TABLE IF NOT EXISTS subtree_attributions (
    subtree_hash TEXT PRIMARY KEY,
    peer_id      TEXT NOT NULL,
    created_at   ${TIMESTAMPTZ} NOT NULL,
    expires_at   ${TIMESTAMPTZ}
);

CREATE INDEX ${IF_NOT_EXISTS_INDEX} idx_subtree_attributions_expires_at ON subtree_attributions (expires_at);

CREATE TABLE IF NOT EXISTS seen_counter_peers (
    txid    TEXT NOT NULL,
    peer_id TEXT NOT NULL,
    weight  INTEGER NOT NULL,
    PRIMARY KEY (txid, peer_id)
);

CREATE INDEX ${IF_NOT_EXISTS_INDEX} idx_seen_counter_peers_txid ON seen_counter_peers (txid);

-- Incremental score for O(1) updates when a new peer is observed.
ALTER TABLE seen_counters ADD COLUMN score INTEGER NOT NULL DEFAULT 0;
