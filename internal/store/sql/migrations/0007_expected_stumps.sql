-- expected_stumps: per (block_hash, callback_url) set of subtree indices that
-- produced a STUMP for that URL. Surfaced on BLOCK_PROCESSED so the receiver
-- knows exactly which STUMPs to expect and can detect a missing one. One row
-- per (block, url, index); add-if-absent gives idempotency under retries.
-- Rows expire via expires_at (swept), re-stamped on every add — same lifetime
-- as subtree_counters (both live until the block is fully processed).
CREATE TABLE IF NOT EXISTS expected_stumps (
    block_hash    TEXT NOT NULL,
    callback_url  TEXT NOT NULL,
    subtree_index INTEGER NOT NULL,
    expires_at    ${TIMESTAMPTZ},
    PRIMARY KEY (block_hash, callback_url, subtree_index)
);

CREATE INDEX ${IF_NOT_EXISTS_INDEX} idx_expected_stumps_expires_at ON expected_stumps (expires_at);
CREATE INDEX ${IF_NOT_EXISTS_INDEX} idx_expected_stumps_lookup ON expected_stumps (block_hash, callback_url);
