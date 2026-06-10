-- block_data: JSON-encoded BlockProcessedData (merkle root, subtree count,
-- subtree hashes, coinbase BUMP) stamped at Init and read back when the counter
-- drains to zero so BLOCK_PROCESSED can carry it. Nullable: pre-existing rows
-- and callers that don't supply data leave it NULL, and the worker treats a
-- missing value as "no data" (the consumer falls back to a datahub).
ALTER TABLE subtree_counters ADD COLUMN block_data TEXT;
