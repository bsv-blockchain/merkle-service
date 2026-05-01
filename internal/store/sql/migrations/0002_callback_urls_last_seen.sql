-- 0002: Bound callback_urls registry growth (F-037 / issue #23).
--
-- Pre-fix: every distinct callback URL ever observed lived forever in
-- callback_urls. BLOCK_PROCESSED fan-out iterated the whole table, including
-- URLs whose last associated registration expired weeks ago. Add a
-- `last_seen_at` column so Add() can refresh recency and the sweeper can
-- evict stale URLs after a retention window.
--
-- The new column is nullable so existing rows survive the migration; the
-- application coalesces NULL to "now" on first read after deploy, which gives
-- existing prod records one full retention window before they are eligible
-- for eviction.

ALTER TABLE callback_urls ADD COLUMN last_seen_at ${TIMESTAMPTZ};

CREATE INDEX ${IF_NOT_EXISTS_INDEX} idx_callback_urls_last_seen_at ON callback_urls (last_seen_at);
