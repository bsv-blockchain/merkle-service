-- 0005: DataHub URL registry for the /reprocess flow.
--
-- block-processor records every DataHub URL it successfully fetches block
-- metadata from. The /reprocess endpoint reads this set (alongside operator
-- fallbacks from cfg.DataHub.FallbackURLs) to find a DataHub that can serve
-- a past block when the API caller doesn't know which DataHubs are live.
--
-- Same recency-based eviction model as callback_urls: Add upserts last_seen_at,
-- the sweeper drops rows older than the retention window so a DataHub that
-- permanently goes away eventually disappears from the registry.

CREATE TABLE IF NOT EXISTS datahub_urls (
    datahub_url   TEXT PRIMARY KEY,
    last_seen_at  ${TIMESTAMPTZ}
);

CREATE INDEX ${IF_NOT_EXISTS_INDEX} idx_datahub_urls_last_seen_at ON datahub_urls (last_seen_at);
