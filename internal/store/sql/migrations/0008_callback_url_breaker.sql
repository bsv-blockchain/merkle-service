-- 0006: Circuit breaker for permanently-failing callback URLs.
--
-- The callback-delivery service records a failure against a URL each time one
-- of its messages lands in the DLQ (permanent error or retries exhausted).
-- Once failure_count crosses the configured breaker threshold the URL is
-- disabled (disabled_at set), and GetAll stops returning it so BLOCK_PROCESSED
-- and STUMP fan-out no longer target a dead endpoint. A fresh /watch (Add)
-- resets failure_count to 0 and clears disabled_at, re-enabling the URL within
-- one round-trip once the tenant comes back.
--
-- This stops a fleet of dead callback URLs (e.g. offline ngrok dev tunnels)
-- from soaking delivery throughput and delaying live tenants' callbacks.
ALTER TABLE callback_urls ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE callback_urls ADD COLUMN disabled_at ${TIMESTAMPTZ};
CREATE INDEX ${IF_NOT_EXISTS_INDEX} idx_callback_urls_disabled_at ON callback_urls (disabled_at);
