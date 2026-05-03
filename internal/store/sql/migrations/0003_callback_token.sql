-- 0003: Per-callback bearer token (paired with arcade /watch callbackToken).
--
-- arcade's callback endpoint requires Authorization: Bearer <cfg.CallbackToken>
-- but until now merkle-service had no concept of a per-callback token. We
-- accept the token via /watch, persist it alongside the URL here, propagate
-- it through Kafka, and apply it on outbound HTTP delivery.
--
-- The new column is NOT NULL DEFAULT '' so existing rows survive the
-- migration without a backfill — empty token means "send no Authorization
-- header", which preserves today's behaviour for any deployment that hasn't
-- yet shipped arcade's matching change.

ALTER TABLE registration_urls ADD COLUMN callback_token TEXT NOT NULL DEFAULT '';
