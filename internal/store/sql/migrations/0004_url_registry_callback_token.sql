-- 0004: Per-URL bearer token in the callback URL registry.
--
-- Mirror of 0003 for the broadcast registry used by BLOCK_PROCESSED fan-out.
-- Same NOT NULL DEFAULT '' shape so existing rows survive the migration; the
-- application updates the column on every Add() call so an actively
-- registering URL converges on the latest arcade-issued token within one
-- /watch round-trip.

ALTER TABLE callback_urls ADD COLUMN callback_token TEXT NOT NULL DEFAULT '';
