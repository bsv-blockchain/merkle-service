-- 0009: Bound seen-counter growth with a TTL.
--
-- Pre-fix: seen_counters rows only ever left the table via the mine-time
-- BatchDelete, which covers just the REGISTERED txids of blocks the service
-- successfully processes. Counters for unregistered txids, txids that never
-- get mined, and txids mined during a block-processing outage lived forever,
-- growing the table without bound (the Aerospike sibling grew until the
-- namespace hit stop-writes and took the whole write path down).
--
-- Add an `expires_at` column, re-stamped on every increment so it acts as an
-- inactivity window, and swept like the other counter tables. The column is
-- nullable so existing rows survive the migration; each row picks up an
-- expiry on its next increment, and rows never touched again are reclaimed
-- by operators or left to the mine-time delete as before.

ALTER TABLE seen_counters ADD COLUMN expires_at ${TIMESTAMPTZ};

CREATE INDEX ${IF_NOT_EXISTS_INDEX} idx_seen_counters_expires_at ON seen_counters (expires_at);
