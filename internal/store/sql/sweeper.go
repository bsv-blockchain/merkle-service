package sql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// ttlTable describes a single TTL-bearing parent table, plus any child tables
// whose rows must be deleted alongside the parent. Children are matched by a
// foreign-key column referencing the parent's primary key.
//
// The schema (0001_init.sql) does not declare ON DELETE CASCADE on child
// tables — adding that constraint to SQLite requires a full table-recreate
// dance which is operationally awkward to apply to existing data. Instead we
// enforce the relationship at the application level: every sweep deletes
// orphan-eligible child rows in the same transaction as the parent. See
// F-055 / issue #17.
type ttlTable struct {
	parent    string
	parentKey string // primary key column on parent (also referenced by children)
	children  []ttlChild
}

type ttlChild struct {
	table string
	fk    string // column on child referencing parent.parentKey
}

// ttlTables lists every table with an expires_at column. The sweeper deletes
// expired rows from each on every tick, plus any child rows whose parent is
// expiring in this batch.
var ttlTables = []ttlTable{
	{
		parent:    "registrations",
		parentKey: "txid",
		children:  []ttlChild{{table: "registration_urls", fk: "txid"}},
	},
	{parent: "callback_dedup"},
	{parent: "subtree_counters"},
	{
		parent:    "callback_accumulator",
		parentKey: "block_hash",
		children:  []ttlChild{{table: "callback_accumulator_entries", fk: "block_hash"}},
	},
}

// sweeper runs a periodic DELETE pass over TTL-bearing tables. One goroutine
// for the whole registry; stops when its context is cancelled.
type sweeper struct {
	db       *sql.DB
	d        *dialect
	interval time.Duration
	// urlRetention bounds the callback_urls registry: rows whose last_seen_at
	// is older than now() - urlRetention are evicted on every tick. Zero or
	// negative disables the eviction (don't do that in prod — F-037).
	urlRetention time.Duration
	logger       *slog.Logger
	done         chan struct{}
	once         sync.Once
}

func newSweeper(db *sql.DB, d *dialect, interval, urlRetention time.Duration, logger *slog.Logger) *sweeper {
	return &sweeper{
		db:           db,
		d:            d,
		interval:     interval,
		urlRetention: urlRetention,
		logger:       logger,
		done:         make(chan struct{}),
	}
}

func (s *sweeper) run(ctx context.Context) {
	defer s.once.Do(func() { close(s.done) })

	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.sweepOnce(ctx)
		}
	}
}

// waitStopped blocks until run has returned. Called by Close() so shutdown is
// deterministic (callers don't see a sweeper DELETE race with db.Close).
func (s *sweeper) waitStopped() {
	<-s.done
}

func (s *sweeper) sweepOnce(ctx context.Context) {
	for _, t := range ttlTables {
		rows, err := s.sweepTable(ctx, t)
		if err != nil {
			if s.logger != nil {
				s.logger.Warn("ttl sweeper: delete failed", "table", t.parent, "error", err)
			}
			continue
		}
		if rows > 0 && s.logger != nil {
			s.logger.Debug("ttl sweeper: expired rows deleted", "table", t.parent, "rows", rows)
		}
	}
	// callback_urls uses a different recency model (last_seen_at refreshed on
	// every Add) so it doesn't fit the expires_at-driven ttlTable shape.
	// Sweep it separately, gated on a positive retention.
	if s.urlRetention > 0 {
		rows, err := s.sweepCallbackURLs(ctx)
		if err != nil {
			if s.logger != nil {
				s.logger.Warn("ttl sweeper: callback_urls delete failed", "error", err)
			}
		} else if rows > 0 && s.logger != nil {
			s.logger.Debug("ttl sweeper: expired callback URLs deleted", "rows", rows)
		}
	}
}

// sweepCallbackURLs deletes URLs whose last_seen_at is older than the
// configured retention. Rows with NULL last_seen_at (legacy rows from before
// migration 0002) are left alone — the next Add() stamps last_seen_at and
// brings them under the eviction window.
func (s *sweeper) sweepCallbackURLs(ctx context.Context) (int64, error) {
	cutoff := -int(s.urlRetention / time.Second)
	q := fmt.Sprintf(
		"DELETE FROM callback_urls WHERE last_seen_at IS NOT NULL AND last_seen_at < %s",
		s.d.intervalSeconds(cutoff))
	res, err := s.db.ExecContext(ctx, q)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// sweepTable deletes up to 1000 parent rows per call to keep locks short.
// Repeats until no more rows or a small batch cap is reached. When the parent
// has child tables (see ttlTable.children) each batch runs in a single
// transaction that deletes the matching child rows before the parents — this
// prevents the orphaned-child rows that F-055 / issue #17 reported.
func (s *sweeper) sweepTable(ctx context.Context, t ttlTable) (int64, error) {
	const batch = 1000
	var total int64
	// Small cap to avoid pathological long sweeps blocking shutdown.
	for i := 0; i < 10; i++ {
		n, err := s.sweepTableBatch(ctx, t, batch)
		if err != nil {
			return total, err
		}
		total += n
		if n < int64(batch) {
			break
		}
	}
	return total, nil
}

// sweepTableBatch deletes a single batch of expired parent rows (and any
// matching child rows) inside one transaction, returning the number of parent
// rows deleted.
//
// For tables without children we delete by rowid/ctid in a single statement.
// When children are present we must delete the matching child rows in the
// same transaction; doing so requires a stable set of parent keys shared by
// both DELETEs (otherwise the parent and child DELETEs could race against
// the LIMIT and leave orphans). We solve that by selecting the expired
// parent keys first, then deleting children and parents using IN (those keys).
func (s *sweeper) sweepTableBatch(ctx context.Context, t ttlTable, batch int) (int64, error) {
	if len(t.children) == 0 {
		return s.sweepParentByPhysicalRowID(ctx, t.parent, batch)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	// 1. Snapshot up to `batch` expired parent keys. Holding the txn means
	//    concurrent writers may block (sqlite) or wait for row locks
	//    (postgres) but we never delete a child whose parent we won't also
	//    delete in this same batch.
	var selectQ string
	if isPostgres(s.d) {
		selectQ = fmt.Sprintf(
			"SELECT %s FROM %s WHERE expires_at IS NOT NULL AND expires_at < now() LIMIT %d",
			t.parentKey, t.parent, batch)
	} else {
		selectQ = fmt.Sprintf(
			"SELECT %s FROM %s WHERE expires_at IS NOT NULL AND expires_at < %s LIMIT %d",
			t.parentKey, t.parent, s.d.now, batch)
	}
	rows, err := tx.QueryContext(ctx, selectQ)
	if err != nil {
		return 0, fmt.Errorf("select expired %s keys: %w", t.parent, err)
	}
	var keys []interface{}
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			rows.Close()
			return 0, err
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	if len(keys) == 0 {
		return 0, nil
	}

	placeholders := make([]string, len(keys))
	for i := range keys {
		placeholders[i] = s.d.placeholder(i + 1)
	}
	inList := strings.Join(placeholders, ", ")

	// 2. Delete child rows for those parents.
	for _, c := range t.children {
		q := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)", c.table, c.fk, inList)
		if _, err := tx.ExecContext(ctx, q, keys...); err != nil {
			return 0, fmt.Errorf("delete children %s: %w", c.table, err)
		}
	}

	// 3. Delete the parents themselves.
	q := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)", t.parent, t.parentKey, inList)
	res, err := tx.ExecContext(ctx, q, keys...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return n, nil
}

// sweepParentByPhysicalRowID deletes a batch of expired rows from a
// childless table using the driver's physical row identifier (ctid/rowid).
// This is the historical fast path retained for tables that have no children.
func (s *sweeper) sweepParentByPhysicalRowID(ctx context.Context, table string, batch int) (int64, error) {
	var q string
	if isPostgres(s.d) {
		q = fmt.Sprintf(
			"DELETE FROM %s WHERE ctid IN (SELECT ctid FROM %s WHERE expires_at IS NOT NULL AND expires_at < now() LIMIT %d)",
			table, table, batch)
	} else {
		q = fmt.Sprintf(
			"DELETE FROM %s WHERE rowid IN (SELECT rowid FROM %s WHERE expires_at IS NOT NULL AND expires_at < %s LIMIT %d)",
			table, table, s.d.now, batch)
	}
	res, err := s.db.ExecContext(ctx, q)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}
