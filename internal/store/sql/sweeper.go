package sql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// ttlTables lists every table with an expires_at column. The sweeper deletes
// expired rows from each on every tick.
var ttlTables = []string{
	"registrations",
	"callback_dedup",
	"subtree_counters",
	"callback_accumulator",
}

// sweeper runs a periodic DELETE pass over TTL-bearing tables. One goroutine
// for the whole registry; stops when its context is cancelled.
type sweeper struct {
	db       *sql.DB
	d        *dialect
	interval time.Duration
	logger   *slog.Logger
	done     chan struct{}
	once     sync.Once
}

func newSweeper(db *sql.DB, d *dialect, interval time.Duration, logger *slog.Logger) *sweeper {
	return &sweeper{
		db:       db,
		d:        d,
		interval: interval,
		logger:   logger,
		done:     make(chan struct{}),
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
	for _, table := range ttlTables {
		rows, err := s.sweepTable(ctx, table)
		if err != nil {
			if s.logger != nil {
				s.logger.Warn("ttl sweeper: delete failed", "table", table, "error", err)
			}
			continue
		}
		if rows > 0 && s.logger != nil {
			s.logger.Debug("ttl sweeper: expired rows deleted", "table", table, "rows", rows)
		}
	}
}

// sweepTable deletes up to 1000 rows per call to keep locks short. Repeats
// until no more rows or a sweeper tick elapses.
func (s *sweeper) sweepTable(ctx context.Context, table string) (int64, error) {
	const batch = 1000
	var total int64
	// Small cap to avoid pathological long sweeps blocking shutdown.
	for i := 0; i < 10; i++ {
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
			return total, err
		}
		n, _ := res.RowsAffected()
		total += n
		if n < batch {
			break
		}
	}
	return total, nil
}
