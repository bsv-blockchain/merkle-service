package metrics

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"
	astypes "github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/prometheus/client_golang/prometheus"
)

// Backend label values.
const (
	BackendAerospike = "aerospike"
	BackendSQL       = "sql"
)

// Store label values. Keep this list aligned with the store packages —
// new stores should add a constant here so reviewers can audit the enum.
const (
	StoreRegistration        = "registration"
	StoreCallbackDedup       = "callback_dedup"
	StoreCallbackURLRegistry = "callback_url_registry"
	StoreSeenCounter         = "seen_counter"
	StoreSubtreeCounter      = "subtree_counter"
	StoreCallbackAccumulator = "callback_accumulator"
	StoreDataHubRegistry     = "datahub_registry"
	StoreStump               = "stump"
	StoreSubtreeBlob         = "subtree_blob"
)

// Operation label values.
const (
	OpGet            = "get"
	OpBatchGet       = "batchget"
	OpPut            = "put"
	OpAdd            = "add"
	OpIncrement      = "increment"
	OpDecrement      = "decrement"
	OpExists         = "exists"
	OpRecord         = "record"
	OpDelete         = "delete"
	OpSweep          = "sweep"
	OpGetAll         = "get_all"
	OpUpdateTTL      = "update_ttl"
	OpBatchUpdateTTL = "batch_update_ttl"
)

var (
	dbOpDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_db_operation_duration_seconds",
			Help:    "Database operation duration in seconds.",
			Buckets: DBBuckets,
		},
		[]string{"backend", "store", "op", "outcome"},
	)

	dbOpTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "merkle_db_operations_total",
			Help: "Total database operations.",
		},
		[]string{"backend", "store", "op", "outcome"},
	)

	dbBatchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_db_batch_size",
			Help:    "Number of keys per batch database operation.",
			Buckets: CountBuckets,
		},
		[]string{"store", "op"},
	)

	dbSQLPoolOpen = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "merkle_db_sql_pool_open",
		Help: "Open SQL connections (in use + idle).",
	})

	dbSQLPoolIdle = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "merkle_db_sql_pool_idle",
		Help: "Idle SQL connections.",
	})

	dbSQLPoolInUse = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "merkle_db_sql_pool_in_use",
		Help: "SQL connections currently checked out.",
	})

	dbSQLPoolWaitCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "merkle_db_sql_pool_wait_count_total",
		Help: "Cumulative count of connection waits.",
	})

	dbSQLPoolWaitDuration = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "merkle_db_sql_pool_wait_duration_seconds_total",
		Help: "Cumulative time spent waiting for a connection, in seconds.",
	})

	dbSweeperDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "merkle_db_sweeper_duration_seconds",
			Help:    "TTL sweeper run duration in seconds.",
			Buckets: DBBuckets,
		},
		[]string{"store"},
	)

	dbSweeperRowsDeleted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "merkle_db_sweeper_rows_deleted_total",
			Help: "Rows deleted by the TTL sweeper.",
		},
		[]string{"store"},
	)
)

func init() {
	Registry.MustRegister(
		dbOpDuration,
		dbOpTotal,
		dbBatchSize,
		dbSQLPoolOpen,
		dbSQLPoolIdle,
		dbSQLPoolInUse,
		dbSQLPoolWaitCount,
		dbSQLPoolWaitDuration,
		dbSweeperDuration,
		dbSweeperRowsDeleted,
	)
}

// DBTimer captures the start of a DB operation. Use StartDB / End in the
// store implementations:
//
//	t := metrics.StartDB(metrics.BackendAerospike, metrics.StoreRegistration, metrics.OpBatchGet)
//	defer func() { t.End(retErr) }()
//
// The defer pattern keeps every code path covered (including panics, though
// Recoverer in the API layer is the catcher of last resort).
type DBTimer struct {
	backend, store, op string
	start              time.Time
}

// StartDB begins timing a DB op.
func StartDB(backend, store, op string) DBTimer {
	return DBTimer{backend: backend, store: store, op: op, start: time.Now()}
}

// End records the elapsed time + outcome for the DB op. Pass the named
// return error from the caller — outcome is derived by ClassifyDBError.
func (t DBTimer) End(err error) {
	outcome := ClassifyDBError(t.backend, err)
	d := time.Since(t.start).Seconds()
	dbOpDuration.WithLabelValues(t.backend, t.store, t.op, outcome).Observe(d)
	dbOpTotal.WithLabelValues(t.backend, t.store, t.op, outcome).Inc()
}

// ObserveDB records a DB operation's duration and outcome, derived from the
// time elapsed since start and the returned error. Useful when StartDB+End
// can't be wrapped around a single statement (e.g. multi-statement code
// blocks) — pass `time.Now()` captured before the operation began.
func ObserveDB(backend, store, op string, start time.Time, err error) {
	outcome := ClassifyDBError(backend, err)
	d := time.Since(start).Seconds()
	dbOpDuration.WithLabelValues(backend, store, op, outcome).Observe(d)
	dbOpTotal.WithLabelValues(backend, store, op, outcome).Inc()
}

// ObserveDBBatchSize records the number of keys in a batch operation.
// Call at the entry of BatchGet / BatchUpdateTTL etc.
func ObserveDBBatchSize(store, op string, n int) {
	if n <= 0 {
		return
	}
	dbBatchSize.WithLabelValues(store, op).Observe(float64(n))
}

// ObserveSweep records a TTL sweeper iteration's duration and the number
// of rows it deleted.
func ObserveSweep(store string, d time.Duration, rowsDeleted int) {
	dbSweeperDuration.WithLabelValues(store).Observe(d.Seconds())
	if rowsDeleted > 0 {
		dbSweeperRowsDeleted.WithLabelValues(store).Add(float64(rowsDeleted))
	}
}

// ClassifyDBError maps a backend-specific error into the bounded outcome
// enum. Aerospike not-found errors and timeouts surface as their own
// labels so they're alertable separately from generic errors.
func ClassifyDBError(backend string, err error) string {
	if err == nil {
		return OutcomeSuccess
	}
	if backend == BackendAerospike {
		var asErr *as.AerospikeError
		if errors.As(err, &asErr) {
			if asErr.Matches(astypes.KEY_NOT_FOUND_ERROR) {
				return OutcomeNotFound
			}
			if asErr.Matches(astypes.TIMEOUT) {
				return OutcomeTimeout
			}
		}
	}
	if errors.Is(err, sql.ErrNoRows) {
		return OutcomeNotFound
	}
	if isTimeoutErr(err) {
		return OutcomeTimeout
	}
	return OutcomeError
}

// RunSQLPoolSampler periodically reads db.Stats() and updates the pool
// gauges + wait counters. Run as a goroutine bound to a cancelable
// context — when ctx is canceled the sampler exits.
//
// Wait counters use delta-add semantics: db.Stats() reports cumulative
// values that can wrap or reset on driver replacement; the sampler only
// adds positive deltas so the Prometheus counter remains monotonic.
func RunSQLPoolSampler(ctx context.Context, db *sql.DB, interval time.Duration, logger *slog.Logger) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()

	var lastWaitCount int64
	var lastWaitDuration time.Duration

	sample := func() {
		stats := db.Stats()
		dbSQLPoolOpen.Set(float64(stats.OpenConnections))
		dbSQLPoolIdle.Set(float64(stats.Idle))
		dbSQLPoolInUse.Set(float64(stats.InUse))

		if stats.WaitCount > lastWaitCount {
			dbSQLPoolWaitCount.Add(float64(stats.WaitCount - lastWaitCount))
			lastWaitCount = stats.WaitCount
		} else if stats.WaitCount < lastWaitCount {
			lastWaitCount = stats.WaitCount
		}
		if stats.WaitDuration > lastWaitDuration {
			dbSQLPoolWaitDuration.Add((stats.WaitDuration - lastWaitDuration).Seconds())
			lastWaitDuration = stats.WaitDuration
		} else if stats.WaitDuration < lastWaitDuration {
			lastWaitDuration = stats.WaitDuration
		}
	}

	sample()
	for {
		select {
		case <-ctx.Done():
			if logger != nil {
				logger.Debug("SQL pool sampler stopped")
			}
			return
		case <-t.C:
			sample()
		}
	}
}
