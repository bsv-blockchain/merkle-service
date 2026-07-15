// Package retryutil holds the retry-backpressure primitives shared by the
// subtree-fetcher (internal/subtree) and the subtree-worker (internal/block):
// an exponential backoff schedule, a context-aware wait, and full-filesystem
// error classification.
//
// Extracted (rather than copied) chiefly for IsDiskFull: its errno-plus-text
// matching is exactly the kind of list that silently forks when duplicated —
// one site learns a new quota spelling, the other keeps dead-lettering. The
// backoff cap is a parameter, not a constant, because the two call sites
// deliberately differ: the fetcher's 3-attempt budget rides a 30s cap, while
// the worker's 10-attempt budget must stay at 5s so a failing 50-record
// consumer chunk (drained without a quit check on graceful revoke) cannot
// sleep past the group's 5m rebalance timeout and get the member fenced.
//
// This package intentionally imports nothing from internal/ so any retry
// path can use it without cycle risk.
package retryutil

import (
	"context"
	"errors"
	"strings"
	"syscall"
	"time"
)

// Backoff returns how long to wait before retry attempt `attempt` (1-based):
// baseMs doubling per attempt, capped at maxBackoff. Returns 0 when the
// backoff is disabled (baseMs <= 0, e.g. a struct-literal test config).
// Attempts < 1 are treated as 1; attempts past the shift-overflow horizon
// (> 30) saturate at maxBackoff, as does any doubled value that exceeds it.
func Backoff(baseMs, attempt int, maxBackoff time.Duration) time.Duration {
	if baseMs <= 0 {
		return 0
	}
	base := time.Duration(baseMs) * time.Millisecond
	if attempt < 1 {
		attempt = 1
	}
	// Doubling past attempt ~32 overflows the shift; anything that far in is
	// at the cap regardless.
	if attempt > 30 {
		return maxBackoff
	}
	d := base << uint(attempt-1)
	if d > maxBackoff || d <= 0 {
		return maxBackoff
	}
	return d
}

// Wait sleeps for d, returning early with ctx.Err() when the context dies
// first (shutdown, lost partition — see the kafka consumer's partitions-lost
// cancellation). d <= 0 returns immediately with nil.
func Wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// IsDiskFull reports whether err is a full-filesystem condition: ENOSPC (via
// the wrapped errno chain) or a quota/space error that only survived as text.
// Callers treat these as operational conditions — park the message under
// Kafka retention, never burn the retry budget, never DLQ.
func IsDiskFull(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ENOSPC) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no space left on device") ||
		strings.Contains(msg, "disk quota exceeded")
}
