package store

import (
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bsv-blockchain/merkle-service/internal/config"
	"github.com/bsv-blockchain/merkle-service/internal/metrics"
)

// zeroByteMaxAge is how long a zero-byte file may exist before the sweeper
// treats it as failed-write litter. os.WriteFile creates-then-writes, so a
// blob is legitimately zero-byte for the instant a write is in flight; a file
// that has stayed empty for minutes is an ENOSPC artifact (the 2026-07-15
// dev-ovh-1 incident left 13,433 of them). Empty blobs are never valid data —
// subtree payloads and STUMPs are non-empty by construction — so this reaps
// them everywhere outside .dah/, regardless of namespace or the caller's age
// threshold.
const zeroByteMaxAge = 5 * time.Minute

// isSubtreeBlobName reports whether name has the shape of a subtree blob key:
// exactly 64 lowercase hex characters, the content-addressed sha256 form every
// subtree is stored under. STUMP blobs share the same hex shape but live under
// the "stump/" prefix (see stumpKeyPrefix), so name shape plus top-level
// placement discriminates the two with certainty.
func isSubtreeBlobName(name string) bool {
	if len(name) != 64 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// SweepOlderThan deletes qualifying blob files and returns how many files and
// bytes it removed. It is the backstop for subtree blobs the delete-at-height
// path can never reach: a blob only gets a DAH schedule when its subtree-work
// item completes, so trimmed queues, long-term parking, and crashes between
// store and schedule orphan blobs forever (dev-ovh-1, 2026-07-15: 39,477 such
// orphans filled a 1TiB volume in ~3h).
//
// Two rules, deliberately narrow:
//
//   - top-level 64-lowercase-hex files (subtree blobs — see isSubtreeBlobName)
//     with mtime older than maxAge are removed. Subtree blobs are a cache:
//     DataHub re-serves them and the worker's miss path re-fetches, so the
//     worst case for a swept blob is one re-fetch.
//   - zero-byte files older than zeroByteMaxAge are removed anywhere outside
//     .dah/ — failed-write (ENOSPC) litter is never valid data.
//
// Everything else is never touched: STUMP blobs (under stump/) are read by
// callback-delivery at delivery time with retry windows up to ~1h, and .dah/
// manifests must survive until their height fires (a schedule outliving its
// blob is a no-op). Keys that match neither rule leak to operators rather
// than risk deleting live data.
//
// The sweep takes no lock and is safe to run concurrently with Set/Get/Del
// and the DAH pruner: blob files are content-addressed (a concurrent
// re-store simply recreates the key), POSIX keeps a deleted file readable
// through any already-open descriptor, and the .dah/ bookkeeping — the only
// state the store mutates under its mutex — is skipped entirely. FileBlobStore
// keeps no per-key in-memory state, so there is nothing else to reconcile.
//
// Errors on individual files are skipped (a file vanishing mid-walk is
// ordinary next to the DAH pruner); only a failure to walk the root is
// returned.
func (f *FileBlobStore) SweepOlderThan(maxAge time.Duration) (files int, bytes int64, err error) {
	now := time.Now()
	cutoff := now.Add(-maxAge)
	zeroByteCutoff := now.Add(-zeroByteMaxAge)
	dahRoot := filepath.Join(f.rootAbs, dahDirName)

	walkErr := filepath.WalkDir(f.rootAbs, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// Entry vanished mid-walk (concurrent prune/sweep) or unreadable:
			// skip it rather than aborting the whole sweep.
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil //nolint:nilerr // per-entry errors are intentionally non-fatal
		}
		if d.IsDir() {
			if path == dahRoot {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil //nolint:nilerr // entry vanished; nothing to sweep
		}

		size := info.Size()
		switch {
		case size == 0 && info.ModTime().Before(zeroByteCutoff):
			// Failed-write litter: reap early regardless of maxAge.
		case isSubtreeBlobName(d.Name()) && filepath.Dir(path) == f.rootAbs && info.ModTime().Before(cutoff):
			// Orphaned subtree blob: top-level, content-addressed, stale.
		default:
			return nil
		}

		// The walk is rooted at the store's own data directory and blob
		// writes never create symlinks, so the TOCTOU window G122 warns
		// about cannot escape the root; worst case a concurrently-replaced
		// file is removed, which for content-addressed blobs means a
		// harmless refetch.
		if os.Remove(path) == nil { //nolint:gosec // see comment above
			files++
			bytes += size
		}
		return nil
	})
	if walkErr != nil {
		return files, bytes, fmt.Errorf("sweeping blob store %s: %w", f.dir, walkErr)
	}
	return files, bytes, nil
}

// StartAgeSweeper runs SweepOlderThan immediately and then every interval,
// until the returned stop function is called. The stop function is idempotent
// and safe to call concurrently. logger may be nil. Metrics are updated on
// every pass; the INFO log fires only when something was removed so a healthy
// store stays quiet. A non-positive interval is clamped to one hour:
// time.NewTicker panics on values <= 0, and this method is exported
// (StartAgeSweeperFromConfig refuses to start in that case, but direct
// callers deserve the same protection).
func (f *FileBlobStore) StartAgeSweeper(interval, maxAge time.Duration, logger *slog.Logger) (stop func()) {
	if interval <= 0 {
		interval = time.Hour
	}
	sweep := func() {
		start := time.Now()
		files, bytes, err := f.SweepOlderThan(maxAge)
		metrics.AddBlobSweep(files, bytes)
		if logger == nil {
			return
		}
		if err != nil {
			logger.Warn("blob age sweep failed", "dir", f.dir, "error", err)
			return
		}
		if files > 0 {
			logger.Info("blob age sweep removed stale blobs",
				"dir", f.dir,
				"files", files,
				"bytes", bytes,
				"maxAge", maxAge.String(),
				"duration", time.Since(start).String(),
			)
		}
	}
	sweep()

	done := make(chan struct{})
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				sweep()
			}
		}
	}()

	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

// StartAgeSweeperFromConfig starts the age sweeper for file-backed blob
// stores when cfg enables it (both SweepIntervalSec and SweepMaxAgeSec > 0)
// and returns a stop function; for memory stores or disabled config it
// returns a no-op. Called from the block-processor ONLY — it is the single
// replica that already executes DAH prunes on the shared volume, and one
// sweeper avoids every replica of every service walking the same CephFS tree
// each interval.
func StartAgeSweeperFromConfig(blob BlobStore, cfg config.BlobStoreConfig, logger *slog.Logger) (stop func()) {
	fbs, ok := blob.(*FileBlobStore)
	if !ok || cfg.SweepIntervalSec <= 0 || cfg.SweepMaxAgeSec <= 0 {
		return func() {}
	}
	return fbs.StartAgeSweeper(
		time.Duration(cfg.SweepIntervalSec)*time.Second,
		time.Duration(cfg.SweepMaxAgeSec)*time.Second,
		logger,
	)
}
