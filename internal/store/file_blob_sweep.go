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
)

// SweepOrphans deletes blob files whose modification time is older than
// maxAge and returns how many it removed. It is the backstop for blobs the
// delete-at-height path can never reach:
//
//   - subtrees announced but never mined — no block ever supplies their
//     height, so no schedule is ever recorded for them;
//   - blobs orphaned by a crash between the file write and the schedule
//     append;
//   - files that predate schedule manifests entirely.
//
// Every blob with a knowable height is pruned by the (earlier, event-driven)
// DAH path, so with a generous maxAge the sweeper only ever touches data the
// chain abandoned. The .dah bookkeeping is skipped: manifests must survive
// until their height fires, and a schedule outliving its blob is a no-op.
//
// Errors on individual files are skipped (another replica sweeping the same
// shared volume concurrently makes ENOENT ordinary); only a failure to walk
// the root is returned.
func (f *FileBlobStore) SweepOrphans(maxAge time.Duration) (int, error) {
	cutoff := time.Now().Add(-maxAge)
	dahRoot := filepath.Join(f.rootAbs, dahDirName)

	removed := 0
	err := filepath.WalkDir(f.rootAbs, func(path string, d fs.DirEntry, err error) error {
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
		if info.ModTime().After(cutoff) {
			return nil
		}
		// The walk is rooted at the store's own data directory and blob
		// writes never create symlinks, so the TOCTOU window G122 warns
		// about cannot escape the root; worst case a concurrently-replaced
		// file is removed, which for content-addressed blobs means a
		// harmless refetch.
		if os.Remove(path) == nil { //nolint:gosec // see comment above
			removed++
		}
		return nil
	})
	if err != nil {
		return removed, fmt.Errorf("sweeping blob store %s: %w", f.dir, err)
	}
	return removed, nil
}

// StartOrphanSweeper runs SweepOrphans immediately and then every interval,
// until the returned stop function is called. The stop function is
// idempotent and safe to call concurrently. logger may be nil.
func (f *FileBlobStore) StartOrphanSweeper(interval, maxAge time.Duration, logger *slog.Logger) (stop func()) {
	sweep := func() {
		removed, err := f.SweepOrphans(maxAge)
		if logger == nil {
			return
		}
		if err != nil {
			logger.Warn("blob orphan sweep failed", "dir", f.dir, "error", err)
			return
		}
		if removed > 0 {
			logger.Info("blob orphan sweep removed stale blobs", "dir", f.dir, "removed", removed, "maxAge", maxAge.String())
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

// StartBlobSweeperFromConfig starts the orphan sweeper for file-backed blob
// stores when cfg enables it (OrphanMaxAgeSec > 0) and returns a stop
// function; for memory stores or disabled config it returns a no-op. The
// store factories call this so every process that mounts the shared blob
// volume participates in sweeping — the sweep is idempotent across replicas
// (a file already removed by another sweeper is simply skipped).
func StartBlobSweeperFromConfig(blob BlobStore, cfg config.BlobStoreConfig, logger *slog.Logger) (stop func()) {
	fbs, ok := blob.(*FileBlobStore)
	if !ok || cfg.OrphanMaxAgeSec <= 0 {
		return func() {}
	}
	interval := time.Duration(cfg.SweepIntervalSec) * time.Second
	if interval <= 0 {
		interval = time.Hour
	}
	return fbs.StartOrphanSweeper(interval, time.Duration(cfg.OrphanMaxAgeSec)*time.Second, logger)
}
