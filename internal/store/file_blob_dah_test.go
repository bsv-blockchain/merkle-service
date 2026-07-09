package store

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

// The delete-at-height (DAH) bookkeeping must live on disk next to the blobs,
// not in process memory. In the microservice topology the writer (subtree-
// fetcher / subtree-worker) and the pruner (whichever process learns the new
// block height) are DIFFERENT processes sharing one RWX volume, and any of
// them can restart between write and prune. An in-memory map silently orphans
// every blob written by another process or a previous incarnation — which is
// how the dev-ovh-1 subtree-blob-store PVC filled to 100% and wedged the
// fetcher. These tests pin the on-disk contract.

// TestFileBlobStore_DAHPruneSurvivesRestart writes a blob with a DAH through
// one store instance, then prunes through a NEW instance on the same
// directory — the restart case.
func TestFileBlobStore_DAHPruneSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore(writer): %v", err)
	}
	if setErr := writer.Set("aa11", []byte("subtree-bytes"), WithDeleteAtHeight(10)); setErr != nil {
		t.Fatalf("Set: %v", setErr)
	}

	// Simulate a process restart: fresh instance, same directory.
	pruner, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore(pruner): %v", err)
	}
	pruner.SetCurrentBlockHeight(10)

	if _, err := pruner.Get("aa11"); !errors.Is(err, ErrBlobNotFound) {
		t.Errorf("blob should be pruned at its DAH by a fresh instance; Get err = %v", err)
	}
}

// TestFileBlobStore_DAHPruneAcrossLiveInstances covers the cross-process
// case: the writer and the pruner are both alive, on the same directory,
// mirroring two pods sharing the PVC.
func TestFileBlobStore_DAHPruneAcrossLiveInstances(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore(writer): %v", err)
	}
	pruner, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore(pruner): %v", err)
	}

	if err := writer.Set("bb22", []byte("data"), WithDeleteAtHeight(5)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	pruner.SetCurrentBlockHeight(5)

	if _, err := writer.Get("bb22"); !errors.Is(err, ErrBlobNotFound) {
		t.Errorf("blob written by one instance should be pruned by another; Get err = %v", err)
	}
}

// TestFileBlobStore_PrunePreservesFutureDAH pins that pruning at height H
// removes only blobs whose DAH <= H, and that the surviving blob's schedule
// remains effective for a later prune — including one issued by yet another
// fresh instance.
func TestFileBlobStore_PrunePreservesFutureDAH(t *testing.T) {
	dir := t.TempDir()

	bs, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore: %v", err)
	}
	if setErr := bs.Set("early", []byte("x"), WithDeleteAtHeight(5)); setErr != nil {
		t.Fatalf("Set(early): %v", setErr)
	}
	if setErr := bs.Set("late", []byte("y"), WithDeleteAtHeight(7)); setErr != nil {
		t.Fatalf("Set(late): %v", setErr)
	}

	bs.SetCurrentBlockHeight(5)

	if _, getErr := bs.Get("early"); !errors.Is(getErr, ErrBlobNotFound) {
		t.Errorf("early blob should be pruned at 5; Get err = %v", getErr)
	}
	if _, getErr := bs.Get("late"); getErr != nil {
		t.Errorf("late blob (DAH 7) must survive prune at 5; Get err = %v", getErr)
	}

	// A fresh instance must still know about the DAH-7 schedule.
	bs2, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore(bs2): %v", err)
	}
	bs2.SetCurrentBlockHeight(7)
	if _, err := bs2.Get("late"); !errors.Is(err, ErrBlobNotFound) {
		t.Errorf("late blob should be pruned at 7 by fresh instance; Get err = %v", err)
	}
}

// TestFileBlobStore_DAHPrunesNestedKeys pins that namespaced keys
// ("stump/<ref>") prune exactly like flat ones.
func TestFileBlobStore_DAHPrunesNestedKeys(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore: %v", err)
	}
	key := "stump/3292be80a8cd32bc53582b666a1f13564259281a256a6b40aae0bc83c4d50a4d"
	if setErr := writer.Set(key, []byte("stump-bytes"), WithDeleteAtHeight(3)); setErr != nil {
		t.Fatalf("Set: %v", setErr)
	}

	pruner, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore(pruner): %v", err)
	}
	pruner.SetCurrentBlockHeight(3)

	if _, err := pruner.Get(key); !errors.Is(err, ErrBlobNotFound) {
		t.Errorf("nested-key blob should be pruned; Get err = %v", err)
	}
}

// TestFileBlobStore_ScheduleIsNotCancelable pins the schedule contract:
// once a delete-at-height is recorded for a key, it fires when the height is
// reached, deleting whatever file is then at that key — an intervening Del
// (or Del + re-store) does not cancel it. Schedules are append-only because
// the bookkeeping is shared across processes; cancellation would require
// rewriting another process's manifest. This is safe for this store's data:
// blobs are content-addressed, so a re-stored key holds the same bytes and a
// pruned blob is simply re-fetched on next use.
func TestFileBlobStore_ScheduleIsNotCancelable(t *testing.T) {
	dir := t.TempDir()

	bs, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore: %v", err)
	}
	if err := bs.Set("cc33", []byte("v1"), WithDeleteAtHeight(4)); err != nil {
		t.Fatalf("Set(v1): %v", err)
	}
	if err := bs.Del("cc33"); err != nil {
		t.Fatalf("Del: %v", err)
	}
	// Re-store the same key without a DAH; the earlier schedule still fires.
	if err := bs.Set("cc33", []byte("v1")); err != nil {
		t.Fatalf("Set again: %v", err)
	}

	bs.SetCurrentBlockHeight(4)

	if _, err := bs.Get("cc33"); !errors.Is(err, ErrBlobNotFound) {
		t.Errorf("schedule must fire at height 4 despite Del+re-store; Get err = %v", err)
	}
}

// TestFileBlobStore_RejectsDAHNamespaceKeys pins that blob keys cannot write
// into the on-disk DAH bookkeeping area (F-038 spirit: keys are
// network-adjacent, the bookkeeping must not be forgeable through Set).
func TestFileBlobStore_RejectsDAHNamespaceKeys(t *testing.T) {
	dir := t.TempDir()

	bs, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore: %v", err)
	}
	for _, key := range []string{".dah", ".dah/5/evil.list"} {
		if err := bs.Set(key, []byte("x")); !errors.Is(err, ErrBlobKeyEscapesRoot) {
			t.Errorf("Set(%q) must be rejected with ErrBlobKeyEscapesRoot, got %v", key, err)
		}
	}
}

// TestFileBlobStore_PruneIgnoresMaliciousManifestEntries pins that even if a
// manifest file somehow contains a traversal key, pruning validates each key
// against the root before deleting — nothing outside the store root may be
// removed.
func TestFileBlobStore_PruneIgnoresMaliciousManifestEntries(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "blobs")

	victim := filepath.Join(parent, "victim.txt")
	if err := os.WriteFile(victim, []byte("do not delete"), 0o600); err != nil {
		t.Fatalf("writing victim file: %v", err)
	}

	bs, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore: %v", err)
	}

	// Forge a manifest the way an attacker with volume access might.
	manifestDir := filepath.Join(dir, ".dah", "2")
	if err := os.MkdirAll(manifestDir, 0o750); err != nil {
		t.Fatalf("mkdir manifest dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(manifestDir, "evil.list"), []byte("../victim.txt\n"), 0o600); err != nil {
		t.Fatalf("writing forged manifest: %v", err)
	}

	bs.SetCurrentBlockHeight(2)

	if _, err := os.Stat(victim); err != nil {
		t.Errorf("file outside store root must never be deleted by pruning: %v", err)
	}
}

// TestFileBlobStore_ScheduleDeleteWithoutRewrite pins the API the
// subtree-worker uses at block time: a blob that was stored earlier with no
// DAH (announcement-time write, height unknown) gets its delete schedule
// attached later WITHOUT rewriting the blob bytes — and the schedule is
// honored by a different, fresh instance, strictly after the height passes.
func TestFileBlobStore_ScheduleDeleteWithoutRewrite(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore(writer): %v", err)
	}
	if setErr := writer.Set("dd44", []byte("announced-subtree")); setErr != nil {
		t.Fatalf("Set: %v", setErr)
	}
	if schedErr := writer.ScheduleDelete("dd44", 6); schedErr != nil {
		t.Fatalf("ScheduleDelete: %v", schedErr)
	}

	pruner, err := NewFileBlobStore(dir)
	if err != nil {
		t.Fatalf("NewFileBlobStore(pruner): %v", err)
	}

	pruner.SetCurrentBlockHeight(5)
	if _, err := pruner.Get("dd44"); err != nil {
		t.Errorf("blob must survive prune below its scheduled height; Get err = %v", err)
	}

	pruner.SetCurrentBlockHeight(6)
	if _, err := pruner.Get("dd44"); !errors.Is(err, ErrBlobNotFound) {
		t.Errorf("blob must be pruned at its scheduled height; Get err = %v", err)
	}
}

// TestFileBlobStore_ScheduleDeleteRejectsInvalidKey pins that schedule
// entries go through the same key validation as blob writes.
func TestFileBlobStore_ScheduleDeleteRejectsInvalidKey(t *testing.T) {
	bs, err := NewFileBlobStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileBlobStore: %v", err)
	}
	if err := bs.ScheduleDelete("../escape", 3); !errors.Is(err, ErrBlobKeyEscapesRoot) {
		t.Errorf("ScheduleDelete with traversal key must be rejected, got %v", err)
	}
}

// TestBlobStore_ScheduleDeleteIsInterfaceMethod pins that ScheduleDelete is
// part of the BlobStore contract (the subtree store wrapper reaches it
// through the interface) and that the in-memory implementation honors the
// same semantics.
func TestBlobStore_ScheduleDeleteIsInterfaceMethod(t *testing.T) {
	var bs BlobStore = NewMemoryBlobStore()

	if err := bs.Set("ee55", []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := bs.ScheduleDelete("ee55", 4); err != nil {
		t.Fatalf("ScheduleDelete: %v", err)
	}

	bs.SetCurrentBlockHeight(3)
	if _, err := bs.Get("ee55"); err != nil {
		t.Errorf("blob must survive prune below scheduled height; Get err = %v", err)
	}
	bs.SetCurrentBlockHeight(4)
	if _, err := bs.Get("ee55"); !errors.Is(err, ErrBlobNotFound) {
		t.Errorf("memory blob must be pruned at scheduled height; Get err = %v", err)
	}
}

// TestSubtreeStore_ScheduleDeleteAppliesDAHOffset pins the SubtreeStore-level
// wrapper: ScheduleDelete(id, blockHeight) schedules at blockHeight +
// dahOffset, exactly like StoreSubtree does for its DAH — so the worker's
// "blob already present" path and its "re-fetched from DataHub" path prune at
// the same height.
func TestSubtreeStore_ScheduleDeleteAppliesDAHOffset(t *testing.T) {
	blob := NewMemoryBlobStore()
	st := NewSubtreeStore(blob, 2, slog.New(slog.NewTextHandler(io.Discard, nil))) // dahOffset = 2

	if err := st.StoreSubtree("ff66", []byte("data"), 0); err != nil { // no DAH
		t.Fatalf("StoreSubtree: %v", err)
	}
	if err := st.ScheduleDelete("ff66", 10); err != nil { // mined at 10 → DAH 12
		t.Fatalf("ScheduleDelete: %v", err)
	}

	st.SetCurrentBlockHeight(11)
	if _, err := st.GetSubtree("ff66"); err != nil {
		t.Errorf("subtree must survive below blockHeight+dahOffset; err = %v", err)
	}
	st.SetCurrentBlockHeight(12)
	if _, err := st.GetSubtree("ff66"); err == nil {
		t.Error("subtree must be pruned at blockHeight+dahOffset")
	}
}
