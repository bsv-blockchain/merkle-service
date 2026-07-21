package sql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	storepkg "github.com/bsv-blockchain/merkle-service/internal/store"
)

type subtreeAttributionStore struct {
	db     *sql.DB
	d      *dialect
	ttlSec int
}

var _ storepkg.SubtreeAttributionStore = (*subtreeAttributionStore)(nil)

func newSubtreeAttributionStore(db *sql.DB, d *dialect, ttlSec int) *subtreeAttributionStore {
	if ttlSec <= 0 {
		ttlSec = 86400
	}
	return &subtreeAttributionStore{db: db, d: d, ttlSec: ttlSec}
}

func (s *subtreeAttributionStore) TryAttribute(subtreeHash, peerID string) (string, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	qIns := fmt.Sprintf( //nolint:gosec
		"INSERT INTO subtree_attributions (subtree_hash, peer_id, created_at, expires_at) VALUES (%s, %s, %s, %s)%s",
		s.d.placeholder(1), s.d.placeholder(2), s.d.now, s.d.intervalSeconds(s.ttlSec), s.d.onConflictDoNothing,
	)
	res, err := s.db.ExecContext(ctx, qIns, subtreeHash, peerID)
	if err != nil {
		return "", false, fmt.Errorf("insert subtree attribution: %w", err)
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		return peerID, true, nil
	}

	qGet := fmt.Sprintf("SELECT peer_id FROM subtree_attributions WHERE subtree_hash = %s", s.d.placeholder(1)) //nolint:gosec
	var stored string
	if err := s.db.QueryRowContext(ctx, qGet, subtreeHash).Scan(&stored); err != nil {
		return "", false, fmt.Errorf("read subtree attribution: %w", err)
	}
	return stored, false, nil
}
