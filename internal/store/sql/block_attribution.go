package sql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	storepkg "github.com/bsv-blockchain/merkle-service/internal/store"
)

type blockAttributionStore struct {
	db *sql.DB
	d  *dialect
}

var _ storepkg.BlockAttributionStore = (*blockAttributionStore)(nil)

func newBlockAttributionStore(db *sql.DB, d *dialect) *blockAttributionStore {
	return &blockAttributionStore{db: db, d: d}
}

func (s *blockAttributionStore) TryAttribute(hash, prevHash, peerID string, height uint32) (string, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	qIns := fmt.Sprintf( //nolint:gosec // SQL built from internal placeholders, no user input
		"INSERT INTO block_attributions (block_hash, prev_hash, height, peer_id, created_at) VALUES (%s, %s, %s, %s, %s)%s",
		s.d.placeholder(1), s.d.placeholder(2), s.d.placeholder(3), s.d.placeholder(4), s.d.now, s.d.onConflictDoNothing,
	)
	res, err := s.db.ExecContext(ctx, qIns, hash, prevHash, height, peerID)
	if err != nil {
		return "", false, fmt.Errorf("insert block attribution: %w", err)
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		return peerID, true, nil
	}

	qGet := fmt.Sprintf("SELECT peer_id FROM block_attributions WHERE block_hash = %s", s.d.placeholder(1)) //nolint:gosec // placeholder from dialect
	var stored string
	if err := s.db.QueryRowContext(ctx, qGet, hash).Scan(&stored); err != nil {
		return "", false, fmt.Errorf("read block attribution: %w", err)
	}
	return stored, false, nil
}

func (s *blockAttributionStore) ListAll() ([]storepkg.BlockAttribution, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `SELECT block_hash, prev_hash, height, peer_id FROM block_attributions`)
	if err != nil {
		return nil, fmt.Errorf("list block attributions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []storepkg.BlockAttribution
	for rows.Next() {
		var a storepkg.BlockAttribution
		var height int64
		if err := rows.Scan(&a.Hash, &a.PrevHash, &height, &a.PeerID); err != nil {
			return nil, err
		}
		// Heights are chain heights and always fit in uint32 for BSV.
		if height < 0 || height > int64(^uint32(0)) {
			return nil, fmt.Errorf("block height out of range: %d", height)
		}
		a.Height = uint32(height)
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *blockAttributionStore) DeleteHashes(hashes []string) error {
	if len(hashes) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, h := range hashes {
		q := fmt.Sprintf("DELETE FROM block_attributions WHERE block_hash = %s", s.d.placeholder(1)) //nolint:gosec // placeholder from dialect
		if _, err := s.db.ExecContext(ctx, q, h); err != nil {
			return fmt.Errorf("delete block attribution %s: %w", h, err)
		}
	}
	return nil
}
