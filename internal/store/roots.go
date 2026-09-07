package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

func (s *Store) CreateSyncRoot(ctx context.Context, root domain.SyncRoot) (domain.SyncRoot, error) {
	if err := root.Validate(); err != nil {
		return domain.SyncRoot{}, err
	}
	if root.ID != 0 {
		return domain.SyncRoot{}, fmt.Errorf("new sync root must not already have an ID")
	}
	s.syncRootMu.Lock()
	defer s.syncRootMu.Unlock()
	if err := s.checkSyncRootOwnership(ctx, root); err != nil {
		return domain.SyncRoot{}, err
	}
	if root.CreatedAt.IsZero() {
		root.CreatedAt = s.now()
	} else {
		root.CreatedAt = root.CreatedAt.UTC()
	}

	result, err := s.db.ExecContext(ctx, `
INSERT INTO sync_roots(uuid, local_root, remote_name, remote_root, enabled, poll_interval_seconds, created_at_ns)
VALUES(?, ?, ?, ?, ?, ?, ?)`,
		root.UUID, root.LocalRoot, root.RemoteName, root.RemoteRoot, boolInt(root.Enabled), root.PollIntervalSeconds, root.CreatedAt.UnixNano())
	if err != nil {
		return domain.SyncRoot{}, fmt.Errorf("insert sync root: %w", err)
	}
	root.ID, err = result.LastInsertId()
	if err != nil {
		return domain.SyncRoot{}, fmt.Errorf("read sync root ID: %w", err)
	}
	return root, nil
}

func (s *Store) GetSyncRoot(ctx context.Context, id int64) (domain.SyncRoot, bool, error) {
	if id <= 0 {
		return domain.SyncRoot{}, false, fmt.Errorf("sync root ID must be positive")
	}
	row := s.db.QueryRowContext(ctx, `
SELECT id, uuid, local_root, remote_name, remote_root, enabled, poll_interval_seconds, created_at_ns
FROM sync_roots WHERE id = ?`, id)
	root, err := scanSyncRoot(row)
	if err == sql.ErrNoRows {
		return domain.SyncRoot{}, false, nil
	}
	if err != nil {
		return domain.SyncRoot{}, false, fmt.Errorf("get sync root: %w", err)
	}
	return root, true, nil
}

func (s *Store) ListSyncRoots(ctx context.Context) ([]domain.SyncRoot, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, uuid, local_root, remote_name, remote_root, enabled, poll_interval_seconds, created_at_ns
FROM sync_roots ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list sync roots: %w", err)
	}
	defer rows.Close()

	var roots []domain.SyncRoot
	for rows.Next() {
		root, err := scanSyncRoot(rows)
		if err != nil {
			return nil, fmt.Errorf("scan sync root: %w", err)
		}
		roots = append(roots, root)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sync roots: %w", err)
	}
	return roots, nil
}

// SetSyncRootEnabled pauses/resumes one selected directory pair without
// discarding its committed baseline or conflict history.
func (s *Store) SetSyncRootEnabled(ctx context.Context, id int64, enabled bool) error {
	if id <= 0 {
		return fmt.Errorf("sync root ID must be positive")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE sync_roots SET enabled = ? WHERE id = ?`, boolInt(enabled), id)
	if err != nil {
		return fmt.Errorf("update sync root enabled state: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read updated sync root count: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("sync root %d not found", id)
	}
	return nil
}

func (s *Store) checkSyncRootOwnership(ctx context.Context, candidate domain.SyncRoot) error {
	roots, err := s.ListSyncRoots(ctx)
	if err != nil {
		return err
	}
	for _, existing := range roots {
		if localRootsOverlap(existing.LocalRoot, candidate.LocalRoot) {
			return fmt.Errorf("local sync root %q overlaps configured root %q", candidate.LocalRoot, existing.LocalRoot)
		}
		if existing.RemoteName == candidate.RemoteName && remoteRootsOverlap(existing.RemoteRoot, candidate.RemoteRoot) {
			return fmt.Errorf("remote sync root %q:%q overlaps configured root %q:%q", candidate.RemoteName, candidate.RemoteRoot, existing.RemoteName, existing.RemoteRoot)
		}
	}
	return nil
}

func localRootsOverlap(a, b string) bool {
	return localRootContains(a, b) || localRootContains(b, a)
}

func localRootContains(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil || filepath.IsAbs(rel) {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func remoteRootsOverlap(a, b string) bool {
	return remoteRootContains(a, b) || remoteRootContains(b, a)
}

func remoteRootContains(parent, child string) bool {
	return child == parent || strings.HasPrefix(child, parent+"/")
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanSyncRoot(row rowScanner) (domain.SyncRoot, error) {
	var root domain.SyncRoot
	var enabled int
	var createdNS int64
	if err := row.Scan(&root.ID, &root.UUID, &root.LocalRoot, &root.RemoteName, &root.RemoteRoot, &enabled, &root.PollIntervalSeconds, &createdNS); err != nil {
		return domain.SyncRoot{}, err
	}
	root.Enabled = enabled != 0
	root.CreatedAt = time.Unix(0, createdNS).UTC()
	return root, nil
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
