package store

import (
	"context"
	"fmt"
	"time"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

func (s *Store) CreateConflict(ctx context.Context, conflict domain.Conflict) (domain.Conflict, error) {
	if conflict.ID != 0 {
		return domain.Conflict{}, fmt.Errorf("new conflict must not already have an ID")
	}
	if conflict.Resolved {
		return domain.Conflict{}, fmt.Errorf("new conflict must start unresolved")
	}
	if conflict.CreatedAt.IsZero() {
		conflict.CreatedAt = s.now()
	} else {
		conflict.CreatedAt = conflict.CreatedAt.UTC()
	}
	conflict.ResolvedAt = time.Time{}
	if err := conflict.Validate(); err != nil {
		return domain.Conflict{}, err
	}

	result, err := s.db.ExecContext(ctx, `
INSERT INTO conflicts(
    sync_root_id, rel_path, kind,
    local_present, local_kind, local_size, local_mtime_ns,
    remote_present, remote_kind, remote_id, remote_rev, remote_size, remote_mtime_us,
    resolved, created_at_ns, resolved_at_ns
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 0)`,
		conflict.SyncRootID,
		conflict.RelPath,
		string(conflict.Kind),
		boolInt(conflict.Local.Present),
		string(conflict.Local.Kind),
		conflict.Local.Size,
		conflict.Local.MtimeNS,
		boolInt(conflict.Remote.Present),
		string(conflict.Remote.Kind),
		conflict.Remote.ID,
		conflict.Remote.Rev,
		conflict.Remote.Size,
		conflict.Remote.MtimeUS,
		conflict.CreatedAt.UnixNano(),
	)
	if err != nil {
		return domain.Conflict{}, fmt.Errorf("insert conflict: %w", err)
	}
	conflict.ID, err = result.LastInsertId()
	if err != nil {
		return domain.Conflict{}, fmt.Errorf("read conflict ID: %w", err)
	}
	return conflict, nil
}

func (s *Store) ListConflicts(ctx context.Context, syncRootID int64, unresolvedOnly bool) ([]domain.Conflict, error) {
	if syncRootID <= 0 {
		return nil, fmt.Errorf("sync root ID must be positive")
	}
	query := conflictSelect + ` WHERE sync_root_id = ?`
	if unresolvedOnly {
		query += ` AND resolved = 0`
	}
	query += ` ORDER BY id`
	rows, err := s.db.QueryContext(ctx, query, syncRootID)
	if err != nil {
		return nil, fmt.Errorf("list conflicts: %w", err)
	}
	defer rows.Close()

	var conflicts []domain.Conflict
	for rows.Next() {
		conflict, err := scanConflict(rows)
		if err != nil {
			return nil, fmt.Errorf("scan conflict: %w", err)
		}
		conflicts = append(conflicts, conflict)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate conflicts: %w", err)
	}
	return conflicts, nil
}

func (s *Store) ResolveConflict(ctx context.Context, id int64) error {
	if id <= 0 {
		return fmt.Errorf("conflict ID must be positive")
	}
	result, err := s.db.ExecContext(ctx, `
UPDATE conflicts
SET resolved = 1, resolved_at_ns = ?
WHERE id = ? AND resolved = 0`, s.now().UnixNano(), id)
	if err != nil {
		return fmt.Errorf("resolve conflict: %w", err)
	}
	return requireOneRow(result, "unresolved conflict")
}

const conflictSelect = `
SELECT id, sync_root_id, rel_path, kind,
       local_present, local_kind, local_size, local_mtime_ns,
       remote_present, remote_kind, remote_id, remote_rev, remote_size, remote_mtime_us,
       resolved, created_at_ns, resolved_at_ns
FROM conflicts`

func scanConflict(row rowScanner) (domain.Conflict, error) {
	var conflict domain.Conflict
	var kind, localKind, remoteKind string
	var localPresent, remotePresent, resolved int
	var createdNS, resolvedNS int64
	if err := row.Scan(
		&conflict.ID,
		&conflict.SyncRootID,
		&conflict.RelPath,
		&kind,
		&localPresent,
		&localKind,
		&conflict.Local.Size,
		&conflict.Local.MtimeNS,
		&remotePresent,
		&remoteKind,
		&conflict.Remote.ID,
		&conflict.Remote.Rev,
		&conflict.Remote.Size,
		&conflict.Remote.MtimeUS,
		&resolved,
		&createdNS,
		&resolvedNS,
	); err != nil {
		return domain.Conflict{}, err
	}
	conflict.Kind = domain.ConflictKind(kind)
	conflict.Local.Present = localPresent != 0
	if conflict.Local.Present {
		conflict.Local.Kind = domain.EntryKind(localKind)
	}
	conflict.Remote.Present = remotePresent != 0
	if conflict.Remote.Present {
		conflict.Remote.Kind = domain.EntryKind(remoteKind)
	}
	conflict.Resolved = resolved != 0
	conflict.CreatedAt = time.Unix(0, createdNS).UTC()
	if resolvedNS != 0 {
		conflict.ResolvedAt = time.Unix(0, resolvedNS).UTC()
	}
	return conflict, nil
}
