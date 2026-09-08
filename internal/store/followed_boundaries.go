package store

import (
	"context"
	"fmt"
	"sort"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

// ListFollowedDirectoryBoundaries returns the durable physical identities that
// were accepted when an initialized root last completed initial pairing.
func (s *Store) ListFollowedDirectoryBoundaries(ctx context.Context, rootID int64) (map[string]string, error) {
	if rootID <= 0 {
		return nil, fmt.Errorf("sync root ID must be positive")
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT rel_path, physical_identity
FROM followed_directory_boundaries
WHERE sync_root_id = ?
ORDER BY rel_path`, rootID)
	if err != nil {
		return nil, fmt.Errorf("list followed directory boundaries: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]string)
	for rows.Next() {
		var relPath, identity string
		if err := rows.Scan(&relPath, &identity); err != nil {
			return nil, fmt.Errorf("scan followed directory boundary: %w", err)
		}
		if err := domain.ValidateRelPath(relPath); err != nil {
			return nil, fmt.Errorf("invalid persisted followed directory boundary %q: %w", relPath, err)
		}
		if identity == "" {
			return nil, fmt.Errorf("followed directory boundary %q has empty physical identity", relPath)
		}
		out[relPath] = identity
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate followed directory boundaries: %w", err)
	}
	return out, nil
}

// InitializeSyncRoot atomically records the followed-directory identity fence
// and closes initial pairing. Once initialized, later identity changes require a
// deliberate root re-pair rather than being learned automatically.
func (s *Store) InitializeSyncRoot(ctx context.Context, rootID int64, boundaries map[string]string) error {
	if rootID <= 0 {
		return fmt.Errorf("sync root ID must be positive")
	}
	paths := make([]string, 0, len(boundaries))
	for relPath, identity := range boundaries {
		if err := domain.ValidateRelPath(relPath); err != nil {
			return fmt.Errorf("followed directory boundary path: %w", err)
		}
		if identity == "" {
			return fmt.Errorf("followed directory boundary %q has empty physical identity", relPath)
		}
		paths = append(paths, relPath)
	}
	sort.Strings(paths)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin sync root initialization: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var initialized int
	if err := tx.QueryRowContext(ctx, `SELECT initialized FROM sync_roots WHERE id = ?`, rootID).Scan(&initialized); err != nil {
		return fmt.Errorf("read sync root initialization state: %w", err)
	}
	if initialized != 0 {
		return fmt.Errorf("sync root %d is already initialized", rootID)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM followed_directory_boundaries WHERE sync_root_id = ?`, rootID); err != nil {
		return fmt.Errorf("clear pre-initialization followed directory boundaries: %w", err)
	}
	for _, relPath := range paths {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO followed_directory_boundaries(sync_root_id, rel_path, physical_identity)
VALUES(?, ?, ?)`, rootID, relPath, boundaries[relPath]); err != nil {
			return fmt.Errorf("persist followed directory boundary %q: %w", relPath, err)
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE sync_roots SET initialized = 1 WHERE id = ? AND initialized = 0`, rootID)
	if err != nil {
		return fmt.Errorf("mark sync root initialized: %w", err)
	}
	if err := requireOneRow(result, "sync root"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit sync root initialization: %w", err)
	}
	return nil
}
