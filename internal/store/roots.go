package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"golang.org/x/text/unicode/norm"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

// SyncRootCreateReservation holds SQLite's writer reservation across the
// product-level marker setup. Close rolls the transaction back unless Commit
// has completed successfully.
type SyncRootCreateReservation struct {
	conn     *sql.Conn
	root     domain.SyncRoot
	finished bool
	muUnlock func()
}

// PrepareSyncRootCreate performs the authoritative ownership check under
// SQLite BEGIN IMMEDIATE. Callers may establish external prerequisites (the
// root marker) while this reservation is held, then Commit the durable row.
func (s *Store) PrepareSyncRootCreate(ctx context.Context, root domain.SyncRoot) (*SyncRootCreateReservation, error) {
	if err := validateNewSyncRoot(root); err != nil {
		return nil, err
	}
	s.syncRootMu.Lock()
	unlocked := false
	unlock := func() {
		if !unlocked {
			unlocked = true
			s.syncRootMu.Unlock()
		}
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		unlock()
		return nil, fmt.Errorf("acquire sync root write connection: %w", err)
	}
	fail := func(err error) (*SyncRootCreateReservation, error) {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		unlock()
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		_ = conn.Close()
		unlock()
		return nil, fmt.Errorf("begin sync root write transaction: %w", err)
	}
	roots, err := listSyncRoots(ctx, conn)
	if err != nil {
		return fail(err)
	}
	if err := checkSyncRootOwnershipAgainst(roots, root); err != nil {
		return fail(err)
	}
	if err := checkSyncRootAgainstFollowedClaims(ctx, conn, root); err != nil {
		return fail(err)
	}
	if root.CreatedAt.IsZero() {
		root.CreatedAt = s.now()
	} else {
		root.CreatedAt = root.CreatedAt.UTC()
	}
	return &SyncRootCreateReservation{conn: conn, root: root, muUnlock: unlock}, nil
}

// Commit inserts the prepared root and commits the SQLite transaction. If this
// returns an error, callers must treat the durable outcome as uncertain and
// preserve any external prerequisite already established.
func (r *SyncRootCreateReservation) Commit(ctx context.Context) (domain.SyncRoot, error) {
	if r == nil || r.conn == nil || r.finished {
		return domain.SyncRoot{}, fmt.Errorf("sync root create reservation is not active")
	}
	result, err := r.conn.ExecContext(ctx, `
INSERT INTO sync_roots(uuid, local_root, remote_name, remote_root, enabled, initialized, symlink_mode, poll_interval_seconds, created_at_ns)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.root.UUID, r.root.LocalRoot, r.root.RemoteName, r.root.RemoteRoot, boolInt(r.root.Enabled), boolInt(r.root.Initialized), r.root.EffectiveSymlinkMode(), r.root.PollIntervalSeconds, r.root.CreatedAt.UnixNano())
	if err != nil {
		return domain.SyncRoot{}, fmt.Errorf("insert sync root: %w", err)
	}
	r.root.ID, err = result.LastInsertId()
	if err != nil {
		return domain.SyncRoot{}, fmt.Errorf("read sync root ID: %w", err)
	}
	if _, err := r.conn.ExecContext(ctx, "COMMIT"); err != nil {
		return domain.SyncRoot{}, fmt.Errorf("commit sync root: %w", err)
	}
	root := r.root
	r.finish()
	return root, nil
}

func (r *SyncRootCreateReservation) Close() error {
	if r == nil || r.finished {
		return nil
	}
	_, rollbackErr := r.conn.ExecContext(context.Background(), "ROLLBACK")
	r.finish()
	if rollbackErr != nil {
		return fmt.Errorf("rollback sync root create reservation: %w", rollbackErr)
	}
	return nil
}

func (r *SyncRootCreateReservation) finish() {
	if r.finished {
		return
	}
	r.finished = true
	_ = r.conn.Close()
	if r.muUnlock != nil {
		r.muUnlock()
	}
}

func (s *Store) CreateSyncRoot(ctx context.Context, root domain.SyncRoot) (domain.SyncRoot, error) {
	reservation, err := s.PrepareSyncRootCreate(ctx, root)
	if err != nil {
		return domain.SyncRoot{}, err
	}
	defer func() { _ = reservation.Close() }()
	return reservation.Commit(ctx)
}

func validateNewSyncRoot(root domain.SyncRoot) error {
	if err := root.Validate(); err != nil {
		return err
	}
	if root.ID != 0 {
		return fmt.Errorf("new sync root must not already have an ID")
	}
	if root.Initialized {
		return fmt.Errorf("new sync root must start uninitialized")
	}
	return nil
}

func (s *Store) GetSyncRoot(ctx context.Context, id int64) (domain.SyncRoot, bool, error) {
	if id <= 0 {
		return domain.SyncRoot{}, false, fmt.Errorf("sync root ID must be positive")
	}
	row := s.db.QueryRowContext(ctx, `
SELECT id, uuid, local_root, remote_name, remote_root, enabled, initialized, symlink_mode, poll_interval_seconds, created_at_ns
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
	return listSyncRoots(ctx, s.db)
}

type rowsQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func listSyncRoots(ctx context.Context, q rowsQuerier) ([]domain.SyncRoot, error) {
	rows, err := q.QueryContext(ctx, `
SELECT id, uuid, local_root, remote_name, remote_root, enabled, initialized, symlink_mode, poll_interval_seconds, created_at_ns
FROM sync_roots ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list sync roots: %w", err)
	}
	defer func() { _ = rows.Close() }()

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

// SetSyncRootSymlinkMode changes local symlink policy only while a root is
// paused and has no pending operation created under the previous policy.
func (s *Store) SetSyncRootSymlinkMode(ctx context.Context, id int64, mode domain.SymlinkMode) error {
	if id <= 0 {
		return fmt.Errorf("sync root ID must be positive")
	}
	parsed, err := domain.ParseSymlinkMode(string(mode))
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `
UPDATE sync_roots
SET symlink_mode = ?
WHERE id = ?
  AND enabled = 0
  AND NOT EXISTS (SELECT 1 FROM operations WHERE operations.sync_root_id = sync_roots.id)`, parsed, id)
	if err != nil {
		return fmt.Errorf("update sync root symlink mode: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read updated sync root count: %w", err)
	}
	if rows == 1 {
		return nil
	}
	if _, ok, getErr := s.GetSyncRoot(ctx, id); getErr != nil {
		return getErr
	} else if !ok {
		return fmt.Errorf("sync root %d not found", id)
	}
	return fmt.Errorf("sync root %d must be paused and have no pending operations before changing symlink mode", id)
}

// DeleteSyncRoot removes one paused, idle root and lets SQLite cascade its
// baseline/conflict history. It never performs filesystem or remote mutations.
// The guarded DELETE is the final authority in case another process changes
// the root after a caller's earlier preflight.
func (s *Store) DeleteSyncRoot(ctx context.Context, id int64) error {
	if id <= 0 {
		return fmt.Errorf("sync root ID must be positive")
	}
	result, err := s.db.ExecContext(ctx, `
DELETE FROM sync_roots
WHERE id = ?
  AND enabled = 0
  AND NOT EXISTS (
      SELECT 1 FROM operations WHERE operations.sync_root_id = sync_roots.id
  )`, id)
	if err != nil {
		return fmt.Errorf("delete idle sync root: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read deleted sync root count: %w", err)
	}
	if rows == 1 {
		return nil
	}
	if _, ok, getErr := s.GetSyncRoot(ctx, id); getErr != nil {
		return getErr
	} else if !ok {
		return fmt.Errorf("sync root %d not found", id)
	}
	return fmt.Errorf("sync root %d must be paused and have no pending operations before removal", id)
}

// MarkSyncRootInitialized permanently closes the non-destructive initial-pairing
// phase for one root. Callers must do this only after a complete initial
// reconciliation has no unresolved conflicts/content checks and every external
// mutation has been observed and committed.
func (s *Store) MarkSyncRootInitialized(ctx context.Context, id int64) error {
	return s.InitializeSyncRoot(ctx, id, nil)
}

func checkSyncRootOwnershipAgainst(roots []domain.SyncRoot, candidate domain.SyncRoot) error {
	for _, existing := range roots {
		if localRootsOverlap(existing.LocalRoot, candidate.LocalRoot) {
			return fmt.Errorf("local sync root %q overlaps configured root %q", candidate.LocalRoot, existing.LocalRoot)
		}
		if remoteRootsOverlap(existing.RemoteRoot, candidate.RemoteRoot) {
			return fmt.Errorf("remote sync root %q:%q overlaps configured root %q:%q", candidate.RemoteName, candidate.RemoteRoot, existing.RemoteName, existing.RemoteRoot)
		}
	}
	return nil
}

func localRootsOverlap(a, b string) bool {
	return localRootsOverlapForOS(a, b, runtime.GOOS)
}

func localRootsOverlapForOS(a, b, targetOS string) bool {
	a = localRootComparisonPath(a, targetOS)
	b = localRootComparisonPath(b, targetOS)
	return localRootContains(a, b) || localRootContains(b, a)
}

func localRootComparisonPath(value, targetOS string) string {
	switch targetOS {
	case "windows":
		return strings.ToLower(value)
	case "darwin":
		return strings.ToLower(norm.NFC.String(value))
	default:
		return value
	}
}

func localRootContains(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil || filepath.IsAbs(rel) {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func physicalOwnershipPathsOverlap(aPath string, aKind domain.EntryKind, bPath string, bKind domain.EntryKind) bool {
	return physicalOwnershipPathsOverlapForOS(aPath, aKind, bPath, bKind, runtime.GOOS)
}

func physicalOwnershipPathsOverlapForOS(aPath string, aKind domain.EntryKind, bPath string, bKind domain.EntryKind, targetOS string) bool {
	if aPath == "" || bPath == "" {
		return false
	}
	aPath = localRootComparisonPath(aPath, targetOS)
	bPath = localRootComparisonPath(bPath, targetOS)
	if aPath == bPath {
		return true
	}
	if aKind == domain.KindDir && localRootContains(aPath, bPath) {
		return true
	}
	return bKind == domain.KindDir && localRootContains(bPath, aPath)
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
	var enabled, initialized int
	var createdNS int64
	if err := row.Scan(&root.ID, &root.UUID, &root.LocalRoot, &root.RemoteName, &root.RemoteRoot, &enabled, &initialized, &root.SymlinkMode, &root.PollIntervalSeconds, &createdNS); err != nil {
		return domain.SyncRoot{}, err
	}
	root.Enabled = enabled != 0
	root.Initialized = initialized != 0
	root.CreatedAt = time.Unix(0, createdNS).UTC()
	if err := root.Validate(); err != nil {
		return domain.SyncRoot{}, fmt.Errorf("invalid persisted sync root %d: %w", root.ID, err)
	}
	return root, nil
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
