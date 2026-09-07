package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

const upsertBaselineSQL = `
INSERT INTO entries(
    sync_root_id, rel_path, kind,
    baseline_local_present, baseline_local_size, baseline_local_mtime_ns,
    baseline_remote_present, baseline_remote_id, baseline_remote_rev,
    baseline_remote_size, baseline_remote_mtime_us
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(sync_root_id, rel_path) DO UPDATE SET
    kind = excluded.kind,
    baseline_local_present = excluded.baseline_local_present,
    baseline_local_size = excluded.baseline_local_size,
    baseline_local_mtime_ns = excluded.baseline_local_mtime_ns,
    baseline_remote_present = excluded.baseline_remote_present,
    baseline_remote_id = excluded.baseline_remote_id,
    baseline_remote_rev = excluded.baseline_remote_rev,
    baseline_remote_size = excluded.baseline_remote_size,
    baseline_remote_mtime_us = excluded.baseline_remote_mtime_us`

func (s *Store) PutBaseline(ctx context.Context, baseline domain.Baseline) error {
	if err := validatePersistedBaseline(baseline); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, upsertBaselineSQL, baselineArgs(baseline)...); err != nil {
		return fmt.Errorf("put baseline: %w", err)
	}
	return nil
}

func (s *Store) GetBaseline(ctx context.Context, syncRootID int64, relPath string) (domain.Baseline, bool, error) {
	if syncRootID <= 0 {
		return domain.Baseline{}, false, fmt.Errorf("sync root ID must be positive")
	}
	if err := domain.ValidateRelPath(relPath); err != nil {
		return domain.Baseline{}, false, err
	}
	row := s.db.QueryRowContext(ctx, `
SELECT sync_root_id, rel_path, kind,
       baseline_local_present, baseline_local_size, baseline_local_mtime_ns,
       baseline_remote_present, baseline_remote_id, baseline_remote_rev,
       baseline_remote_size, baseline_remote_mtime_us
FROM entries WHERE sync_root_id = ? AND rel_path = ?`, syncRootID, relPath)
	baseline, err := scanBaseline(row)
	if err == sql.ErrNoRows {
		return domain.Baseline{}, false, nil
	}
	if err != nil {
		return domain.Baseline{}, false, fmt.Errorf("get baseline: %w", err)
	}
	return baseline, true, nil
}

func (s *Store) ListBaselines(ctx context.Context, syncRootID int64) ([]domain.Baseline, error) {
	if syncRootID <= 0 {
		return nil, fmt.Errorf("sync root ID must be positive")
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT sync_root_id, rel_path, kind,
       baseline_local_present, baseline_local_size, baseline_local_mtime_ns,
       baseline_remote_present, baseline_remote_id, baseline_remote_rev,
       baseline_remote_size, baseline_remote_mtime_us
FROM entries WHERE sync_root_id = ? ORDER BY rel_path`, syncRootID)
	if err != nil {
		return nil, fmt.Errorf("list baselines: %w", err)
	}
	defer rows.Close()

	var baselines []domain.Baseline
	for rows.Next() {
		baseline, err := scanBaseline(rows)
		if err != nil {
			return nil, fmt.Errorf("scan baseline: %w", err)
		}
		baselines = append(baselines, baseline)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate baselines: %w", err)
	}
	return baselines, nil
}

func (s *Store) DeleteBaseline(ctx context.Context, syncRootID int64, relPath string) error {
	if syncRootID <= 0 {
		return fmt.Errorf("sync root ID must be positive")
	}
	if err := domain.ValidateRelPath(relPath); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM entries WHERE sync_root_id = ? AND rel_path = ?`, syncRootID, relPath); err != nil {
		return fmt.Errorf("delete baseline: %w", err)
	}
	return nil
}

// CommitBaselineAndDeleteOperation atomically records an observed successful
// postcondition and removes the corresponding durable external-side-effect
// intent. This is the normal completion transaction after Phase C executes an
// operation and observes its resulting local/remote state.
func (s *Store) CommitBaselineAndDeleteOperation(ctx context.Context, baseline domain.Baseline, operationID int64) error {
	if err := validatePersistedBaseline(baseline); err != nil {
		return err
	}
	if operationID <= 0 {
		return fmt.Errorf("operation ID must be positive")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin baseline completion transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, upsertBaselineSQL, baselineArgs(baseline)...); err != nil {
		return fmt.Errorf("commit observed baseline: %w", err)
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM operations WHERE id = ? AND sync_root_id = ?`, operationID, baseline.SyncRootID)
	if err != nil {
		return fmt.Errorf("delete completed operation: %w", err)
	}
	if err := requireOneRow(result, "completed operation"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit baseline completion transaction: %w", err)
	}
	return nil
}

// DropBaselineAndDeleteOperation atomically commits a synchronized deletion on
// both sides and removes the completed semantic operation intent.
func (s *Store) DropBaselineAndDeleteOperation(ctx context.Context, syncRootID int64, relPath string, operationID int64) error {
	if syncRootID <= 0 || operationID <= 0 {
		return fmt.Errorf("sync root ID and operation ID must be positive")
	}
	if err := domain.ValidateRelPath(relPath); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin baseline drop transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM entries WHERE sync_root_id = ? AND rel_path = ?`, syncRootID, relPath); err != nil {
		return fmt.Errorf("drop baseline: %w", err)
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM operations WHERE id = ? AND sync_root_id = ?`, operationID, syncRootID)
	if err != nil {
		return fmt.Errorf("delete completed operation: %w", err)
	}
	if err := requireOneRow(result, "completed operation"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit baseline drop transaction: %w", err)
	}
	return nil
}

func validatePersistedBaseline(baseline domain.Baseline) error {
	if err := baseline.Validate(); err != nil {
		return err
	}
	if !baseline.Local.Present && !baseline.Remote.Present {
		return fmt.Errorf("fully absent baseline must be deleted instead of persisted")
	}
	return nil
}

func baselineArgs(baseline domain.Baseline) []any {
	kind := baseline.Remote.Kind
	if baseline.Local.Present {
		kind = baseline.Local.Kind
	}
	return []any{
		baseline.SyncRootID,
		baseline.RelPath,
		string(kind),
		boolInt(baseline.Local.Present),
		baseline.Local.Size,
		baseline.Local.MtimeNS,
		boolInt(baseline.Remote.Present),
		baseline.Remote.ID,
		baseline.Remote.Rev,
		baseline.Remote.Size,
		baseline.Remote.MtimeUS,
	}
}

func scanBaseline(row rowScanner) (domain.Baseline, error) {
	var baseline domain.Baseline
	var kind string
	var localPresent, remotePresent int
	if err := row.Scan(
		&baseline.SyncRootID,
		&baseline.RelPath,
		&kind,
		&localPresent,
		&baseline.Local.Size,
		&baseline.Local.MtimeNS,
		&remotePresent,
		&baseline.Remote.ID,
		&baseline.Remote.Rev,
		&baseline.Remote.Size,
		&baseline.Remote.MtimeUS,
	); err != nil {
		return domain.Baseline{}, err
	}
	entryKind := domain.EntryKind(kind)
	baseline.Local.Present = localPresent != 0
	if baseline.Local.Present {
		baseline.Local.Kind = entryKind
	}
	baseline.Remote.Present = remotePresent != 0
	if baseline.Remote.Present {
		baseline.Remote.Kind = entryKind
	}
	return baseline, nil
}

func requireOneRow(result sql.Result, what string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read affected rows for %s: %w", what, err)
	}
	if rows != 1 {
		return fmt.Errorf("expected exactly one %s row, affected %d", what, rows)
	}
	return nil
}
