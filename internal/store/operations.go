package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

func (s *Store) CreateOperation(ctx context.Context, operation domain.Operation) (domain.Operation, error) {
	if operation.ID != 0 {
		return domain.Operation{}, fmt.Errorf("new operation must not already have an ID")
	}
	if operation.Phase == "" {
		operation.Phase = domain.OperationPlanned
	}
	now := s.now()
	if operation.CreatedAt.IsZero() {
		operation.CreatedAt = now
	} else {
		operation.CreatedAt = operation.CreatedAt.UTC()
	}
	if operation.UpdatedAt.IsZero() {
		operation.UpdatedAt = operation.CreatedAt
	} else {
		operation.UpdatedAt = operation.UpdatedAt.UTC()
	}
	if err := operation.Validate(); err != nil {
		return domain.Operation{}, err
	}

	result, err := s.db.ExecContext(ctx, `
INSERT INTO operations(
    sync_root_id, kind, entry_kind, src_path, dst_path,
    expected_local_present, expected_local_kind, expected_local_size, expected_local_mtime_ns,
    expected_remote_absent, expected_remote_id, expected_remote_rev,
    phase, attempts, last_error, created_at_ns, updated_at_ns
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		operation.SyncRootID,
		string(operation.Kind),
		string(operation.EntryKind),
		operation.SrcPath,
		operation.DstPath,
		boolInt(operation.ExpectedLocal.Present),
		string(operation.ExpectedLocal.Kind),
		operation.ExpectedLocal.Size,
		operation.ExpectedLocal.MtimeNS,
		boolInt(operation.ExpectedRemote.Absent),
		operation.ExpectedRemote.ID,
		operation.ExpectedRemote.Rev,
		string(operation.Phase),
		operation.Attempts,
		operation.LastError,
		operation.CreatedAt.UnixNano(),
		operation.UpdatedAt.UnixNano(),
	)
	if err != nil {
		return domain.Operation{}, fmt.Errorf("insert operation: %w", err)
	}
	operation.ID, err = result.LastInsertId()
	if err != nil {
		return domain.Operation{}, fmt.Errorf("read operation ID: %w", err)
	}
	return operation, nil
}

func (s *Store) GetOperation(ctx context.Context, id int64) (domain.Operation, bool, error) {
	if id <= 0 {
		return domain.Operation{}, false, fmt.Errorf("operation ID must be positive")
	}
	row := s.db.QueryRowContext(ctx, operationSelect+` WHERE id = ?`, id)
	operation, err := scanOperation(row)
	if err == sql.ErrNoRows {
		return domain.Operation{}, false, nil
	}
	if err != nil {
		return domain.Operation{}, false, fmt.Errorf("get operation: %w", err)
	}
	return operation, true, nil
}

func (s *Store) ListOperations(ctx context.Context, syncRootID int64) ([]domain.Operation, error) {
	if syncRootID <= 0 {
		return nil, fmt.Errorf("sync root ID must be positive")
	}
	rows, err := s.db.QueryContext(ctx, operationSelect+` WHERE sync_root_id = ? ORDER BY id`, syncRootID)
	if err != nil {
		return nil, fmt.Errorf("list operations: %w", err)
	}
	defer rows.Close()

	var operations []domain.Operation
	for rows.Next() {
		operation, err := scanOperation(rows)
		if err != nil {
			return nil, fmt.Errorf("scan operation: %w", err)
		}
		operations = append(operations, operation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate operations: %w", err)
	}
	return operations, nil
}

// DeleteOperation removes an intent only when the caller has proved no external
// mutation started (for example, a planned intent whose preconditions are now
// stale and must be replanned). Running/recovering intents must never be dropped
// through this helper.
func (s *Store) DeleteOperation(ctx context.Context, id int64) error {
	if id <= 0 {
		return fmt.Errorf("operation ID must be positive")
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM operations WHERE id = ? AND phase = ?`, id, string(domain.OperationPlanned))
	if err != nil {
		return fmt.Errorf("delete planned operation: %w", err)
	}
	return requireOneRow(result, "planned operation")
}

// SetOperationPhase updates recovery state. incrementAttempts should be true
// only when an actual external mutation attempt is about to start.
func (s *Store) SetOperationPhase(ctx context.Context, id int64, phase domain.OperationPhase, lastError string, incrementAttempts bool) error {
	if id <= 0 {
		return fmt.Errorf("operation ID must be positive")
	}
	if !validOperationPhase(phase) {
		return fmt.Errorf("invalid operation phase %q", phase)
	}
	increment := 0
	if incrementAttempts {
		increment = 1
	}
	result, err := s.db.ExecContext(ctx, `
UPDATE operations
SET phase = ?, last_error = ?, attempts = attempts + ?, updated_at_ns = ?
WHERE id = ?`, string(phase), lastError, increment, s.now().UnixNano(), id)
	if err != nil {
		return fmt.Errorf("update operation phase: %w", err)
	}
	return requireOneRow(result, "operation")
}

const operationSelect = `
SELECT id, sync_root_id, kind, entry_kind, src_path, dst_path,
       expected_local_present, expected_local_kind, expected_local_size, expected_local_mtime_ns,
       expected_remote_absent, expected_remote_id, expected_remote_rev,
       phase, attempts, last_error, created_at_ns, updated_at_ns
FROM operations`

func scanOperation(row rowScanner) (domain.Operation, error) {
	var operation domain.Operation
	var kind, entryKind, phase, expectedLocalKind string
	var localPresent, remoteAbsent int
	var createdNS, updatedNS int64
	if err := row.Scan(
		&operation.ID,
		&operation.SyncRootID,
		&kind,
		&entryKind,
		&operation.SrcPath,
		&operation.DstPath,
		&localPresent,
		&expectedLocalKind,
		&operation.ExpectedLocal.Size,
		&operation.ExpectedLocal.MtimeNS,
		&remoteAbsent,
		&operation.ExpectedRemote.ID,
		&operation.ExpectedRemote.Rev,
		&phase,
		&operation.Attempts,
		&operation.LastError,
		&createdNS,
		&updatedNS,
	); err != nil {
		return domain.Operation{}, err
	}
	operation.Kind = domain.OperationKind(kind)
	operation.EntryKind = domain.EntryKind(entryKind)
	operation.ExpectedLocal.Present = localPresent != 0
	if operation.ExpectedLocal.Present {
		operation.ExpectedLocal.Kind = domain.EntryKind(expectedLocalKind)
	}
	operation.ExpectedRemote.Absent = remoteAbsent != 0
	operation.Phase = domain.OperationPhase(phase)
	operation.CreatedAt = time.Unix(0, createdNS).UTC()
	operation.UpdatedAt = time.Unix(0, updatedNS).UTC()
	return operation, nil
}

func validOperationPhase(phase domain.OperationPhase) bool {
	switch phase {
	case domain.OperationPlanned, domain.OperationRunning, domain.OperationRecovering, domain.OperationBlocked:
		return true
	default:
		return false
	}
}
