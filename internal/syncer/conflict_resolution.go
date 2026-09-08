package syncer

import (
	"fmt"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

// ConflictResolution chooses which observed side should become authoritative.
// The resulting operation still revalidates both conflict fingerprints before
// mutating anything; this choice is never last-writer-wins.
type ConflictResolution string

const (
	ConflictKeepLocal  ConflictResolution = "keep-local"
	ConflictKeepRemote ConflictResolution = "keep-remote"
)

// OperationForConflictResolution converts one unresolved conflict snapshot into
// the ordinary durable operation model used by reconciliation and crash
// recovery. Directory-subtree and kind-mismatch replacement need multi-operation
// choreography and are intentionally rejected rather than implemented as unsafe
// single-entry shortcuts.
func OperationForConflictResolution(conflict domain.Conflict, resolution ConflictResolution) (domain.Operation, error) {
	if conflict.ID <= 0 {
		return domain.Operation{}, fmt.Errorf("conflict must have a durable ID")
	}
	if conflict.Resolved {
		return domain.Operation{}, fmt.Errorf("conflict %d is already resolved", conflict.ID)
	}
	if err := conflict.Validate(); err != nil {
		return domain.Operation{}, err
	}
	if conflict.Kind == domain.ConflictKindMismatch {
		return domain.Operation{}, fmt.Errorf("conflict %d is a kind mismatch; automatic keep-local/keep-remote replacement is not supported yet", conflict.ID)
	}
	if (conflict.Local.Present && conflict.Local.Kind == domain.KindDir) ||
		(conflict.Remote.Present && conflict.Remote.Kind == domain.KindDir) {
		return domain.Operation{}, fmt.Errorf("conflict %d involves a directory; automatic keep-local/keep-remote subtree resolution is not supported yet", conflict.ID)
	}

	remoteExpectation := expectationFromConflictRemote(conflict.Remote)
	operation := domain.Operation{
		SyncRootID:     conflict.SyncRootID,
		SrcPath:        conflict.RelPath,
		ExpectedLocal:  conflict.Local,
		ExpectedRemote: remoteExpectation,
		Phase:          domain.OperationPlanned,
	}

	switch resolution {
	case ConflictKeepLocal:
		if conflict.Local.Present {
			if conflict.Remote.Present && conflict.Local.Kind != conflict.Remote.Kind {
				return domain.Operation{}, fmt.Errorf("conflict %d has mismatched entry kinds", conflict.ID)
			}
			operation.Kind = domain.OperationEnsureRemote
			operation.EntryKind = conflict.Local.Kind
		} else if conflict.Remote.Present {
			operation.Kind = domain.OperationDeleteRemote
			operation.EntryKind = conflict.Remote.Kind
		} else {
			return domain.Operation{}, fmt.Errorf("conflict %d has neither side present", conflict.ID)
		}
	case ConflictKeepRemote:
		if conflict.Remote.Present {
			if conflict.Local.Present && conflict.Local.Kind != conflict.Remote.Kind {
				return domain.Operation{}, fmt.Errorf("conflict %d has mismatched entry kinds", conflict.ID)
			}
			operation.Kind = domain.OperationEnsureLocal
			operation.EntryKind = conflict.Remote.Kind
		} else if conflict.Local.Present {
			operation.Kind = domain.OperationDeleteLocal
			operation.EntryKind = conflict.Local.Kind
		} else {
			return domain.Operation{}, fmt.Errorf("conflict %d has neither side present", conflict.ID)
		}
	default:
		return domain.Operation{}, fmt.Errorf("unsupported conflict resolution %q", resolution)
	}

	if err := operation.Validate(); err != nil {
		return domain.Operation{}, fmt.Errorf("build conflict %d resolution operation: %w", conflict.ID, err)
	}
	return operation, nil
}

func expectationFromConflictRemote(remote domain.RemoteFingerprint) domain.RemoteExpectation {
	if !remote.Present {
		return domain.RemoteExpectation{Absent: true}
	}
	return domain.RemoteExpectation{ID: remote.ID, Rev: remote.Rev}
}
