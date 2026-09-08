package syncer

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"strings"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
	"github.com/rijuyuezhu/pkudisk-sync/internal/reconcile"
	"github.com/rijuyuezhu/pkudisk-sync/internal/rootmarker"
	"github.com/rijuyuezhu/pkudisk-sync/internal/store"
)

const maxCyclePasses = 8

// DataPlane is the narrow in-process execution surface needed by one complete
// reconciliation cycle. executor.RootExecutor implements this interface.
type DataPlane interface {
	ScanLocal(context.Context) (map[string]domain.LocalFingerprint, []string, error)
	ScanRemote(context.Context) (map[string]domain.RemoteFingerprint, bool, error)
	ObserveLocalEntry(context.Context, string) (domain.LocalFingerprint, error)
	ObserveRemoteEntry(context.Context, string) (domain.RemoteFingerprint, error)
	ResolveLocalMutationTarget(context.Context, string, domain.LocalFingerprint) (string, error)
	LocalRecoveryArtifact(context.Context, domain.Operation) (string, bool, error)
	CompareFileContent(context.Context, string, domain.LocalFingerprint, domain.RemoteExpectation) (bool, error)
	Upload(context.Context, string, domain.LocalFingerprint, domain.RemoteExpectation) (domain.RemoteFingerprint, error)
	EnsureRemoteDir(context.Context, string, domain.RemoteExpectation) error
	EnsureLocalFile(context.Context, domain.Operation) (domain.LocalFingerprint, error)
	EnsureLocalDir(context.Context, domain.Operation) error
	DeleteRemoteFile(context.Context, domain.RemoteExpectation) error
	DeleteRemoteDir(context.Context, domain.RemoteExpectation) error
	DeleteLocal(context.Context, domain.Operation) error
}

// CycleResult summarizes one selected-root cycle without making caller-visible
// policy decisions depend on log text.
type CycleResult struct {
	Skipped       bool
	Initial       bool
	Initialized   bool
	Blocked       bool
	BlockReason   string
	BlockDetail   string
	Passes        int
	Applied       int
	ContentChecks int
	Conflicts     int
	Recovered     int
}

// RunRootCycle performs one durable, crash-recoverable synchronization cycle
// for a selected root. External mutations are always journaled before execution,
// and a running/recovering intent is never blindly replayed.
func RunRootCycle(ctx context.Context, rootID int64, state *store.Store, data DataPlane, deletePolicy reconcile.DeletePolicy) (CycleResult, error) {
	return runRootCycle(ctx, rootID, state, data, deletePolicy, runtime.GOOS)
}

func runRootCycle(ctx context.Context, rootID int64, state *store.Store, data DataPlane, deletePolicy reconcile.DeletePolicy, targetOS string) (CycleResult, error) {
	if state == nil {
		return CycleResult{}, fmt.Errorf("state store must not be nil")
	}
	if data == nil {
		return CycleResult{}, fmt.Errorf("data plane must not be nil")
	}
	root, ok, err := state.GetSyncRoot(ctx, rootID)
	if err != nil {
		return CycleResult{}, err
	}
	if !ok {
		return CycleResult{}, fmt.Errorf("sync root %d not found", rootID)
	}
	result := CycleResult{Initial: !root.Initialized, Initialized: root.Initialized}
	if !root.Enabled {
		result.Skipped = true
		return result, nil
	}
	if err := rootmarker.Check(root.LocalRoot, root.UUID); err != nil {
		result.Blocked = true
		result.BlockReason = string(reconcile.BlockRootUnhealthy)
		result.BlockDetail = err.Error()
		return result, nil
	}

	operations, err := state.ListOperations(ctx, root.ID)
	if err != nil {
		return result, err
	}
	if len(operations) > 0 {
		preflight, _, err := scanCompleteSnapshot(ctx, data, root.Initialized)
		if err != nil {
			return result, fmt.Errorf("operation recovery namespace preflight: %w", err)
		}
		if err := validateSnapshotNamespace(preflight.Local, preflight.Remote, targetOS); err != nil {
			result.Blocked = true
			result.BlockReason = string(reconcile.BlockNamespaceUnsafe)
			result.BlockDetail = err.Error()
			return result, nil
		}
	}

	blocked, recovered, err := recoverExistingOperations(ctx, state, data, operations)
	result.Recovered += recovered
	if err != nil {
		return result, err
	}
	if blocked {
		result.Blocked = true
		result.BlockReason = "operation-recovery-blocked"
		return result, nil
	}

	for pass := 1; pass <= maxCyclePasses; pass++ {
		result.Passes = pass
		root, ok, err = state.GetSyncRoot(ctx, rootID)
		if err != nil {
			return result, err
		}
		if !ok {
			return result, fmt.Errorf("sync root %d disappeared during cycle", rootID)
		}
		if !root.Enabled {
			result.Skipped = true
			return result, nil
		}
		if err := rootmarker.Check(root.LocalRoot, root.UUID); err != nil {
			result.Blocked = true
			result.BlockReason = string(reconcile.BlockRootUnhealthy)
			result.BlockDetail = err.Error()
			return result, nil
		}

		snapshot, remoteRootPresent, err := scanCompleteSnapshot(ctx, data, root.Initialized)
		if err != nil {
			return result, err
		}
		if err := validateSnapshotNamespace(snapshot.Local, snapshot.Remote, targetOS); err != nil {
			result.Blocked = true
			result.BlockReason = string(reconcile.BlockNamespaceUnsafe)
			result.BlockDetail = err.Error()
			return result, nil
		}
		baselines, err := state.ListBaselines(ctx, root.ID)
		if err != nil {
			return result, err
		}
		// A brand-new empty local root has no entry mutation that could create a
		// missing remote boundary. Keep it dormant and uninitialized instead of
		// recording a false "both empty" baseline state. The watcher will retry
		// as soon as local content appears; an externally created remote root also
		// lets a later cycle initialize normally.
		if !root.Initialized && !remoteRootPresent && len(snapshot.Local) == 0 && len(baselines) == 0 {
			result.Skipped = true
			return result, nil
		}
		plan, err := reconcile.PlanFullSnapshot(root.ID, !root.Initialized, baselines, snapshot, deletePolicy)
		if err != nil {
			return result, err
		}
		if plan.Blocked {
			result.Blocked = true
			result.BlockReason = string(plan.BlockReason)
			return result, nil
		}

		for plan.ContentChecks > 0 {
			for _, decision := range plan.Decisions {
				if decision.Kind != domain.DecisionCompareContent {
					continue
				}
				equal, err := data.CompareFileContent(ctx, decision.RelPath, decision.ExpectedLocal, decision.ExpectedRemote)
				if err != nil {
					return result, fmt.Errorf("compare content %q: %w", decision.RelPath, err)
				}
				if snapshot.Content == nil {
					snapshot.Content = make(map[string]domain.ContentEvidence)
				}
				if equal {
					snapshot.Content[decision.RelPath] = domain.ContentEqual
				} else {
					snapshot.Content[decision.RelPath] = domain.ContentDifferent
				}
				result.ContentChecks++
			}
			plan, err = reconcile.PlanFullSnapshot(root.ID, !root.Initialized, baselines, snapshot, deletePolicy)
			if err != nil {
				return result, err
			}
			if plan.Blocked {
				result.Blocked = true
				result.BlockReason = string(plan.BlockReason)
				return result, nil
			}
		}

		if err := reconcileConflictRecords(ctx, root.ID, state, snapshot, plan.Decisions); err != nil {
			return result, err
		}
		result.Conflicts = plan.Conflicts

		for _, decision := range plan.Decisions {
			switch decision.Kind {
			case domain.DecisionCommitBaseline:
				if err := state.PutBaseline(ctx, baselineFromSnapshot(root.ID, decision.RelPath, snapshot)); err != nil {
					return result, fmt.Errorf("commit baseline %q: %w", decision.RelPath, err)
				}
			case domain.DecisionDropBaseline:
				if err := state.DeleteBaseline(ctx, root.ID, decision.RelPath); err != nil {
					return result, fmt.Errorf("drop baseline %q: %w", decision.RelPath, err)
				}
			}
		}

		external := externalDecisions(plan.Decisions)
		if len(external) == 0 {
			unresolved, err := state.ListConflicts(ctx, root.ID, true)
			if err != nil {
				return result, err
			}
			if !root.Initialized && len(unresolved) == 0 && plan.Conflicts == 0 {
				if err := state.MarkSyncRootInitialized(ctx, root.ID); err != nil {
					return result, err
				}
				result.Initialized = true
			}
			return result, nil
		}

		sortExternalDecisions(external)
		for _, decision := range external {
			op, err := state.CreateOperation(ctx, operationFromDecision(root.ID, decision))
			if err != nil {
				return result, fmt.Errorf("journal operation %q: %w", decision.RelPath, err)
			}
			op, err = pinLocalMutationTarget(ctx, state, data, op)
			if err != nil {
				return result, fmt.Errorf("pin local mutation target %q: %w", decision.RelPath, err)
			}
			if err := executePersistedOperation(ctx, state, data, op); err != nil {
				return result, err
			}
			result.Applied++
		}
	}
	return result, fmt.Errorf("sync root %d did not stabilize after %d passes", rootID, maxCyclePasses)
}

func scanCompleteSnapshot(ctx context.Context, data DataPlane, requireRemoteRoot bool) (reconcile.Snapshot, bool, error) {
	local, excluded, err := data.ScanLocal(ctx)
	if err != nil {
		return reconcile.Snapshot{}, false, fmt.Errorf("complete local scan: %w", err)
	}
	remote, remoteRootPresent, err := data.ScanRemote(ctx)
	if err != nil {
		return reconcile.Snapshot{}, remoteRootPresent, fmt.Errorf("complete remote scan: %w", err)
	}
	if requireRemoteRoot && !remoteRootPresent {
		return reconcile.Snapshot{}, false, fmt.Errorf("complete remote scan: selected remote root is missing after initialization")
	}
	for relPath := range remote {
		if reconcile.PathExcluded(excluded, relPath) {
			delete(remote, relPath)
		}
	}
	return reconcile.Snapshot{
		Local:          local,
		Remote:         remote,
		Content:        make(map[string]domain.ContentEvidence),
		Excluded:       excluded,
		LocalComplete:  true,
		RemoteComplete: true,
		RootHealthy:    true,
	}, remoteRootPresent, nil
}

func isLocalMutation(op domain.Operation) bool {
	return op.Kind == domain.OperationEnsureLocal || op.Kind == domain.OperationDeleteLocal
}

func pinLocalMutationTarget(ctx context.Context, state *store.Store, data DataPlane, op domain.Operation) (domain.Operation, error) {
	if !isLocalMutation(op) || op.LocalTargetPath != "" {
		return op, nil
	}
	if op.Phase != domain.OperationPlanned || op.Attempts != 0 {
		return op, fmt.Errorf("operation %d has no pinned local target after mutation attempts started", op.ID)
	}
	target, err := data.ResolveLocalMutationTarget(ctx, op.SrcPath, op.ExpectedLocal)
	if err != nil {
		return op, err
	}
	if err := state.SetOperationLocalTarget(ctx, op.ID, target); err != nil {
		return op, err
	}
	op.LocalTargetPath = target
	return op, nil
}

func recoverExistingOperations(ctx context.Context, state *store.Store, data DataPlane, operations []domain.Operation) (blocked bool, recovered int, err error) {
	for _, op := range operations {
		switch op.Phase {
		case domain.OperationBlocked:
			return true, recovered, nil
		case domain.OperationPlanned:
			op, err = pinLocalMutationTarget(ctx, state, data, op)
			if err != nil {
				return false, recovered, err
			}
			holds, err := operationPreconditionsHold(ctx, data, op)
			if err != nil {
				return false, recovered, err
			}
			if !holds {
				if err := state.DeleteOperation(ctx, op.ID); err != nil {
					return false, recovered, err
				}
				continue
			}
			if err := executePersistedOperation(ctx, state, data, op); err != nil {
				return false, recovered, err
			}
			recovered++
		case domain.OperationRunning, domain.OperationRecovering:
			if op.Phase == domain.OperationRunning {
				if err := state.SetOperationPhase(ctx, op.ID, domain.OperationRecovering, "recovering unknown prior outcome", false); err != nil {
					return false, recovered, err
				}
			}
			if isLocalMutation(op) {
				if op.LocalTargetPath == "" {
					if err := state.SetOperationPhase(ctx, op.ID, domain.OperationBlocked, "local mutation started before a physical target was pinned", false); err != nil {
						return false, recovered, err
					}
					return true, recovered, nil
				}
				recoveryPath, pending, err := data.LocalRecoveryArtifact(ctx, op)
				if err != nil {
					return false, recovered, err
				}
				if pending {
					detail := fmt.Sprintf("local data preserved at recovery artifact %q; automatic replay is unsafe", recoveryPath)
					if err := state.SetOperationPhase(ctx, op.ID, domain.OperationBlocked, detail, false); err != nil {
						return false, recovered, err
					}
					return true, recovered, nil
				}
			}
			satisfied, local, remote, err := proveRecoveredPostcondition(ctx, data, op)
			if err != nil {
				_ = state.SetOperationPhase(ctx, op.ID, domain.OperationRecovering, err.Error(), false)
				return false, recovered, err
			}
			if satisfied {
				if err := commitCompletedOperation(ctx, state, op, local, remote); err != nil {
					return false, recovered, err
				}
				recovered++
				continue
			}
			holds, err := operationPreconditionsHold(ctx, data, op)
			if err != nil {
				return false, recovered, err
			}
			if !holds {
				if err := state.SetOperationPhase(ctx, op.ID, domain.OperationBlocked, "cannot prove prior completion or safe retry", false); err != nil {
					return false, recovered, err
				}
				return true, recovered, nil
			}
			if err := executePersistedOperation(ctx, state, data, op); err != nil {
				return false, recovered, err
			}
			recovered++
		default:
			return false, recovered, fmt.Errorf("operation %d has unsupported phase %q", op.ID, op.Phase)
		}
	}
	return false, recovered, nil
}

func executePersistedOperation(ctx context.Context, state *store.Store, data DataPlane, op domain.Operation) error {
	if op.Kind == domain.OperationMoveRemote || op.Kind == domain.OperationMoveLocal {
		if err := state.SetOperationPhase(ctx, op.ID, domain.OperationBlocked, "move operations are not emitted by the current path planner", false); err != nil {
			return err
		}
		return fmt.Errorf("operation %d uses unsupported move recovery", op.ID)
	}
	if err := state.SetOperationPhase(ctx, op.ID, domain.OperationRunning, "", true); err != nil {
		return err
	}

	var (
		err                    error
		exactUploadResult      domain.RemoteFingerprint
		hasExactUploadResult   bool
		exactDownloadResult    domain.LocalFingerprint
		hasExactDownloadResult bool
	)
	switch op.Kind {
	case domain.OperationEnsureRemote:
		if op.EntryKind == domain.KindDir {
			err = data.EnsureRemoteDir(ctx, op.SrcPath, op.ExpectedRemote)
		} else {
			exactUploadResult, err = data.Upload(ctx, op.SrcPath, op.ExpectedLocal, op.ExpectedRemote)
			hasExactUploadResult = err == nil
		}
	case domain.OperationEnsureLocal:
		if op.EntryKind == domain.KindDir {
			err = data.EnsureLocalDir(ctx, op)
		} else {
			exactDownloadResult, err = data.EnsureLocalFile(ctx, op)
			hasExactDownloadResult = err == nil
		}
	case domain.OperationDeleteRemote:
		if op.EntryKind == domain.KindDir {
			err = data.DeleteRemoteDir(ctx, op.ExpectedRemote)
		} else {
			err = data.DeleteRemoteFile(ctx, op.ExpectedRemote)
		}
	case domain.OperationDeleteLocal:
		err = data.DeleteLocal(ctx, op)
	default:
		err = fmt.Errorf("unsupported operation kind %q", op.Kind)
	}
	if err != nil {
		_ = state.SetOperationPhase(ctx, op.ID, domain.OperationRecovering, err.Error(), false)
		return fmt.Errorf("execute operation %d (%s %q): %w", op.ID, op.Kind, op.SrcPath, err)
	}

	local, err := data.ObserveLocalEntry(ctx, op.SrcPath)
	if err != nil {
		_ = state.SetOperationPhase(ctx, op.ID, domain.OperationRecovering, err.Error(), false)
		return err
	}
	remote, err := data.ObserveRemoteEntry(ctx, op.SrcPath)
	if err != nil {
		_ = state.SetOperationPhase(ctx, op.ID, domain.OperationRecovering, err.Error(), false)
		return err
	}
	if hasExactUploadResult && !domain.RemoteEquivalent(remote, exactUploadResult) {
		err := fmt.Errorf("operation %d upload result changed before baseline observation", op.ID)
		_ = state.SetOperationPhase(ctx, op.ID, domain.OperationRecovering, err.Error(), false)
		return err
	}
	if hasExactDownloadResult && !domain.LocalEquivalent(local, exactDownloadResult) {
		err := fmt.Errorf("operation %d download result changed before baseline observation", op.ID)
		_ = state.SetOperationPhase(ctx, op.ID, domain.OperationRecovering, err.Error(), false)
		return err
	}
	if !postconditionAfterSuccessfulCall(op, local, remote) {
		err := fmt.Errorf("operation %d returned success but observed postcondition is not satisfied", op.ID)
		_ = state.SetOperationPhase(ctx, op.ID, domain.OperationRecovering, err.Error(), false)
		return err
	}
	return commitCompletedOperation(ctx, state, op, local, remote)
}

func operationPreconditionsHold(ctx context.Context, data DataPlane, op domain.Operation) (bool, error) {
	local, err := data.ObserveLocalEntry(ctx, op.SrcPath)
	if err != nil {
		return false, err
	}
	remote, err := data.ObserveRemoteEntry(ctx, op.SrcPath)
	if err != nil {
		return false, err
	}
	return domain.LocalEquivalent(local, op.ExpectedLocal) && remoteMatchesExpectation(remote, op.ExpectedRemote, op.EntryKind), nil
}

func proveRecoveredPostcondition(ctx context.Context, data DataPlane, op domain.Operation) (bool, domain.LocalFingerprint, domain.RemoteFingerprint, error) {
	local, err := data.ObserveLocalEntry(ctx, op.SrcPath)
	if err != nil {
		return false, domain.LocalFingerprint{}, domain.RemoteFingerprint{}, err
	}
	remote, err := data.ObserveRemoteEntry(ctx, op.SrcPath)
	if err != nil {
		return false, domain.LocalFingerprint{}, domain.RemoteFingerprint{}, err
	}

	switch op.Kind {
	case domain.OperationDeleteRemote, domain.OperationDeleteLocal:
		return !local.Present && !remote.Present, local, remote, nil
	case domain.OperationEnsureRemote:
		if op.EntryKind == domain.KindDir {
			satisfied := domain.LocalEquivalent(local, op.ExpectedLocal) && local.Present && remote.Present && remote.Kind == domain.KindDir
			return satisfied, local, remote, nil
		}
		if !domain.LocalEquivalent(local, op.ExpectedLocal) || !local.Present || !remote.Present || remote.Kind != domain.KindFile || local.Size != remote.Size {
			return false, local, remote, nil
		}
		if !op.ExpectedRemote.Absent && remote.ID == op.ExpectedRemote.ID && remote.Rev == op.ExpectedRemote.Rev {
			return false, local, remote, nil
		}
		equal, err := data.CompareFileContent(ctx, op.SrcPath, local, expectationFromRemote(remote))
		if err != nil {
			return false, local, remote, err
		}
		if !equal {
			return false, local, remote, nil
		}
		local2, remote2, err := observeStablePair(ctx, data, op.SrcPath, local, remote)
		return err == nil, local2, remote2, err
	case domain.OperationEnsureLocal:
		if !remoteMatchesExpectation(remote, op.ExpectedRemote, op.EntryKind) {
			return false, local, remote, nil
		}
		if op.EntryKind == domain.KindDir {
			return local.Present && local.Kind == domain.KindDir, local, remote, nil
		}
		if !local.Present || local.Kind != domain.KindFile || local.Size != remote.Size {
			return false, local, remote, nil
		}
		equal, err := data.CompareFileContent(ctx, op.SrcPath, local, op.ExpectedRemote)
		if err != nil {
			return false, local, remote, err
		}
		if !equal {
			return false, local, remote, nil
		}
		local2, remote2, err := observeStablePair(ctx, data, op.SrcPath, local, remote)
		return err == nil, local2, remote2, err
	default:
		return false, local, remote, nil
	}
}

func observeStablePair(ctx context.Context, data DataPlane, relPath string, beforeLocal domain.LocalFingerprint, beforeRemote domain.RemoteFingerprint) (domain.LocalFingerprint, domain.RemoteFingerprint, error) {
	local, err := data.ObserveLocalEntry(ctx, relPath)
	if err != nil {
		return domain.LocalFingerprint{}, domain.RemoteFingerprint{}, err
	}
	remote, err := data.ObserveRemoteEntry(ctx, relPath)
	if err != nil {
		return domain.LocalFingerprint{}, domain.RemoteFingerprint{}, err
	}
	if !domain.LocalEquivalent(local, beforeLocal) || !domain.RemoteEquivalent(remote, beforeRemote) {
		return local, remote, fmt.Errorf("entry %q changed while proving operation recovery", relPath)
	}
	return local, remote, nil
}

func postconditionAfterSuccessfulCall(op domain.Operation, local domain.LocalFingerprint, remote domain.RemoteFingerprint) bool {
	switch op.Kind {
	case domain.OperationDeleteRemote, domain.OperationDeleteLocal:
		return !local.Present && !remote.Present
	case domain.OperationEnsureRemote:
		if !domain.LocalEquivalent(local, op.ExpectedLocal) || !local.Present || !remote.Present || local.Kind != op.EntryKind || remote.Kind != op.EntryKind {
			return false
		}
		if op.EntryKind == domain.KindFile {
			if local.Size != remote.Size {
				return false
			}
			if op.ExpectedRemote.Absent {
				return true
			}
			return remote.ID == op.ExpectedRemote.ID && remote.Rev != op.ExpectedRemote.Rev
		}
		return true
	case domain.OperationEnsureLocal:
		if !remoteMatchesExpectation(remote, op.ExpectedRemote, op.EntryKind) || !local.Present || local.Kind != op.EntryKind {
			return false
		}
		if op.EntryKind == domain.KindFile {
			return local.Size == remote.Size
		}
		return true
	default:
		return false
	}
}

func commitCompletedOperation(ctx context.Context, state *store.Store, op domain.Operation, local domain.LocalFingerprint, remote domain.RemoteFingerprint) error {
	if !local.Present && !remote.Present {
		return state.DropBaselineAndDeleteOperation(ctx, op.SyncRootID, op.SrcPath, op.ID)
	}
	if !local.Present || !remote.Present || local.Kind != remote.Kind {
		return fmt.Errorf("operation %d produced non-converged state local=%+v remote=%+v", op.ID, local, remote)
	}
	return state.CommitBaselineAndDeleteOperation(ctx, domain.Baseline{
		SyncRootID: op.SyncRootID,
		RelPath:    op.SrcPath,
		Local:      local,
		Remote:     remote,
	}, op.ID)
}

func baselineFromSnapshot(rootID int64, relPath string, snapshot reconcile.Snapshot) domain.Baseline {
	return domain.Baseline{
		SyncRootID: rootID,
		RelPath:    relPath,
		Local:      snapshot.Local[relPath],
		Remote:     snapshot.Remote[relPath],
	}
}

func operationFromDecision(rootID int64, decision domain.Decision) domain.Operation {
	var kind domain.OperationKind
	switch decision.Kind {
	case domain.DecisionEnsureRemote:
		kind = domain.OperationEnsureRemote
	case domain.DecisionEnsureLocal:
		kind = domain.OperationEnsureLocal
	case domain.DecisionDeleteRemote:
		kind = domain.OperationDeleteRemote
	case domain.DecisionDeleteLocal:
		kind = domain.OperationDeleteLocal
	}
	return domain.Operation{
		SyncRootID:     rootID,
		Kind:           kind,
		EntryKind:      decision.EntryKind,
		SrcPath:        decision.RelPath,
		ExpectedLocal:  decision.ExpectedLocal,
		ExpectedRemote: decision.ExpectedRemote,
	}
}

func externalDecisions(decisions []domain.Decision) []domain.Decision {
	out := make([]domain.Decision, 0, len(decisions))
	for _, decision := range decisions {
		switch decision.Kind {
		case domain.DecisionEnsureRemote, domain.DecisionEnsureLocal, domain.DecisionDeleteRemote, domain.DecisionDeleteLocal:
			out = append(out, decision)
		}
	}
	return out
}

func sortExternalDecisions(decisions []domain.Decision) {
	sort.SliceStable(decisions, func(i, j int) bool {
		gi := decisionGroup(decisions[i])
		gj := decisionGroup(decisions[j])
		if gi != gj {
			return gi < gj
		}
		di := pathDepth(decisions[i].RelPath)
		dj := pathDepth(decisions[j].RelPath)
		if di != dj {
			if gi >= 2 {
				return di > dj
			}
			return di < dj
		}
		return decisions[i].RelPath < decisions[j].RelPath
	})
}

func decisionGroup(decision domain.Decision) int {
	isDelete := decision.Kind == domain.DecisionDeleteRemote || decision.Kind == domain.DecisionDeleteLocal
	if !isDelete && decision.EntryKind == domain.KindDir {
		return 0
	}
	if !isDelete {
		return 1
	}
	if decision.EntryKind == domain.KindFile {
		return 2
	}
	return 3
}

func pathDepth(relPath string) int {
	return strings.Count(relPath, "/") + 1
}

func remoteMatchesExpectation(remote domain.RemoteFingerprint, expected domain.RemoteExpectation, kind domain.EntryKind) bool {
	if expected.Absent {
		return !remote.Present
	}
	if !remote.Present || remote.Kind != kind || remote.ID != expected.ID {
		return false
	}
	if kind == domain.KindFile {
		return remote.Rev == expected.Rev
	}
	return expected.Rev == ""
}

func expectationFromRemote(remote domain.RemoteFingerprint) domain.RemoteExpectation {
	if !remote.Present {
		return domain.RemoteExpectation{Absent: true}
	}
	return domain.RemoteExpectation{ID: remote.ID, Rev: remote.Rev}
}

func reconcileConflictRecords(ctx context.Context, rootID int64, state *store.Store, snapshot reconcile.Snapshot, decisions []domain.Decision) error {
	current := make(map[string]domain.Decision)
	for _, decision := range decisions {
		if decision.Kind == domain.DecisionConflict {
			current[decision.RelPath] = decision
		}
	}
	existing, err := state.ListConflicts(ctx, rootID, true)
	if err != nil {
		return err
	}
	existingByPath := make(map[string]domain.Conflict, len(existing))
	for _, conflict := range existing {
		if reconcile.PathExcluded(snapshot.Excluded, conflict.RelPath) {
			continue
		}
		decision, stillConflict := current[conflict.RelPath]
		if !stillConflict {
			if err := state.ResolveConflict(ctx, conflict.ID); err != nil {
				return err
			}
			continue
		}
		local := snapshot.Local[conflict.RelPath]
		remote := snapshot.Remote[conflict.RelPath]
		if conflict.Kind != decision.Conflict || !domain.LocalEquivalent(conflict.Local, local) || !domain.RemoteEquivalent(conflict.Remote, remote) {
			if err := state.ResolveConflict(ctx, conflict.ID); err != nil {
				return err
			}
			continue
		}
		existingByPath[conflict.RelPath] = conflict
	}
	for relPath, decision := range current {
		if _, exists := existingByPath[relPath]; exists {
			continue
		}
		if _, err := state.CreateConflict(ctx, domain.Conflict{
			SyncRootID: rootID,
			RelPath:    relPath,
			Kind:       decision.Conflict,
			Local:      snapshot.Local[relPath],
			Remote:     snapshot.Remote[relPath],
		}); err != nil {
			return err
		}
	}
	return nil
}
