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
	ScanLocal(context.Context, []domain.Operation, []string) (map[string]domain.LocalFingerprint, []string, map[string]domain.FollowedPhysicalClaim, map[string]domain.CopyProjectionEvidence, error)
	ScanRemote(context.Context) (map[string]domain.RemoteFingerprint, bool, error)
	ObserveLocalEntry(context.Context, string) (domain.LocalFingerprint, error)
	ObservePinnedLocalEntry(context.Context, domain.Operation) (domain.LocalFingerprint, bool, error)
	ObserveRemoteEntry(context.Context, string) (domain.RemoteFingerprint, error)
	ResolveLocalMutationTarget(context.Context, string, domain.LocalFingerprint, domain.EntryKind, []string) (domain.LocalMutationTarget, error)
	PinnedLocalPreconditionHolds(context.Context, domain.Operation) (bool, error)
	LocalRecoveryArtifact(context.Context, domain.Operation) (string, bool, error)
	CleanupLocalDownloadArtifact(context.Context, domain.Operation) error
	CleanupLocalRecoveryArtifact(context.Context, domain.Operation) error
	CompareFileContent(context.Context, string, domain.LocalFingerprint, domain.RemoteExpectation) (bool, error)
	ComparePinnedFileContent(context.Context, domain.Operation, domain.RemoteExpectation) (bool, error)
	Upload(context.Context, string, domain.LocalFingerprint, domain.RemoteExpectation) (domain.RemoteFingerprint, error)
	EnsureRemoteDir(context.Context, string, domain.RemoteExpectation) error
	EnsureLocalFile(context.Context, domain.Operation, []string, func() error) (domain.LocalFingerprint, error)
	EnsureLocalDir(context.Context, domain.Operation, []string, func() error) error
	DeleteRemoteFile(context.Context, domain.RemoteExpectation) error
	DeleteRemoteDir(context.Context, domain.RemoteExpectation) error
	DeleteLocal(context.Context, domain.Operation, []string, func() error) error
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
		peerRoots, err := configuredPeerLocalRoots(ctx, state, root.ID)
		if err != nil {
			return result, err
		}
		preflight, _, followedClaims, _, err := scanCompleteSnapshot(ctx, data, root.Initialized, operations, peerRoots)
		if err != nil {
			return result, fmt.Errorf("operation recovery namespace preflight: %w", err)
		}
		if err := validateSnapshotNamespace(preflight.Local, preflight.Remote, targetOS); err != nil {
			result.Blocked = true
			result.BlockReason = string(reconcile.BlockNamespaceUnsafe)
			result.BlockDetail = err.Error()
			return result, nil
		}
		ok, detail, err := followedPhysicalAuthority(ctx, state, root, preflight, followedClaims)
		if err != nil {
			return result, err
		}
		if !ok {
			result.Blocked = true
			result.BlockReason = "followed-physical-authority-blocked"
			result.BlockDetail = detail
			return result, nil
		}
	}

	blocked, recovered, err := recoverExistingOperations(ctx, state, data, operations, root.EffectiveSymlinkMode() == domain.SymlinkCopy)
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

		peerRoots, err := configuredPeerLocalRoots(ctx, state, root.ID)
		if err != nil {
			return result, err
		}
		snapshot, remoteRootPresent, followedClaims, copyEvidence, err := scanCompleteSnapshot(ctx, data, root.Initialized, nil, peerRoots)
		if err != nil {
			return result, err
		}
		if err := validateSnapshotNamespace(snapshot.Local, snapshot.Remote, targetOS); err != nil {
			result.Blocked = true
			result.BlockReason = string(reconcile.BlockNamespaceUnsafe)
			result.BlockDetail = err.Error()
			return result, nil
		}
		ok, detail, err := followedPhysicalAuthority(ctx, state, root, snapshot, followedClaims)
		if err != nil {
			return result, err
		}
		if !ok {
			result.Blocked = true
			result.BlockReason = "followed-physical-authority-blocked"
			result.BlockDetail = detail
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
				if err := state.InitializeSyncRoot(ctx, root.ID, followedClaims); err != nil {
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
			op, err = pinLocalMutationTarget(ctx, state, data, op, copyEvidence)
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

func configuredPeerLocalRoots(ctx context.Context, state *store.Store, rootID int64) ([]string, error) {
	roots, err := state.ListSyncRoots(ctx)
	if err != nil {
		return nil, fmt.Errorf("list sync roots for physical ownership: %w", err)
	}
	peers := make([]string, 0, len(roots))
	for _, root := range roots {
		if root.ID != rootID {
			peers = append(peers, root.LocalRoot)
		}
	}
	return peers, nil
}

func scanCompleteSnapshot(ctx context.Context, data DataPlane, requireRemoteRoot bool, operations []domain.Operation, peerLocalRoots []string) (reconcile.Snapshot, bool, map[string]domain.FollowedPhysicalClaim, map[string]domain.CopyProjectionEvidence, error) {
	local, excluded, followedClaims, copyEvidence, err := data.ScanLocal(ctx, operations, peerLocalRoots)
	if err != nil {
		return reconcile.Snapshot{}, false, nil, nil, fmt.Errorf("complete local scan: %w", err)
	}
	remote, remoteRootPresent, err := data.ScanRemote(ctx)
	if err != nil {
		return reconcile.Snapshot{}, remoteRootPresent, nil, nil, fmt.Errorf("complete remote scan: %w", err)
	}
	if requireRemoteRoot && !remoteRootPresent {
		return reconcile.Snapshot{}, false, nil, nil, fmt.Errorf("complete remote scan: selected remote root is missing after initialization")
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
	}, remoteRootPresent, followedClaims, copyEvidence, nil
}

func followedPhysicalAuthority(ctx context.Context, state *store.Store, root domain.SyncRoot, snapshot reconcile.Snapshot, observed map[string]domain.FollowedPhysicalClaim) (bool, string, error) {
	ok, detail, err := state.ReserveFollowedPhysicalClaims(ctx, root.ID, observed)
	if err != nil || !ok {
		return ok, detail, err
	}
	expected, err := state.ListFollowedPhysicalClaims(ctx, root.ID)
	if err != nil {
		return false, "", err
	}
	for relPath := range expected {
		if _, ok := observed[relPath]; ok {
			continue
		}
		if !root.Initialized {
			return false, fmt.Sprintf("followed physical claim %q disappeared during initial pairing; re-pair the root before continuing", relPath), nil
		}
		if reconcile.PathExcluded(snapshot.Excluded, relPath) {
			continue
		}
		if local, ok := snapshot.Local[relPath]; ok && local.Present {
			return false, fmt.Sprintf("followed path %q no longer resolves through its durable physical claim; re-pair the root before accepting the replacement", relPath), nil
		}
		// For an initialized root, deletion of the lexical symlink itself is an
		// ordinary local namespace deletion. Its durable claim intentionally stays
		// reserved for the lifetime of this root pairing, so recreating the path
		// with a different physical object still requires an explicit re-pair.
	}
	return true, "", nil
}

func isLocalMutation(op domain.Operation) bool {
	return op.Kind == domain.OperationEnsureLocal || op.Kind == domain.OperationDeleteLocal
}

func copyProjectionEvidenceApplies(boundary string, evidence domain.CopyProjectionEvidence, relPath string) bool {
	return boundary == relPath || (evidence.Kind == domain.KindDir && strings.HasPrefix(relPath, boundary+"/"))
}

func copyProjectionContinuityHolds(relPath string, scanned, current map[string]domain.CopyProjectionEvidence) (bool, string) {
	for boundary, evidence := range scanned {
		if !copyProjectionEvidenceApplies(boundary, evidence, relPath) {
			continue
		}
		currentEvidence, ok := current[boundary]
		if !ok {
			return false, fmt.Sprintf("scan-time copy boundary %q disappeared", boundary)
		}
		if currentEvidence != evidence {
			return false, fmt.Sprintf("copy boundary %q retargeted from %q to %q", boundary, evidence.TargetPath, currentEvidence.TargetPath)
		}
	}
	for boundary, evidence := range current {
		if !copyProjectionEvidenceApplies(boundary, evidence, relPath) {
			return false, fmt.Sprintf("resolved copy boundary %q does not authorize mutation path %q", boundary, relPath)
		}
		scanEvidence, ok := scanned[boundary]
		if !ok {
			return false, fmt.Sprintf("copy boundary %q appeared after the complete scan", boundary)
		}
		if scanEvidence != evidence {
			return false, fmt.Sprintf("copy boundary %q changed since the complete scan", boundary)
		}
	}
	return true, ""
}

func pinLocalMutationTarget(ctx context.Context, state *store.Store, data DataPlane, op domain.Operation, scanCopyEvidence map[string]domain.CopyProjectionEvidence) (domain.Operation, error) {
	if !isLocalMutation(op) {
		return op, nil
	}
	if op.LocalTargetPath != "" || op.LocalTargetIdentity != "" || op.LocalTargetAuthority != "" {
		if op.LocalTargetPath == "" || op.LocalTargetIdentity == "" || op.LocalTargetAuthority == "" {
			return op, fmt.Errorf("operation %d has an incomplete physical local target pin", op.ID)
		}
		return op, nil
	}
	if op.Phase != domain.OperationPlanned || op.Attempts != 0 {
		return op, fmt.Errorf("operation %d has no pinned local target after mutation attempts started", op.ID)
	}
	peerRoots, err := configuredPeerLocalRoots(ctx, state, op.SyncRootID)
	if err != nil {
		return op, err
	}
	target, err := data.ResolveLocalMutationTarget(ctx, op.SrcPath, op.ExpectedLocal, op.EntryKind, peerRoots)
	if err != nil {
		// This intent is still planned, unpinned, and has never attempted an
		// external mutation. A resolution failure means the snapshot used to
		// create it is no longer a safe authority; discard it so the next cycle
		// must obtain a fresh complete scan instead of retrying stale intent.
		if deleteErr := state.DeleteOperation(ctx, op.ID); deleteErr != nil {
			return op, fmt.Errorf("resolve local mutation target: %v; discard stale planned operation: %w", err, deleteErr)
		}
		return op, err
	}
	if scanCopyEvidence != nil {
		if ok, detail := copyProjectionContinuityHolds(op.SrcPath, scanCopyEvidence, target.CopyProjectionEvidence); !ok {
			if deleteErr := state.DeleteOperation(ctx, op.ID); deleteErr != nil {
				return op, fmt.Errorf("copy projection authority changed since complete scan: %s; discard stale planned operation: %w", detail, deleteErr)
			}
			return op, fmt.Errorf("copy projection authority changed since complete scan: %s", detail)
		}
	}
	ok, detail, err := state.AuthorizeAndPinLocalMutation(ctx, op.ID, target)
	if err != nil {
		return op, err
	}
	if !ok {
		if deleteErr := state.DeleteOperation(ctx, op.ID); deleteErr != nil {
			return op, fmt.Errorf("local mutation authority changed before pin: %s; discard stale planned operation: %w", detail, deleteErr)
		}
		return op, fmt.Errorf("local mutation authority changed before pin: %s", detail)
	}
	op.LocalTargetPath = target.Path
	op.LocalTargetIdentity = target.AnchorIdentity
	op.LocalTargetAuthority = target.Authority
	if op.LocalTargetAuthority == "" {
		if len(target.FollowedClaims) != 0 {
			op.LocalTargetAuthority = domain.LocalMutationFollowPhysical
		} else {
			op.LocalTargetAuthority = domain.LocalMutationLexical
		}
	}
	op.LocalSymlinkTarget = target.SymlinkTarget
	return op, nil
}

func recoverExistingOperations(ctx context.Context, state *store.Store, data DataPlane, operations []domain.Operation, discardUnpinnedCopyPlans bool) (blocked bool, recovered int, err error) {
	for _, op := range operations {
		switch op.Phase {
		case domain.OperationBlocked:
			return true, recovered, nil
		case domain.OperationPlanned:
			if discardUnpinnedCopyPlans && isLocalMutation(op) && op.Attempts == 0 && op.LocalTargetPath == "" && op.LocalTargetIdentity == "" && op.LocalTargetAuthority == "" {
				// This operation survived from a prior cycle without a durable pin,
				// so the in-memory copy projection evidence that produced it is
				// gone. No side effect has started: discard and force a fresh full
				// scan/replan instead of reconstructing authority from fingerprints.
				if err := state.DeleteOperation(ctx, op.ID); err != nil {
					return false, recovered, err
				}
				continue
			}
			op, err = pinLocalMutationTarget(ctx, state, data, op, nil)
			if err != nil {
				return false, recovered, err
			}
			if ownsDisposablePlannedDownload(op) {
				// A hard crash may have left the exact operation-owned download
				// staging slot behind while this durable operation was still planned.
				// Remove that disposable state before any branch can discard the
				// journal row; otherwise the next complete scan would see an orphaned
				// reserved artifact with no remaining durable owner.
				if err := data.CleanupLocalDownloadArtifact(ctx, op); err != nil {
					return false, recovered, err
				}
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
			if isLocalMutation(op) && (op.LocalTargetPath == "" || op.LocalTargetIdentity == "" || op.LocalTargetAuthority == "") {
				detail := "local mutation started before a complete target path, identity, and authority were pinned; automatic replay is unsafe"
				if err := state.SetOperationPhase(ctx, op.ID, domain.OperationBlocked, detail, false); err != nil {
					return false, recovered, err
				}
				return true, recovered, nil
			}
			satisfied, local, remote, err := proveRecoveredPostcondition(ctx, data, op)
			if err != nil {
				_ = state.SetOperationPhase(ctx, op.ID, domain.OperationRecovering, err.Error(), false)
				return false, recovered, err
			}
			if satisfied {
				if isLocalMutation(op) {
					if err := data.CleanupLocalRecoveryArtifact(ctx, op); err != nil {
						_ = state.SetOperationPhase(ctx, op.ID, domain.OperationRecovering, err.Error(), false)
						return false, recovered, err
					}
				}
				if err := commitCompletedOperation(ctx, state, op, local, remote); err != nil {
					return false, recovered, err
				}
				recovered++
				continue
			}
			if isLocalMutation(op) {
				recoveryPath, pending, err := data.LocalRecoveryArtifact(ctx, op)
				if err != nil {
					return false, recovered, err
				}
				if pending {
					detail := fmt.Sprintf("local data preserved at recovery artifact %q; prior completion is not proven and automatic replay is unsafe", recoveryPath)
					if err := state.SetOperationPhase(ctx, op.ID, domain.OperationBlocked, detail, false); err != nil {
						return false, recovered, err
					}
					return true, recovered, nil
				}
			}
			holds, err := recoveryRetryPreconditionsHold(ctx, data, op)
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

func ownsDisposablePlannedDownload(op domain.Operation) bool {
	if op.Kind != domain.OperationEnsureLocal || op.EntryKind != domain.KindFile || op.Phase != domain.OperationPlanned || op.Attempts != 0 || op.ID <= 0 {
		return false
	}
	if op.LocalTargetPath == "" || op.LocalTargetIdentity == "" || op.LocalTargetAuthority == "" {
		return false
	}
	return true
}

func executePersistedOperation(ctx context.Context, state *store.Store, data DataPlane, op domain.Operation) error {
	if op.Kind == domain.OperationMoveRemote || op.Kind == domain.OperationMoveLocal {
		if err := state.SetOperationPhase(ctx, op.ID, domain.OperationBlocked, "move operations are not emitted by the current path planner", false); err != nil {
			return err
		}
		return fmt.Errorf("operation %d uses unsupported move recovery", op.ID)
	}
	localMutation := isLocalMutation(op)
	var peerRoots []string
	if localMutation {
		var peerErr error
		peerRoots, peerErr = configuredPeerLocalRoots(ctx, state, op.SyncRootID)
		if peerErr != nil {
			if op.Phase == domain.OperationPlanned && op.Attempts == 0 {
				if deleteErr := state.DeleteOperation(ctx, op.ID); deleteErr != nil {
					return fmt.Errorf("load peer roots before local mutation: %v; discard known-unstarted operation: %w", peerErr, deleteErr)
				}
			} else {
				_ = state.SetOperationPhase(ctx, op.ID, domain.OperationRecovering, peerErr.Error(), false)
			}
			return peerErr
		}
	} else if err := state.SetOperationPhase(ctx, op.ID, domain.OperationRunning, "", true); err != nil {
		return err
	}

	localSideEffectStarted := false
	beginLocalSideEffect := func() error {
		if localSideEffectStarted {
			return fmt.Errorf("operation %d attempted to begin its local side effect more than once", op.ID)
		}
		if err := state.SetOperationPhase(ctx, op.ID, domain.OperationRunning, "", true); err != nil {
			return err
		}
		localSideEffectStarted = true
		return nil
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
			err = data.EnsureLocalDir(ctx, op, peerRoots, beginLocalSideEffect)
		} else {
			exactDownloadResult, err = data.EnsureLocalFile(ctx, op, peerRoots, beginLocalSideEffect)
			hasExactDownloadResult = err == nil
		}
	case domain.OperationDeleteRemote:
		if op.EntryKind == domain.KindDir {
			err = data.DeleteRemoteDir(ctx, op.ExpectedRemote)
		} else {
			err = data.DeleteRemoteFile(ctx, op.ExpectedRemote)
		}
	case domain.OperationDeleteLocal:
		err = data.DeleteLocal(ctx, op, peerRoots, beginLocalSideEffect)
	default:
		err = fmt.Errorf("unsupported operation kind %q", op.Kind)
	}
	if err != nil {
		if localMutation && op.Phase == domain.OperationPlanned && op.Attempts == 0 && !localSideEffectStarted {
			// The executor returned before crossing the durable side-effect boundary.
			// The current pin is therefore stale authority, not an unknown outcome:
			// discard it and require a fresh complete scan/replan next cycle.
			if deleteErr := state.DeleteOperation(ctx, op.ID); deleteErr != nil {
				return fmt.Errorf("execute operation %d (%s %q): %v; discard known-unstarted operation: %w", op.ID, op.Kind, op.SrcPath, err, deleteErr)
			}
		} else {
			_ = state.SetOperationPhase(ctx, op.ID, domain.OperationRecovering, err.Error(), false)
		}
		return fmt.Errorf("execute operation %d (%s %q): %w", op.ID, op.Kind, op.SrcPath, err)
	}
	if localMutation && !localSideEffectStarted {
		err := fmt.Errorf("operation %d returned local-mutation success without durably entering the side-effect phase", op.ID)
		if op.Phase == domain.OperationPlanned && op.Attempts == 0 {
			if deleteErr := state.DeleteOperation(ctx, op.ID); deleteErr != nil {
				return fmt.Errorf("%v; discard known-unstarted operation: %w", err, deleteErr)
			}
		} else {
			_ = state.SetOperationPhase(ctx, op.ID, domain.OperationRecovering, err.Error(), false)
		}
		return err
	}

	local, ordinary, err := observeOperationLocalPostState(ctx, data, op)
	if err != nil {
		_ = state.SetOperationPhase(ctx, op.ID, domain.OperationRecovering, err.Error(), false)
		return err
	}
	if !ordinary {
		err := fmt.Errorf("operation %d returned success but pinned local target is not materialized as an ordinary entry", op.ID)
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

func recoveryRetryPreconditionsHold(ctx context.Context, data DataPlane, op domain.Operation) (bool, error) {
	if !isLocalMutation(op) {
		return operationPreconditionsHold(ctx, data, op)
	}
	localHolds, err := data.PinnedLocalPreconditionHolds(ctx, op)
	if err != nil || !localHolds {
		return localHolds, err
	}
	remote, err := data.ObserveRemoteEntry(ctx, op.SrcPath)
	if err != nil {
		return false, err
	}
	return remoteMatchesExpectation(remote, op.ExpectedRemote, op.EntryKind), nil
}

func observeOperationLocalPostState(ctx context.Context, data DataPlane, op domain.Operation) (domain.LocalFingerprint, bool, error) {
	if !isLocalMutation(op) {
		local, err := data.ObserveLocalEntry(ctx, op.SrcPath)
		return local, true, err
	}
	return data.ObservePinnedLocalEntry(ctx, op)
}

func proveRecoveredPostcondition(ctx context.Context, data DataPlane, op domain.Operation) (bool, domain.LocalFingerprint, domain.RemoteFingerprint, error) {
	local, ordinary, err := observeOperationLocalPostState(ctx, data, op)
	if err != nil {
		return false, domain.LocalFingerprint{}, domain.RemoteFingerprint{}, err
	}
	remote, err := data.ObserveRemoteEntry(ctx, op.SrcPath)
	if err != nil {
		return false, domain.LocalFingerprint{}, domain.RemoteFingerprint{}, err
	}
	if !ordinary {
		return false, local, remote, nil
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
		equal, err := data.ComparePinnedFileContent(ctx, op, op.ExpectedRemote)
		if err != nil {
			return false, local, remote, err
		}
		if !equal {
			return false, local, remote, nil
		}
		local2, remote2, err := observeStableLocalOperationPair(ctx, data, op, local, remote)
		return err == nil, local2, remote2, err
	default:
		return false, local, remote, nil
	}
}

func observeStableLocalOperationPair(ctx context.Context, data DataPlane, op domain.Operation, beforeLocal domain.LocalFingerprint, beforeRemote domain.RemoteFingerprint) (domain.LocalFingerprint, domain.RemoteFingerprint, error) {
	local, ordinary, err := data.ObservePinnedLocalEntry(ctx, op)
	if err != nil {
		return domain.LocalFingerprint{}, domain.RemoteFingerprint{}, err
	}
	if !ordinary {
		return local, domain.RemoteFingerprint{}, fmt.Errorf("pinned local target for %q changed to a non-ordinary entry while proving operation recovery", op.SrcPath)
	}
	remote, err := data.ObserveRemoteEntry(ctx, op.SrcPath)
	if err != nil {
		return domain.LocalFingerprint{}, domain.RemoteFingerprint{}, err
	}
	if !domain.LocalEquivalent(local, beforeLocal) || !domain.RemoteEquivalent(remote, beforeRemote) {
		return local, remote, fmt.Errorf("entry %q changed while proving operation recovery", op.SrcPath)
	}
	return local, remote, nil
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
