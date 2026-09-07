package reconcile

import (
	"fmt"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

// PlanInitial creates a non-destructive initial-pairing decision for one path.
// Deletion is never inferred because there is no committed baseline yet.
func PlanInitial(relPath string, local domain.LocalFingerprint, remote domain.RemoteFingerprint, content domain.ContentEvidence) (domain.Decision, error) {
	if err := domain.ValidateRelPath(relPath); err != nil {
		return domain.Decision{}, err
	}
	if err := local.Validate(); err != nil {
		return domain.Decision{}, fmt.Errorf("local: %w", err)
	}
	if err := remote.Validate(); err != nil {
		return domain.Decision{}, fmt.Errorf("remote: %w", err)
	}
	if err := validateContentEvidence(content); err != nil {
		return domain.Decision{}, err
	}

	base := domain.Decision{RelPath: relPath, ExpectedLocal: local, ExpectedRemote: remoteExpectation(remote)}

	switch {
	case !local.Present && !remote.Present:
		base.Kind = domain.DecisionNoop
		base.Reason = "path is absent on both sides"
		return base, nil
	case local.Present && !remote.Present:
		base.Kind = domain.DecisionEnsureRemote
		base.EntryKind = local.Kind
		base.Reason = "initial local-only path"
		return base, nil
	case !local.Present && remote.Present:
		base.Kind = domain.DecisionEnsureLocal
		base.EntryKind = remote.Kind
		base.Reason = "initial remote-only path"
		return base, nil
	}

	if local.Kind != remote.Kind {
		return conflict(base, domain.ConflictKindMismatch, local.Kind, "initial path kinds differ"), nil
	}
	base.EntryKind = local.Kind

	if local.Kind == domain.KindDir {
		base.Kind = domain.DecisionCommitBaseline
		base.Reason = "directory exists on both sides; children reconcile independently"
		return base, nil
	}

	if local.Size != remote.Size {
		return conflict(base, domain.ConflictSimultaneousCreate, domain.KindFile, "initial same-path files have different sizes"), nil
	}

	return planContentConvergence(base, domain.ConflictSimultaneousCreate, content, "initial same-size files require byte comparison")
}

// PlanThreeWay compares current local/remote state against the last committed
// baseline and returns one semantic decision. It performs no I/O.
func PlanThreeWay(baseline domain.Baseline, local domain.LocalFingerprint, remote domain.RemoteFingerprint, content domain.ContentEvidence) (domain.Decision, error) {
	if err := baseline.Validate(); err != nil {
		return domain.Decision{}, fmt.Errorf("baseline: %w", err)
	}
	if err := local.Validate(); err != nil {
		return domain.Decision{}, fmt.Errorf("local: %w", err)
	}
	if err := remote.Validate(); err != nil {
		return domain.Decision{}, fmt.Errorf("remote: %w", err)
	}
	if err := validateContentEvidence(content); err != nil {
		return domain.Decision{}, err
	}

	base := domain.Decision{
		RelPath:        baseline.RelPath,
		ExpectedLocal:  local,
		ExpectedRemote: remoteExpectation(remote),
	}

	localChanged := !domain.LocalEquivalent(baseline.Local, local)
	remoteChanged := !domain.RemoteEquivalent(baseline.Remote, remote)

	if local.Present && remote.Present && local.Kind != remote.Kind {
		return conflict(base, domain.ConflictKindMismatch, local.Kind, "current path kinds differ"), nil
	}
	if local.Present && remote.Present {
		if (baseline.Local.Present && baseline.Local.Kind != local.Kind) ||
			(baseline.Remote.Present && baseline.Remote.Kind != remote.Kind) {
			return conflict(base, domain.ConflictKindMismatch, local.Kind, "entry kind changed from committed baseline"), nil
		}
	}

	if !localChanged && !remoteChanged {
		base.Kind = domain.DecisionNoop
		base.EntryKind = presentKind(local, remote)
		base.Reason = "neither side changed from baseline"
		return base, nil
	}

	if local.Present && remote.Present && local.Kind == domain.KindDir {
		base.Kind = domain.DecisionCommitBaseline
		base.EntryKind = domain.KindDir
		base.Reason = "directory path exists on both sides; reconcile child entries independently"
		return base, nil
	}

	if localChanged && !remoteChanged {
		return planLocalOnlyChange(base, local, remote), nil
	}
	if !localChanged && remoteChanged {
		return planRemoteOnlyChange(base, local, remote), nil
	}

	// Both sides changed from their committed baseline.
	if !local.Present && !remote.Present {
		base.Kind = domain.DecisionDropBaseline
		base.Reason = "both sides deleted the path"
		return base, nil
	}
	if !local.Present {
		return conflict(base, domain.ConflictLocalDeleteRemoteEdit, remote.Kind, "local deletion conflicts with remote change"), nil
	}
	if !remote.Present {
		return conflict(base, domain.ConflictRemoteDeleteLocalEdit, local.Kind, "remote deletion conflicts with local change"), nil
	}

	base.EntryKind = local.Kind

	conflictKind := domain.ConflictBothModified
	if !baseline.Local.Present && !baseline.Remote.Present {
		conflictKind = domain.ConflictSimultaneousCreate
	}
	if local.Size != remote.Size {
		return conflict(base, conflictKind, domain.KindFile, "both sides changed and file sizes differ"), nil
	}
	return planContentConvergence(base, conflictKind, content, "both sides changed; equal-size files require byte comparison")
}

func planLocalOnlyChange(base domain.Decision, local domain.LocalFingerprint, remote domain.RemoteFingerprint) domain.Decision {
	if !local.Present {
		if !remote.Present {
			base.Kind = domain.DecisionDropBaseline
			base.Reason = "local deleted and remote is already absent"
			return base
		}
		base.Kind = domain.DecisionDeleteRemote
		base.EntryKind = remote.Kind
		base.Reason = "local deleted while remote stayed at baseline"
		return base
	}

	base.Kind = domain.DecisionEnsureRemote
	base.EntryKind = local.Kind
	if remote.Present {
		base.Reason = "local changed while remote stayed at baseline"
	} else {
		base.Reason = "local path appeared while remote baseline remains absent"
	}
	return base
}

func planRemoteOnlyChange(base domain.Decision, local domain.LocalFingerprint, remote domain.RemoteFingerprint) domain.Decision {
	if !remote.Present {
		if !local.Present {
			base.Kind = domain.DecisionDropBaseline
			base.Reason = "remote deleted and local is already absent"
			return base
		}
		base.Kind = domain.DecisionDeleteLocal
		base.EntryKind = local.Kind
		base.Reason = "remote deleted while local stayed at baseline"
		return base
	}

	base.Kind = domain.DecisionEnsureLocal
	base.EntryKind = remote.Kind
	if local.Present {
		base.Reason = "remote changed while local stayed at baseline"
	} else {
		base.Reason = "remote path appeared while local baseline remains absent"
	}
	return base
}

func planContentConvergence(base domain.Decision, conflictKind domain.ConflictKind, evidence domain.ContentEvidence, compareReason string) (domain.Decision, error) {
	switch evidence {
	case domain.ContentUnknown:
		base.Kind = domain.DecisionCompareContent
		base.Reason = compareReason
		return base, nil
	case domain.ContentEqual:
		base.Kind = domain.DecisionCommitBaseline
		base.Reason = "explicit content comparison proved convergence"
		return base, nil
	case domain.ContentDifferent:
		return conflict(base, conflictKind, domain.KindFile, "explicit content comparison proved divergence"), nil
	default:
		return domain.Decision{}, fmt.Errorf("invalid content evidence %d", evidence)
	}
}

func conflict(base domain.Decision, kind domain.ConflictKind, entryKind domain.EntryKind, reason string) domain.Decision {
	base.Kind = domain.DecisionConflict
	base.EntryKind = entryKind
	base.Conflict = kind
	base.Reason = reason
	return base
}

func remoteExpectation(remote domain.RemoteFingerprint) domain.RemoteExpectation {
	if !remote.Present {
		return domain.RemoteExpectation{Absent: true}
	}
	return domain.RemoteExpectation{ID: remote.ID, Rev: remote.Rev}
}

func presentKind(local domain.LocalFingerprint, remote domain.RemoteFingerprint) domain.EntryKind {
	if local.Present {
		return local.Kind
	}
	if remote.Present {
		return remote.Kind
	}
	return ""
}

func validateContentEvidence(e domain.ContentEvidence) error {
	if e > domain.ContentDifferent {
		return fmt.Errorf("invalid content evidence %d", e)
	}
	return nil
}
