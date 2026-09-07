package reconcile

import (
	"fmt"
	"sort"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

// Snapshot contains a complete repair/discovery observation. Entries maps
// contain present paths only; omission means absent and is trusted only when
// the corresponding Complete flag is true.
type Snapshot struct {
	Local          map[string]domain.LocalFingerprint
	Remote         map[string]domain.RemoteFingerprint
	Content        map[string]domain.ContentEvidence
	LocalComplete  bool
	RemoteComplete bool
	RootHealthy    bool
}

// RootPlan is deterministic and side-effect free. A blocked plan must not be
// partially executed; the caller may surface the reason or explicitly approve
// a mass-delete threshold violation and re-plan.
type RootPlan struct {
	Initial         bool
	Decisions       []domain.Decision
	ProposedDeletes int
	Conflicts       int
	ContentChecks   int
	Blocked         bool
	BlockReason     BlockReason
}

// PlanFullSnapshot performs full-root reconciliation. It intentionally fails
// closed on incomplete scans or an unhealthy root marker because omission in a
// snapshot would otherwise be indistinguishable from deletion.
func PlanFullSnapshot(syncRootID int64, initial bool, baselines []domain.Baseline, snapshot Snapshot, deletePolicy DeletePolicy) (RootPlan, error) {
	if syncRootID <= 0 {
		return RootPlan{}, fmt.Errorf("sync root ID must be positive")
	}
	if !snapshot.LocalComplete {
		return RootPlan{Initial: initial, Blocked: true, BlockReason: BlockIncompleteLocal}, nil
	}
	if !snapshot.RemoteComplete {
		return RootPlan{Initial: initial, Blocked: true, BlockReason: BlockIncompleteRemote}, nil
	}
	if !snapshot.RootHealthy {
		return RootPlan{Initial: initial, Blocked: true, BlockReason: BlockRootUnhealthy}, nil
	}
	if err := validateSnapshot(snapshot); err != nil {
		return RootPlan{}, err
	}

	baselineByPath := make(map[string]domain.Baseline, len(baselines))
	for _, baseline := range baselines {
		if err := baseline.Validate(); err != nil {
			return RootPlan{}, fmt.Errorf("baseline %q: %w", baseline.RelPath, err)
		}
		if baseline.SyncRootID != syncRootID {
			return RootPlan{}, fmt.Errorf("baseline %q belongs to sync root %d, want %d", baseline.RelPath, baseline.SyncRootID, syncRootID)
		}
		if _, exists := baselineByPath[baseline.RelPath]; exists {
			return RootPlan{}, fmt.Errorf("duplicate baseline path %q", baseline.RelPath)
		}
		baselineByPath[baseline.RelPath] = baseline
	}
	if initial && len(baselineByPath) != 0 {
		return RootPlan{}, fmt.Errorf("initial reconciliation requires an empty committed baseline")
	}

	paths := unionPaths(baselineByPath, snapshot.Local, snapshot.Remote)
	plan := RootPlan{Initial: initial, Decisions: make([]domain.Decision, 0, len(paths))}

	for _, relPath := range paths {
		local := snapshot.Local[relPath]
		remote := snapshot.Remote[relPath]
		content := snapshot.Content[relPath]

		var (
			decision domain.Decision
			err      error
		)
		if initial {
			decision, err = PlanInitial(relPath, local, remote, content)
		} else {
			baseline, exists := baselineByPath[relPath]
			if !exists {
				baseline = domain.Baseline{SyncRootID: syncRootID, RelPath: relPath}
			}
			decision, err = PlanThreeWay(baseline, local, remote, content)
		}
		if err != nil {
			return RootPlan{}, fmt.Errorf("plan %q: %w", relPath, err)
		}
		plan.Decisions = append(plan.Decisions, decision)
		switch decision.Kind {
		case domain.DecisionDeleteLocal, domain.DecisionDeleteRemote:
			plan.ProposedDeletes++
		case domain.DecisionConflict:
			plan.Conflicts++
		case domain.DecisionCompareContent:
			plan.ContentChecks++
		}
	}

	if initial && plan.ProposedDeletes != 0 {
		return RootPlan{}, fmt.Errorf("internal error: initial reconciliation proposed %d deletes", plan.ProposedDeletes)
	}
	if !initial {
		reason, err := evaluateDeleteGate(deletePolicy, len(baselineByPath), plan.ProposedDeletes)
		if err != nil {
			return RootPlan{}, err
		}
		if reason != BlockNone {
			plan.Blocked = true
			plan.BlockReason = reason
		}
	}
	return plan, nil
}

func validateSnapshot(snapshot Snapshot) error {
	for relPath, local := range snapshot.Local {
		if err := domain.ValidateRelPath(relPath); err != nil {
			return fmt.Errorf("local snapshot path: %w", err)
		}
		if !local.Present {
			return fmt.Errorf("local snapshot %q stores an absent entry; omit absent paths instead", relPath)
		}
		if err := local.Validate(); err != nil {
			return fmt.Errorf("local snapshot %q: %w", relPath, err)
		}
	}
	for relPath, remote := range snapshot.Remote {
		if err := domain.ValidateRelPath(relPath); err != nil {
			return fmt.Errorf("remote snapshot path: %w", err)
		}
		if !remote.Present {
			return fmt.Errorf("remote snapshot %q stores an absent entry; omit absent paths instead", relPath)
		}
		if err := remote.Validate(); err != nil {
			return fmt.Errorf("remote snapshot %q: %w", relPath, err)
		}
	}
	for relPath, evidence := range snapshot.Content {
		if err := domain.ValidateRelPath(relPath); err != nil {
			return fmt.Errorf("content evidence path: %w", err)
		}
		if evidence > domain.ContentDifferent {
			return fmt.Errorf("content evidence %q has invalid value %d", relPath, evidence)
		}
		_, localExists := snapshot.Local[relPath]
		_, remoteExists := snapshot.Remote[relPath]
		if !localExists || !remoteExists {
			return fmt.Errorf("content evidence %q requires present local and remote entries", relPath)
		}
	}
	return nil
}

func unionPaths(baselines map[string]domain.Baseline, local map[string]domain.LocalFingerprint, remote map[string]domain.RemoteFingerprint) []string {
	set := make(map[string]struct{}, len(baselines)+len(local)+len(remote))
	for relPath := range baselines {
		set[relPath] = struct{}{}
	}
	for relPath := range local {
		set[relPath] = struct{}{}
	}
	for relPath := range remote {
		set[relPath] = struct{}{}
	}
	paths := make([]string, 0, len(set))
	for relPath := range set {
		paths = append(paths, relPath)
	}
	sort.Strings(paths)
	return paths
}
