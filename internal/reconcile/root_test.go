package reconcile

import (
	"testing"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

func TestPlanFullSnapshotNormal(t *testing.T) {
	baselines := []domain.Baseline{
		rootBaseline("a.txt", lf(1, 10), rf("a", "ra", 1)),
		rootBaseline("b.txt", lf(2, 20), rf("b", "rb", 2)),
		rootBaseline("c.txt", lf(3, 30), rf("c", "rc", 3)),
		rootBaseline("d.txt", lf(4, 40), rf("d", "rd", 4)),
	}
	snapshot := completeSnapshot(
		map[string]domain.LocalFingerprint{
			"a.txt": lf(1, 10),
			"b.txt": lf(22, 220),
			"c.txt": lf(3, 30),
		},
		map[string]domain.RemoteFingerprint{
			"a.txt": rf("a", "ra", 1),
			"b.txt": rf("b", "rb", 2),
			"c.txt": rf("c", "rc2", 33),
			"d.txt": rf("d", "rd", 4),
		},
	)

	plan, err := PlanFullSnapshot(1, false, baselines, snapshot, DeletePolicy{MaxCount: 10, MaxFraction: 1})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Blocked {
		t.Fatalf("plan unexpectedly blocked: %+v", plan)
	}
	wantKinds := []domain.DecisionKind{
		domain.DecisionNoop,
		domain.DecisionEnsureRemote,
		domain.DecisionEnsureLocal,
		domain.DecisionDeleteRemote,
	}
	if len(plan.Decisions) != len(wantKinds) {
		t.Fatalf("decisions = %+v", plan.Decisions)
	}
	for i, want := range wantKinds {
		if plan.Decisions[i].Kind != want {
			t.Fatalf("decision[%d] path=%q kind=%q want=%q", i, plan.Decisions[i].RelPath, plan.Decisions[i].Kind, want)
		}
	}
	if plan.ProposedDeletes != 1 {
		t.Fatalf("proposed deletes = %d, want 1", plan.ProposedDeletes)
	}
	if got := plan.Decisions[1].ExpectedRemote; got.ID != "b" || got.Rev != "rb" || got.Absent {
		t.Fatalf("local edit lost remote CAS: %+v", got)
	}
}

func TestPlanFullSnapshotNewPathUsesExpectedAbsent(t *testing.T) {
	plan, err := PlanFullSnapshot(1, false, nil, completeSnapshot(
		map[string]domain.LocalFingerprint{"new.txt": lf(5, 50)},
		nil,
	), DeletePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Blocked || len(plan.Decisions) != 1 {
		t.Fatalf("plan = %+v", plan)
	}
	decision := plan.Decisions[0]
	if decision.Kind != domain.DecisionEnsureRemote || !decision.ExpectedRemote.Absent {
		t.Fatalf("new path decision = %+v", decision)
	}
}

func TestPlanFullSnapshotInitialIsNonDestructive(t *testing.T) {
	snapshot := completeSnapshot(
		map[string]domain.LocalFingerprint{
			"both.txt":  lf(7, 70),
			"local.txt": lf(1, 10),
		},
		map[string]domain.RemoteFingerprint{
			"both.txt":   rf("both", "r1", 7),
			"remote.txt": rf("remote", "r1", 2),
		},
	)
	plan, err := PlanFullSnapshot(1, true, nil, snapshot, DeletePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Blocked || plan.ProposedDeletes != 0 {
		t.Fatalf("initial plan = %+v", plan)
	}
	if plan.ContentChecks != 1 {
		t.Fatalf("initial content checks = %d, want 1", plan.ContentChecks)
	}
	for _, decision := range plan.Decisions {
		if decision.Kind == domain.DecisionDeleteLocal || decision.Kind == domain.DecisionDeleteRemote {
			t.Fatalf("initial plan proposed delete: %+v", decision)
		}
	}
}

func TestPlanFullSnapshotSafetyGates(t *testing.T) {
	base := []domain.Baseline{rootBaseline("a.txt", lf(1, 1), rf("a", "r", 1))}
	complete := completeSnapshot(nil, map[string]domain.RemoteFingerprint{"a.txt": rf("a", "r", 1)})

	tests := []struct {
		name     string
		snapshot Snapshot
		policy   DeletePolicy
		want     BlockReason
	}{
		{name: "local incomplete", snapshot: Snapshot{RemoteComplete: true, RootHealthy: true}, policy: DeletePolicy{MaxCount: 1}, want: BlockIncompleteLocal},
		{name: "remote incomplete", snapshot: Snapshot{LocalComplete: true, RootHealthy: true}, policy: DeletePolicy{MaxCount: 1}, want: BlockIncompleteRemote},
		{name: "root unhealthy", snapshot: Snapshot{LocalComplete: true, RemoteComplete: true}, policy: DeletePolicy{MaxCount: 1}, want: BlockRootUnhealthy},
		{name: "delete policy unset", snapshot: complete, policy: DeletePolicy{}, want: BlockDeletePolicyUnset},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := PlanFullSnapshot(1, false, base, tt.snapshot, tt.policy)
			if err != nil {
				t.Fatal(err)
			}
			if !plan.Blocked || plan.BlockReason != tt.want {
				t.Fatalf("plan = %+v, want block %q", plan, tt.want)
			}
		})
	}
}

func TestPlanFullSnapshotMassDeleteCountAndFraction(t *testing.T) {
	baselines := []domain.Baseline{
		rootBaseline("a.txt", lf(1, 1), rf("a", "r", 1)),
		rootBaseline("b.txt", lf(1, 1), rf("b", "r", 1)),
		rootBaseline("c.txt", lf(1, 1), rf("c", "r", 1)),
		rootBaseline("d.txt", lf(1, 1), rf("d", "r", 1)),
	}
	remote := map[string]domain.RemoteFingerprint{
		"a.txt": rf("a", "r", 1),
		"b.txt": rf("b", "r", 1),
		"c.txt": rf("c", "r", 1),
		"d.txt": rf("d", "r", 1),
	}

	countPlan, err := PlanFullSnapshot(1, false, baselines, completeSnapshot(nil, remote), DeletePolicy{MaxCount: 2, MaxFraction: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !countPlan.Blocked || countPlan.BlockReason != BlockMassDeleteCount || countPlan.ProposedDeletes != 4 {
		t.Fatalf("count-gated plan = %+v", countPlan)
	}

	oneLocal := map[string]domain.LocalFingerprint{
		"b.txt": lf(1, 1),
		"c.txt": lf(1, 1),
		"d.txt": lf(1, 1),
	}
	fractionPlan, err := PlanFullSnapshot(1, false, baselines, completeSnapshot(oneLocal, remote), DeletePolicy{MaxCount: 10, MaxFraction: 0.20})
	if err != nil {
		t.Fatal(err)
	}
	if !fractionPlan.Blocked || fractionPlan.BlockReason != BlockMassDeleteFraction || fractionPlan.ProposedDeletes != 1 {
		t.Fatalf("fraction-gated plan = %+v", fractionPlan)
	}

	approved, err := PlanFullSnapshot(1, false, baselines, completeSnapshot(nil, remote), DeletePolicy{MaxCount: 1, MaxFraction: 0.01, MassDeleteApproved: true})
	if err != nil {
		t.Fatal(err)
	}
	if approved.Blocked || approved.ProposedDeletes != 4 {
		t.Fatalf("approved mass-delete plan = %+v", approved)
	}
}

func TestPlanFullSnapshotRejectsUnsafeSnapshotEncoding(t *testing.T) {
	_, err := PlanFullSnapshot(1, false, nil, completeSnapshot(
		map[string]domain.LocalFingerprint{"absent.txt": {}},
		nil,
	), DeletePolicy{})
	if err == nil {
		t.Fatal("expected explicit absent map entry to be rejected")
	}

	_, err = PlanFullSnapshot(1, false, nil, Snapshot{
		Local:          map[string]domain.LocalFingerprint{"local.txt": lf(1, 1)},
		Content:        map[string]domain.ContentEvidence{"local.txt": domain.ContentEqual},
		LocalComplete:  true,
		RemoteComplete: true,
		RootHealthy:    true,
	}, DeletePolicy{})
	if err == nil {
		t.Fatal("expected content evidence without both present entries to be rejected")
	}
}

func TestPlanFullSnapshotInitialAllowsPartialBaselinesWithoutDeleteInference(t *testing.T) {
	baselines := []domain.Baseline{
		rootBaseline("gone.txt", lf(1, 1), rf("gone", "r1", 1)),
		rootBaseline("restore-local.txt", lf(2, 2), rf("restore", "r2", 2)),
	}
	snapshot := completeSnapshot(
		map[string]domain.LocalFingerprint{
			"restore-local.txt": lf(2, 2),
		},
		map[string]domain.RemoteFingerprint{},
	)
	plan, err := PlanFullSnapshot(1, true, baselines, snapshot, DeletePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if plan.ProposedDeletes != 0 {
		t.Fatalf("partial initial plan proposed %d deletes", plan.ProposedDeletes)
	}
	decisions := make(map[string]domain.Decision, len(plan.Decisions))
	for _, decision := range plan.Decisions {
		decisions[decision.RelPath] = decision
	}
	if decisions["gone.txt"].Kind != domain.DecisionDropBaseline {
		t.Fatalf("gone baseline decision = %+v", decisions["gone.txt"])
	}
	if decisions["restore-local.txt"].Kind != domain.DecisionEnsureRemote || !decisions["restore-local.txt"].ExpectedRemote.Absent {
		t.Fatalf("restore-local decision = %+v", decisions["restore-local.txt"])
	}
}

func TestPlanFullSnapshotExcludedPrefixHasNoDeleteOrDownloadAuthority(t *testing.T) {
	dirLocal := domain.LocalFingerprint{Present: true, Kind: domain.KindDir}
	dirRemote := domain.RemoteFingerprint{Present: true, Kind: domain.KindDir, ID: "dir-linked"}
	baselines := []domain.Baseline{
		rootBaseline("linked", dirLocal, dirRemote),
		rootBaseline("linked/file.txt", lf(4, 40), rf("file-linked", "r1", 4)),
		rootBaseline("ordinary.txt", lf(5, 50), rf("ordinary", "r1", 5)),
	}
	snapshot := completeSnapshot(
		map[string]domain.LocalFingerprint{"ordinary.txt": lf(5, 50)},
		map[string]domain.RemoteFingerprint{
			"linked":          dirRemote,
			"linked/file.txt": rf("file-linked", "r1", 4),
			"ordinary.txt":    rf("ordinary", "r1", 5),
		},
	)
	snapshot.Excluded = []string{"linked"}

	plan, err := PlanFullSnapshot(1, false, baselines, snapshot, DeletePolicy{MaxCount: 1, MaxFraction: 0.01})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Blocked || plan.ProposedDeletes != 0 {
		t.Fatalf("excluded namespace influenced delete gate: %+v", plan)
	}
	if len(plan.Decisions) != 1 || plan.Decisions[0].RelPath != "ordinary.txt" || plan.Decisions[0].Kind != domain.DecisionNoop {
		t.Fatalf("excluded namespace produced decisions: %+v", plan.Decisions)
	}
}

func completeSnapshot(local map[string]domain.LocalFingerprint, remote map[string]domain.RemoteFingerprint) Snapshot {
	return Snapshot{
		Local:          local,
		Remote:         remote,
		LocalComplete:  true,
		RemoteComplete: true,
		RootHealthy:    true,
	}
}

func rootBaseline(relPath string, local domain.LocalFingerprint, remote domain.RemoteFingerprint) domain.Baseline {
	return domain.Baseline{SyncRootID: 1, RelPath: relPath, Local: local, Remote: remote}
}
