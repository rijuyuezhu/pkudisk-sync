package syncer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
	"github.com/rijuyuezhu/pkudisk-sync/internal/executor"
	"github.com/rijuyuezhu/pkudisk-sync/internal/reconcile"
	"github.com/rijuyuezhu/pkudisk-sync/internal/rootmarker"
	"github.com/rijuyuezhu/pkudisk-sync/internal/store"
)

var _ DataPlane = (*executor.RootExecutor)(nil)
var _ DataPlane = (*fakeDataPlane)(nil)
var _ DataPlane = (*cancelUploadDataPlane)(nil)

func TestRunRootCycleCancellationLeavesRecoverableDurableIntent(t *testing.T) {
	state, root := newCycleRoot(t, true, false)
	base := &fakeDataPlane{
		local: map[string]domain.LocalFingerprint{
			"cancel.txt": localFileFP(4, 40),
		},
		remote: make(map[string]domain.RemoteFingerprint),
	}
	data := &cancelUploadDataPlane{
		fakeDataPlane: base,
		started:       make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{})
		done <- err
	}()

	select {
	case <-data.started:
	case <-time.After(3 * time.Second):
		t.Fatal("upload did not reach cancellable mutation")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled cycle error = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled cycle did not return")
	}

	operations, err := state.ListOperations(context.Background(), root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(operations) != 1 || (operations[0].Phase != domain.OperationRunning && operations[0].Phase != domain.OperationRecovering) {
		t.Fatalf("canceled mutation did not leave one recoverable intent: %+v", operations)
	}

	result, err := RunRootCycle(context.Background(), root.ID, state, base, reconcile.DeletePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Recovered != 1 || !result.Initialized {
		t.Fatalf("recovery result = %+v", result)
	}
	if operations, err := state.ListOperations(context.Background(), root.ID); err != nil || len(operations) != 0 {
		t.Fatalf("recovered operation remains = %+v err=%v", operations, err)
	}
}

func TestRunRootCycleInitialNestedTreeConvergesAndInitializes(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, false)
	data := &fakeDataPlane{
		local: map[string]domain.LocalFingerprint{
			"docs":       localDirFP(),
			"docs/a.txt": localFileFP(5, 50),
		},
		remote: make(map[string]domain.RemoteFingerprint),
	}

	result, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{MaxCount: 10, MaxFraction: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.Blocked || !result.Initial || !result.Initialized || result.Applied != 2 {
		t.Fatalf("cycle result = %+v", result)
	}
	if got := strings.Join(data.calls, ","); got != "ensure-remote-dir:docs,upload:docs/a.txt" {
		t.Fatalf("mutation order = %q", got)
	}
	assertRootInitialized(t, state, root.ID, true)
	baselines, err := state.ListBaselines(ctx, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(baselines) != 2 {
		t.Fatalf("baseline count = %d, want 2: %+v", len(baselines), baselines)
	}
	operations, err := state.ListOperations(ctx, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(operations) != 0 {
		t.Fatalf("completed operations remain: %+v", operations)
	}
}

func TestRunRootCycleInitialMissingRemoteRootAndEmptyLocalStaysDormant(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, false)
	data := &fakeDataPlane{
		local:             make(map[string]domain.LocalFingerprint),
		remote:            make(map[string]domain.RemoteFingerprint),
		remoteRootMissing: true,
	}

	result, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Initial || result.Initialized || !result.Skipped || result.Applied != 0 {
		t.Fatalf("cycle result = %+v", result)
	}
	assertRootInitialized(t, state, root.ID, false)
	if len(data.calls) != 0 {
		t.Fatalf("dormant empty root performed mutations: %v", data.calls)
	}
}

func TestRunRootCycleInitialMissingRemoteRootCanCreateIt(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, false)
	data := &fakeDataPlane{
		local:             map[string]domain.LocalFingerprint{"new.txt": localFileFP(3, 30)},
		remote:            make(map[string]domain.RemoteFingerprint),
		remoteRootMissing: true,
	}

	result, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{MaxCount: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Initial || !result.Initialized || result.Applied != 1 {
		t.Fatalf("cycle result = %+v", result)
	}
	if data.remoteRootMissing {
		t.Fatal("successful initial upload did not establish the remote root")
	}
	if got := strings.Join(data.calls, ","); got != "upload:new.txt" {
		t.Fatalf("calls = %q", got)
	}
	assertRootInitialized(t, state, root.ID, true)
}

func TestRunRootCycleInitializedMissingRemoteRootFailsClosed(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, false)
	data := &fakeDataPlane{
		local:  map[string]domain.LocalFingerprint{"keep.txt": localFileFP(4, 40)},
		remote: make(map[string]domain.RemoteFingerprint),
	}
	first, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{MaxCount: 10})
	if err != nil || !first.Initialized {
		t.Fatalf("initial cycle = %+v err=%v", first, err)
	}
	callsBefore := len(data.calls)
	data.remote = make(map[string]domain.RemoteFingerprint)
	data.remoteRootMissing = true

	second, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{MaxCount: 10})
	if err == nil || !strings.Contains(err.Error(), "selected remote root is missing after initialization") {
		t.Fatalf("missing initialized root error = %v", err)
	}
	if second.Applied != 0 || second.Blocked {
		t.Fatalf("missing initialized root result = %+v", second)
	}
	if len(data.calls) != callsBefore {
		t.Fatalf("missing initialized root performed mutations: %v", data.calls[callsBefore:])
	}
	if _, ok := data.local["keep.txt"]; !ok {
		t.Fatal("missing initialized remote root deleted local data")
	}
	operations, listErr := state.ListOperations(ctx, root.ID)
	if listErr != nil {
		t.Fatal(listErr)
	}
	if len(operations) != 0 {
		t.Fatalf("missing initialized root journaled operations: %+v", operations)
	}
}

func TestRunRootCycleInitialEqualContentCommitsWithoutMutation(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, false)
	data := &fakeDataPlane{
		local: map[string]domain.LocalFingerprint{"same.txt": localFileFP(4, 40)},
		remote: map[string]domain.RemoteFingerprint{
			"same.txt": remoteFileFP("doc-same", "rev-same", 4),
		},
		contentEqual: map[string]bool{"same.txt": true},
	}

	result, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Initialized || result.ContentChecks != 1 || result.Applied != 0 {
		t.Fatalf("cycle result = %+v", result)
	}
	if got := strings.Join(data.calls, ","); got != "compare:same.txt" {
		t.Fatalf("calls = %q", got)
	}
	baseline, ok, err := state.GetBaseline(ctx, root.ID, "same.txt")
	if err != nil || !ok {
		t.Fatalf("baseline = %+v ok=%v err=%v", baseline, ok, err)
	}
	if baseline.Remote.ID != "doc-same" || baseline.Remote.Rev != "rev-same" {
		t.Fatalf("baseline remote = %+v", baseline.Remote)
	}
}

func TestRunRootCycleInitialConflictPersistsAndDoesNotInitialize(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, false)
	data := &fakeDataPlane{
		local: map[string]domain.LocalFingerprint{"conflict.txt": localFileFP(4, 40)},
		remote: map[string]domain.RemoteFingerprint{
			"conflict.txt": remoteFileFP("doc-conflict", "rev-a", 4),
		},
		contentEqual: map[string]bool{"conflict.txt": false},
	}

	first, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if first.Initialized || first.Conflicts != 1 || first.ContentChecks != 1 {
		t.Fatalf("first result = %+v", first)
	}
	assertRootInitialized(t, state, root.ID, false)
	conflicts, err := state.ListConflicts(ctx, root.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(conflicts) != 1 || conflicts[0].RelPath != "conflict.txt" {
		t.Fatalf("conflicts = %+v", conflicts)
	}
	oldID := conflicts[0].ID

	second, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if second.Initialized || second.Conflicts != 1 {
		t.Fatalf("second result = %+v", second)
	}
	conflicts, err = state.ListConflicts(ctx, root.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(conflicts) != 1 || conflicts[0].ID != oldID {
		t.Fatalf("unchanged conflict was replaced or duplicated: %+v", conflicts)
	}
	data.remote["conflict.txt"] = remoteFileFP("doc-conflict", "rev-b", 4)
	third, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if third.Initialized || third.Conflicts != 1 {
		t.Fatalf("third result = %+v", third)
	}
	conflicts, err = state.ListConflicts(ctx, root.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(conflicts) != 1 || conflicts[0].ID == oldID || conflicts[0].Remote.Rev != "rev-b" {
		t.Fatalf("changed conflict did not refresh durable fingerprint: %+v", conflicts)
	}
	allConflicts, err := state.ListConflicts(ctx, root.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(allConflicts) != 2 || !allConflicts[0].Resolved || allConflicts[1].Resolved {
		t.Fatalf("conflict history after fingerprint refresh = %+v", allConflicts)
	}
}

func TestRunRootCycleAppliesQueuedConflictResolutionThroughDurableOperation(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, false)
	data := &fakeDataPlane{
		local: map[string]domain.LocalFingerprint{"conflict.txt": localFileFP(4, 40)},
		remote: map[string]domain.RemoteFingerprint{
			"conflict.txt": remoteFileFP("doc-conflict", "rev-a", 4),
		},
		contentEqual: map[string]bool{"conflict.txt": false},
	}
	if _, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{}); err != nil {
		t.Fatal(err)
	}
	conflicts, err := state.ListConflicts(ctx, root.ID, true)
	if err != nil || len(conflicts) != 1 {
		t.Fatalf("initial conflicts = %+v err=%v", conflicts, err)
	}
	op, err := OperationForConflictResolution(conflicts[0], ConflictKeepLocal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.CreateOperation(ctx, op); err != nil {
		t.Fatal(err)
	}

	result, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Recovered != 1 || !result.Initialized {
		t.Fatalf("resolution cycle result = %+v", result)
	}
	if operations, err := state.ListOperations(ctx, root.ID); err != nil || len(operations) != 0 {
		t.Fatalf("resolution operation remains = %+v err=%v", operations, err)
	}
	if conflicts, err := state.ListConflicts(ctx, root.ID, true); err != nil || len(conflicts) != 0 {
		t.Fatalf("resolved conflict remains unresolved = %+v err=%v", conflicts, err)
	}
	if got := data.remote["conflict.txt"]; got.Rev == "rev-a" || got.Size != 4 {
		t.Fatalf("keep-local did not replace remote conflict state: %+v", got)
	}
}

func TestRunRootCycleAppliesKeepRemoteConflictResolution(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, false)
	data := &fakeDataPlane{
		local: map[string]domain.LocalFingerprint{"conflict.txt": localFileFP(4, 40)},
		remote: map[string]domain.RemoteFingerprint{
			"conflict.txt": remoteFileFP("doc-conflict", "rev-a", 7),
		},
		contentEqual: map[string]bool{"conflict.txt": false},
	}
	if _, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{}); err != nil {
		t.Fatal(err)
	}
	conflicts, err := state.ListConflicts(ctx, root.ID, true)
	if err != nil || len(conflicts) != 1 {
		t.Fatalf("initial conflicts = %+v err=%v", conflicts, err)
	}
	op, err := OperationForConflictResolution(conflicts[0], ConflictKeepRemote)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.CreateOperation(ctx, op); err != nil {
		t.Fatal(err)
	}

	result, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Recovered != 1 || !result.Initialized {
		t.Fatalf("keep-remote resolution cycle result = %+v", result)
	}
	if got := data.local["conflict.txt"]; !got.Present || got.Kind != domain.KindFile || got.Size != 7 {
		t.Fatalf("keep-remote did not replace local conflict state: %+v", got)
	}
	if conflicts, err := state.ListConflicts(ctx, root.ID, true); err != nil || len(conflicts) != 0 {
		t.Fatalf("keep-remote conflict remains unresolved = %+v err=%v", conflicts, err)
	}
}

func TestRunRootCycleDropsStaleConflictResolutionAndRefreshesConflict(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, false)
	data := &fakeDataPlane{
		local: map[string]domain.LocalFingerprint{"conflict.txt": localFileFP(4, 40)},
		remote: map[string]domain.RemoteFingerprint{
			"conflict.txt": remoteFileFP("doc-conflict", "rev-a", 4),
		},
		contentEqual: map[string]bool{"conflict.txt": false},
	}
	if _, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{}); err != nil {
		t.Fatal(err)
	}
	conflicts, err := state.ListConflicts(ctx, root.ID, true)
	if err != nil || len(conflicts) != 1 {
		t.Fatalf("initial conflicts = %+v err=%v", conflicts, err)
	}
	oldConflictID := conflicts[0].ID
	op, err := OperationForConflictResolution(conflicts[0], ConflictKeepLocal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.CreateOperation(ctx, op); err != nil {
		t.Fatal(err)
	}

	data.remote["conflict.txt"] = remoteFileFP("doc-conflict", "rev-b", 4)
	beforeCalls := len(data.calls)
	result, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Recovered != 0 || result.Conflicts != 1 {
		t.Fatalf("stale resolution cycle result = %+v", result)
	}
	for _, call := range data.calls[beforeCalls:] {
		if strings.HasPrefix(call, "upload:") || strings.HasPrefix(call, "ensure-") || strings.HasPrefix(call, "delete-") {
			t.Fatalf("stale resolution mutated data: calls=%v", data.calls[beforeCalls:])
		}
	}
	if got := data.remote["conflict.txt"]; got.Rev != "rev-b" {
		t.Fatalf("stale resolution overwrote newer remote state: %+v", got)
	}
	if operations, err := state.ListOperations(ctx, root.ID); err != nil || len(operations) != 0 {
		t.Fatalf("stale resolution operation remains = %+v err=%v", operations, err)
	}
	conflicts, err = state.ListConflicts(ctx, root.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(conflicts) != 1 || conflicts[0].ID == oldConflictID || conflicts[0].Remote.Rev != "rev-b" {
		t.Fatalf("stale conflict was not refreshed: %+v", conflicts)
	}
}

func TestRunRootCycleDropsStalePlannedIntentThenReplans(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, false)
	stale := domain.Operation{
		SyncRootID:     root.ID,
		Kind:           domain.OperationEnsureRemote,
		EntryKind:      domain.KindFile,
		SrcPath:        "a.txt",
		ExpectedLocal:  localFileFP(1, 10),
		ExpectedRemote: domain.RemoteExpectation{Absent: true},
	}
	if _, err := state.CreateOperation(ctx, stale); err != nil {
		t.Fatal(err)
	}
	data := &fakeDataPlane{
		local:  map[string]domain.LocalFingerprint{"a.txt": localFileFP(2, 20)},
		remote: make(map[string]domain.RemoteFingerprint),
	}

	result, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Initialized || result.Applied != 1 {
		t.Fatalf("cycle result = %+v", result)
	}
	if got := strings.Join(data.calls, ","); got != "upload:a.txt" {
		t.Fatalf("stale planned operation was replayed or extra mutation occurred: %q", got)
	}
	baseline, ok, err := state.GetBaseline(ctx, root.ID, "a.txt")
	if err != nil || !ok {
		t.Fatalf("baseline = %+v ok=%v err=%v", baseline, ok, err)
	}
	if baseline.Local != localFileFP(2, 20) {
		t.Fatalf("baseline committed stale local state: %+v", baseline.Local)
	}
}

func TestRunRootCycleRecoversAlreadyCompletedRunningDeleteWithoutReplay(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, true)
	baseline := domain.Baseline{
		SyncRootID: root.ID,
		RelPath:    "gone.txt",
		Local:      localFileFP(3, 30),
		Remote:     remoteFileFP("doc-gone", "rev-gone", 3),
	}
	if err := state.PutBaseline(ctx, baseline); err != nil {
		t.Fatal(err)
	}
	op, err := state.CreateOperation(ctx, domain.Operation{
		SyncRootID:     root.ID,
		Kind:           domain.OperationDeleteRemote,
		EntryKind:      domain.KindFile,
		SrcPath:        "gone.txt",
		ExpectedLocal:  domain.LocalFingerprint{},
		ExpectedRemote: domain.RemoteExpectation{ID: "doc-gone", Rev: "rev-gone"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.SetOperationPhase(ctx, op.ID, domain.OperationRunning, "", true); err != nil {
		t.Fatal(err)
	}
	data := &fakeDataPlane{local: make(map[string]domain.LocalFingerprint), remote: make(map[string]domain.RemoteFingerprint)}

	result, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{MaxCount: 10, MaxFraction: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.Recovered != 1 || result.Applied != 0 || result.Blocked {
		t.Fatalf("cycle result = %+v", result)
	}
	if len(data.calls) != 0 {
		t.Fatalf("already-completed running delete was replayed: %+v", data.calls)
	}
	if _, ok, err := state.GetBaseline(ctx, root.ID, "gone.txt"); err != nil || ok {
		t.Fatalf("deleted baseline remains: ok=%v err=%v", ok, err)
	}
	if operations, err := state.ListOperations(ctx, root.ID); err != nil || len(operations) != 0 {
		t.Fatalf("recovered operation remains: %+v err=%v", operations, err)
	}
}

func TestRunRootCycleBlocksUnsafeWindowsNamespaceBeforePlanning(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, false)
	data := &fakeDataPlane{
		local: map[string]domain.LocalFingerprint{
			"Foo.txt": localFileFP(3, 30),
		},
		remote: map[string]domain.RemoteFingerprint{
			"foo.txt": remoteFileFP("doc-foo", "rev-foo", 3),
		},
	}

	result, err := runRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{}, "windows")
	if err != nil {
		t.Fatal(err)
	}
	if !result.Blocked || result.BlockReason != string(reconcile.BlockNamespaceUnsafe) || !strings.Contains(result.BlockDetail, "namespace collision") {
		t.Fatalf("cycle result = %+v", result)
	}
	if len(data.calls) != 0 {
		t.Fatalf("unsafe namespace reached mutation plane: %+v", data.calls)
	}
	if rootState, ok, err := state.GetSyncRoot(ctx, root.ID); err != nil || !ok || rootState.Initialized {
		t.Fatalf("unsafe namespace changed root initialization: root=%+v ok=%v err=%v", rootState, ok, err)
	}
}

func TestRunRootCycleBlocksUnsafeNamespaceBeforeOperationRecovery(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, true)
	planned := domain.Operation{
		SyncRootID:     root.ID,
		Kind:           domain.OperationEnsureRemote,
		EntryKind:      domain.KindFile,
		SrcPath:        "foo.txt",
		ExpectedLocal:  localFileFP(3, 30),
		ExpectedRemote: domain.RemoteExpectation{Absent: true},
	}
	if _, err := state.CreateOperation(ctx, planned); err != nil {
		t.Fatal(err)
	}
	data := &fakeDataPlane{
		local: map[string]domain.LocalFingerprint{
			"foo.txt": localFileFP(3, 30),
		},
		remote: map[string]domain.RemoteFingerprint{
			"FOO.TXT": remoteFileFP("doc-other", "rev-other", 7),
		},
	}

	result, err := runRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{}, "windows")
	if err != nil {
		t.Fatal(err)
	}
	if !result.Blocked || result.BlockReason != string(reconcile.BlockNamespaceUnsafe) || result.Recovered != 0 {
		t.Fatalf("cycle result = %+v", result)
	}
	if len(data.calls) != 0 {
		t.Fatalf("namespace-unsafe recovery mutated data: %+v", data.calls)
	}
	operations, err := state.ListOperations(ctx, root.ID)
	if err != nil || len(operations) != 1 || operations[0].Phase != domain.OperationPlanned {
		t.Fatalf("planned operation changed despite namespace block: %+v err=%v", operations, err)
	}
}

func TestRunRootCycleBlocksWhenPinnedLocalRecoveryArtifactExists(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, true)
	target := filepath.Join(t.TempDir(), "outside-target.txt")
	op, err := state.CreateOperation(ctx, domain.Operation{
		SyncRootID:      root.ID,
		Kind:            domain.OperationDeleteLocal,
		EntryKind:       domain.KindFile,
		SrcPath:         "linked.txt",
		LocalTargetPath: target,
		ExpectedLocal:   localFileFP(4, 40),
		ExpectedRemote:  domain.RemoteExpectation{Absent: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.SetOperationPhase(ctx, op.ID, domain.OperationRunning, "", true); err != nil {
		t.Fatal(err)
	}
	artifact := filepath.Join(filepath.Dir(target), ".pkudisk-sync-tmp-op-recovery")
	base := &fakeDataPlane{
		local:  map[string]domain.LocalFingerprint{"linked.txt": localFileFP(4, 40)},
		remote: make(map[string]domain.RemoteFingerprint),
	}
	data := &recoveryArtifactDataPlane{fakeDataPlane: base, artifact: artifact}

	result, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{MaxCount: 10, MaxFraction: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Blocked || result.BlockReason != "operation-recovery-blocked" || result.Recovered != 0 {
		t.Fatalf("cycle result = %+v", result)
	}
	if len(base.calls) != 0 {
		t.Fatalf("recovery artifact allowed local mutation replay: %+v", base.calls)
	}
	operations, err := state.ListOperations(ctx, root.ID)
	if err != nil || len(operations) != 1 {
		t.Fatalf("operations = %+v err=%v", operations, err)
	}
	if operations[0].Phase != domain.OperationBlocked ||
		!strings.Contains(operations[0].LastError, "local data preserved at recovery artifact") ||
		!strings.Contains(operations[0].LastError, filepath.Base(artifact)) {
		t.Fatalf("recovery artifact was not retained as blocked authority: %+v", operations[0])
	}
}

func TestRunRootCycleMassDeleteGateHasNoSideEffects(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, true)
	baseline := domain.Baseline{
		SyncRootID: root.ID,
		RelPath:    "keep-remote.txt",
		Local:      localFileFP(3, 30),
		Remote:     remoteFileFP("doc-keep", "rev-keep", 3),
	}
	if err := state.PutBaseline(ctx, baseline); err != nil {
		t.Fatal(err)
	}
	data := &fakeDataPlane{
		local: make(map[string]domain.LocalFingerprint),
		remote: map[string]domain.RemoteFingerprint{
			"keep-remote.txt": baseline.Remote,
		},
	}

	result, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Blocked || result.BlockReason != string(reconcile.BlockDeletePolicyUnset) {
		t.Fatalf("cycle result = %+v", result)
	}
	if len(data.calls) != 0 {
		t.Fatalf("blocked delete plan mutated data: %+v", data.calls)
	}
	if operations, err := state.ListOperations(ctx, root.ID); err != nil || len(operations) != 0 {
		t.Fatalf("blocked plan journaled operations: %+v err=%v", operations, err)
	}
}

func TestRunRootCycleDeletesChildrenBeforeParentDirectory(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, true)
	dirRemote := remoteDirFP("dir-doc")
	fileRemote := remoteFileFP("file-doc", "file-rev", 2)
	for _, baseline := range []domain.Baseline{
		{SyncRootID: root.ID, RelPath: "docs", Local: localDirFP(), Remote: dirRemote},
		{SyncRootID: root.ID, RelPath: "docs/a.txt", Local: localFileFP(2, 20), Remote: fileRemote},
	} {
		if err := state.PutBaseline(ctx, baseline); err != nil {
			t.Fatal(err)
		}
	}
	data := &fakeDataPlane{
		local: make(map[string]domain.LocalFingerprint),
		remote: map[string]domain.RemoteFingerprint{
			"docs":       dirRemote,
			"docs/a.txt": fileRemote,
		},
	}

	result, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{MaxCount: 10, MaxFraction: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.Blocked || result.Applied != 2 {
		t.Fatalf("cycle result = %+v", result)
	}
	if got := strings.Join(data.calls, ","); got != "delete-remote-file:docs/a.txt,delete-remote-dir:docs" {
		t.Fatalf("delete order = %q", got)
	}
}

func TestRunRootCycleMissingMarkerBlocksBeforeScan(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, false, false)
	data := &fakeDataPlane{local: make(map[string]domain.LocalFingerprint), remote: make(map[string]domain.RemoteFingerprint)}

	result, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Blocked || result.BlockReason != string(reconcile.BlockRootUnhealthy) || result.BlockDetail == "" {
		t.Fatalf("cycle result = %+v", result)
	}
	if data.scans != 0 || len(data.calls) != 0 {
		t.Fatalf("unhealthy root reached data plane: scans=%d calls=%+v", data.scans, data.calls)
	}
}

func TestRunRootCycleBlocksChangedFollowedDirectoryIdentity(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, false)
	if err := state.InitializeSyncRoot(ctx, root.ID, map[string]string{"link": "linux:1:1"}); err != nil {
		t.Fatal(err)
	}
	for _, baseline := range []domain.Baseline{
		{SyncRootID: root.ID, RelPath: "link", Local: localDirFP(), Remote: remoteDirFP("dir-link")},
		{SyncRootID: root.ID, RelPath: "link/a.txt", Local: localFileFP(3, 30), Remote: remoteFileFP("doc-a", "rev-a", 3)},
	} {
		if err := state.PutBaseline(ctx, baseline); err != nil {
			t.Fatal(err)
		}
	}
	data := &fakeDataPlane{
		local:               map[string]domain.LocalFingerprint{"link": localDirFP()},
		remote:              map[string]domain.RemoteFingerprint{"link": remoteDirFP("dir-link"), "link/a.txt": remoteFileFP("doc-a", "rev-a", 3)},
		followedDirectories: map[string]string{"link": "linux:2:2"},
	}
	result, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{MaxCount: 10, MaxFraction: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Blocked || result.BlockReason != "followed-directory-identity-changed" || !strings.Contains(result.BlockDetail, "changed physical identity") {
		t.Fatalf("cycle result = %+v", result)
	}
	if len(data.calls) != 0 {
		t.Fatalf("identity change reached mutation path: %+v", data.calls)
	}
	if _, ok, err := state.GetBaseline(ctx, root.ID, "link/a.txt"); err != nil || !ok {
		t.Fatalf("identity change altered descendant baseline: ok=%v err=%v", ok, err)
	}
}

func TestRunRootCycleKeepsUnavailableFollowedDirectoryNonAuthoritative(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, false)
	if err := state.InitializeSyncRoot(ctx, root.ID, map[string]string{"link": "linux:1:1"}); err != nil {
		t.Fatal(err)
	}
	baseline := domain.Baseline{
		SyncRootID: root.ID,
		RelPath:    "link/a.txt",
		Local:      localFileFP(3, 30),
		Remote:     remoteFileFP("doc-a", "rev-a", 3),
	}
	if err := state.PutBaseline(ctx, baseline); err != nil {
		t.Fatal(err)
	}
	data := &fakeDataPlane{
		local:    make(map[string]domain.LocalFingerprint),
		remote:   map[string]domain.RemoteFingerprint{"link/a.txt": baseline.Remote},
		excluded: []string{"link"},
	}
	result, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{MaxCount: 10, MaxFraction: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.Blocked || len(data.calls) != 0 {
		t.Fatalf("unavailable followed boundary gained authority: result=%+v calls=%+v", result, data.calls)
	}
	if _, ok, err := state.GetBaseline(ctx, root.ID, "link/a.txt"); err != nil || !ok {
		t.Fatalf("excluded boundary changed baseline: ok=%v err=%v", ok, err)
	}
}

func TestRunRootCycleBlocksNewFollowedDirectoryAfterInitialization(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, true)
	data := &fakeDataPlane{
		local:               map[string]domain.LocalFingerprint{"link": localDirFP()},
		remote:              map[string]domain.RemoteFingerprint{"link": remoteDirFP("dir-link")},
		followedDirectories: map[string]string{"link": "linux:1:1"},
	}
	result, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{MaxCount: 10, MaxFraction: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Blocked || result.BlockReason != "followed-directory-identity-changed" || !strings.Contains(result.BlockDetail, "no durable physical identity") {
		t.Fatalf("cycle result = %+v", result)
	}
	if len(data.calls) != 0 {
		t.Fatalf("new followed boundary reached mutation path: %+v", data.calls)
	}
}

func TestRunRootCycleCleansStaleRecoveryAfterProvenPostcondition(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, true)
	baseline := domain.Baseline{
		SyncRootID: root.ID,
		RelPath:    "gone.txt",
		Local:      localFileFP(4, 40),
		Remote:     remoteFileFP("doc-gone", "rev-gone", 4),
	}
	if err := state.PutBaseline(ctx, baseline); err != nil {
		t.Fatal(err)
	}
	op, err := state.CreateOperation(ctx, domain.Operation{
		SyncRootID:      root.ID,
		Kind:            domain.OperationDeleteLocal,
		EntryKind:       domain.KindFile,
		SrcPath:         "gone.txt",
		LocalTargetPath: filepath.Join(root.LocalRoot, "gone.txt"),
		ExpectedLocal:   baseline.Local,
		ExpectedRemote:  domain.RemoteExpectation{Absent: true},
		Phase:           domain.OperationRecovering,
		Attempts:        1,
	})
	if err != nil {
		t.Fatal(err)
	}
	data := &recoveryArtifactDataPlane{
		fakeDataPlane: &fakeDataPlane{local: make(map[string]domain.LocalFingerprint), remote: make(map[string]domain.RemoteFingerprint)},
		artifact:      filepath.Join(root.LocalRoot, ".pkudisk-sync-tmp-op-stale-recovery"),
	}
	result, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{MaxCount: 10, MaxFraction: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.Blocked || result.Recovered != 1 || !data.cleaned {
		t.Fatalf("stale recovery did not auto-complete: result=%+v cleaned=%v", result, data.cleaned)
	}
	if _, ok, err := state.GetOperation(ctx, op.ID); err != nil || ok {
		t.Fatalf("completed recovery operation remains: ok=%v err=%v", ok, err)
	}
	if _, ok, err := state.GetBaseline(ctx, root.ID, "gone.txt"); err != nil || ok {
		t.Fatalf("completed delete-local baseline remains: ok=%v err=%v", ok, err)
	}
}

func newCycleRoot(t *testing.T, marker, initialized bool) (*store.Store, domain.SyncRoot) {
	t.Helper()
	ctx := context.Background()
	base := t.TempDir()
	localRoot := filepath.Join(base, "root")
	if err := os.Mkdir(localRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	state, err := store.Open(ctx, filepath.Join(base, "state.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	root := domain.SyncRoot{
		UUID:                "cycle-root-uuid",
		LocalRoot:           localRoot,
		RemoteName:          "pkudisk",
		RemoteRoot:          "Personal/Sync",
		Enabled:             true,
		PollIntervalSeconds: 60,
	}
	root, err = state.CreateSyncRoot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if marker {
		if err := rootmarker.Ensure(root.LocalRoot, root.UUID); err != nil {
			t.Fatal(err)
		}
	}
	if initialized {
		if err := state.MarkSyncRootInitialized(ctx, root.ID); err != nil {
			t.Fatal(err)
		}
		root.Initialized = true
	}
	return state, root
}

func assertRootInitialized(t *testing.T, state *store.Store, rootID int64, want bool) {
	t.Helper()
	root, ok, err := state.GetSyncRoot(context.Background(), rootID)
	if err != nil || !ok {
		t.Fatalf("GetSyncRoot() = %+v ok=%v err=%v", root, ok, err)
	}
	if root.Initialized != want {
		t.Fatalf("root initialized = %v, want %v", root.Initialized, want)
	}
}

type fakeDataPlane struct {
	local                map[string]domain.LocalFingerprint
	remote               map[string]domain.RemoteFingerprint
	followedDirectories  map[string]string
	excluded             []string
	remoteRootMissing    bool
	contentEqual         map[string]bool
	calls                []string
	scans                int
	revCounter           int
	uploadPostOverride   map[string]domain.RemoteFingerprint
	downloadPostOverride map[string]domain.LocalFingerprint
}

type cancelUploadDataPlane struct {
	*fakeDataPlane
	started chan struct{}
}

type recoveryArtifactDataPlane struct {
	*fakeDataPlane
	artifact string
	cleaned  bool
}

func (f *recoveryArtifactDataPlane) LocalRecoveryArtifact(context.Context, domain.Operation) (string, bool, error) {
	return f.artifact, true, nil
}

func (f *recoveryArtifactDataPlane) CleanupLocalRecoveryArtifact(context.Context, domain.Operation) error {
	f.cleaned = true
	return nil
}

func (f *cancelUploadDataPlane) Upload(ctx context.Context, _ string, _ domain.LocalFingerprint, _ domain.RemoteExpectation) (domain.RemoteFingerprint, error) {
	close(f.started)
	<-ctx.Done()
	return domain.RemoteFingerprint{}, ctx.Err()
}

func (f *fakeDataPlane) ScanLocal(context.Context, []domain.Operation, []string) (map[string]domain.LocalFingerprint, []string, map[string]string, error) {
	f.scans++
	return cloneLocal(f.local), append([]string(nil), f.excluded...), cloneStringsMap(f.followedDirectories), nil
}

func (f *fakeDataPlane) ScanRemote(context.Context) (map[string]domain.RemoteFingerprint, bool, error) {
	f.scans++
	return cloneRemote(f.remote), !f.remoteRootMissing, nil
}

func (f *fakeDataPlane) ObserveLocalEntry(_ context.Context, rel string) (domain.LocalFingerprint, error) {
	return f.local[rel], nil
}

func (f *fakeDataPlane) ObserveRemoteEntry(_ context.Context, rel string) (domain.RemoteFingerprint, error) {
	return f.remote[rel], nil
}

func (f *fakeDataPlane) ResolveLocalMutationTarget(_ context.Context, rel string, expected domain.LocalFingerprint) (string, error) {
	if !domain.LocalEquivalent(f.local[rel], expected) {
		return "", fmt.Errorf("local target precondition mismatch for %q", rel)
	}
	return filepath.Join(os.TempDir(), "pkudisk-sync-fake", filepath.FromSlash(rel)), nil
}

func (f *fakeDataPlane) LocalRecoveryArtifact(context.Context, domain.Operation) (string, bool, error) {
	return "", false, nil
}

func (f *fakeDataPlane) CleanupLocalRecoveryArtifact(context.Context, domain.Operation) error {
	return nil
}

func (f *fakeDataPlane) CompareFileContent(_ context.Context, rel string, local domain.LocalFingerprint, remote domain.RemoteExpectation) (bool, error) {
	if !domain.LocalEquivalent(f.local[rel], local) || !fakeRemoteMatches(f.remote[rel], remote, domain.KindFile) {
		return false, fmt.Errorf("compare precondition mismatch for %q", rel)
	}
	f.calls = append(f.calls, "compare:"+rel)
	return f.contentEqual[rel], nil
}

func (f *fakeDataPlane) Upload(_ context.Context, rel string, local domain.LocalFingerprint, remote domain.RemoteExpectation) (domain.RemoteFingerprint, error) {
	if !domain.LocalEquivalent(f.local[rel], local) || !fakeRemoteMatches(f.remote[rel], remote, domain.KindFile) {
		return domain.RemoteFingerprint{}, fmt.Errorf("upload precondition mismatch for %q", rel)
	}
	f.calls = append(f.calls, "upload:"+rel)
	f.remoteRootMissing = false
	f.revCounter++
	id := remote.ID
	if remote.Absent {
		id = "remote:" + rel
	}
	written := remoteFileFP(id, fmt.Sprintf("rev-%d", f.revCounter), local.Size)
	f.remote[rel] = written
	converged := true
	if override, ok := f.uploadPostOverride[rel]; ok {
		f.remote[rel] = override
		converged = false
	}
	if f.contentEqual == nil {
		f.contentEqual = make(map[string]bool)
	}
	f.contentEqual[rel] = converged
	return written, nil
}

func (f *fakeDataPlane) EnsureRemoteDir(_ context.Context, rel string, expected domain.RemoteExpectation) error {
	if !fakeRemoteMatches(f.remote[rel], expected, domain.KindDir) {
		return fmt.Errorf("remote dir precondition mismatch for %q", rel)
	}
	f.calls = append(f.calls, "ensure-remote-dir:"+rel)
	f.remoteRootMissing = false
	f.remote[rel] = remoteDirFP("dir:" + rel)
	return nil
}

func (f *fakeDataPlane) EnsureLocalFile(_ context.Context, op domain.Operation) (domain.LocalFingerprint, error) {
	rel := op.SrcPath
	if !domain.LocalEquivalent(f.local[rel], op.ExpectedLocal) || !fakeRemoteMatches(f.remote[rel], op.ExpectedRemote, domain.KindFile) {
		return domain.LocalFingerprint{}, fmt.Errorf("local file precondition mismatch for %q", rel)
	}
	f.calls = append(f.calls, "ensure-local-file:"+rel)
	remote := f.remote[rel]
	written := localFileFP(remote.Size, remote.MtimeUS*1000+1)
	f.local[rel] = written
	converged := true
	if override, ok := f.downloadPostOverride[rel]; ok {
		f.local[rel] = override
		converged = false
	}
	if f.contentEqual == nil {
		f.contentEqual = make(map[string]bool)
	}
	f.contentEqual[rel] = converged
	return written, nil
}

func (f *fakeDataPlane) EnsureLocalDir(_ context.Context, op domain.Operation) error {
	rel := op.SrcPath
	expected := op.ExpectedLocal
	if !domain.LocalEquivalent(f.local[rel], expected) || expected.Present {
		return fmt.Errorf("local dir precondition mismatch for %q", rel)
	}
	f.calls = append(f.calls, "ensure-local-dir:"+rel)
	f.local[rel] = localDirFP()
	return nil
}

func (f *fakeDataPlane) DeleteRemoteFile(_ context.Context, expected domain.RemoteExpectation) error {
	rel, current, ok := f.findRemoteByID(expected.ID)
	if !ok || !fakeRemoteMatches(current, expected, domain.KindFile) {
		return fmt.Errorf("remote file delete precondition mismatch for %q", expected.ID)
	}
	f.calls = append(f.calls, "delete-remote-file:"+rel)
	delete(f.remote, rel)
	return nil
}

func (f *fakeDataPlane) DeleteRemoteDir(_ context.Context, expected domain.RemoteExpectation) error {
	rel, current, ok := f.findRemoteByID(expected.ID)
	if !ok || !fakeRemoteMatches(current, expected, domain.KindDir) {
		return fmt.Errorf("remote dir delete precondition mismatch for %q", expected.ID)
	}
	prefix := rel + "/"
	for child := range f.remote {
		if strings.HasPrefix(child, prefix) {
			return fmt.Errorf("remote dir %q is not empty", rel)
		}
	}
	f.calls = append(f.calls, "delete-remote-dir:"+rel)
	delete(f.remote, rel)
	return nil
}

func (f *fakeDataPlane) DeleteLocal(_ context.Context, op domain.Operation) error {
	rel := op.SrcPath
	expected := op.ExpectedLocal
	if !domain.LocalEquivalent(f.local[rel], expected) {
		return fmt.Errorf("local delete precondition mismatch for %q", rel)
	}
	if expected.Kind == domain.KindDir {
		prefix := rel + "/"
		for child := range f.local {
			if strings.HasPrefix(child, prefix) {
				return fmt.Errorf("local dir %q is not empty", rel)
			}
		}
	}
	f.calls = append(f.calls, "delete-local:"+rel)
	delete(f.local, rel)
	return nil
}

func (f *fakeDataPlane) findRemoteByID(id string) (string, domain.RemoteFingerprint, bool) {
	for rel, remote := range f.remote {
		if remote.ID == id {
			return rel, remote, true
		}
	}
	return "", domain.RemoteFingerprint{}, false
}

func cloneLocal(in map[string]domain.LocalFingerprint) map[string]domain.LocalFingerprint {
	out := make(map[string]domain.LocalFingerprint, len(in))
	for rel, fp := range in {
		out[rel] = fp
	}
	return out
}

func cloneStringsMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneRemote(in map[string]domain.RemoteFingerprint) map[string]domain.RemoteFingerprint {
	out := make(map[string]domain.RemoteFingerprint, len(in))
	for rel, fp := range in {
		out[rel] = fp
	}
	return out
}

func fakeRemoteMatches(current domain.RemoteFingerprint, expected domain.RemoteExpectation, kind domain.EntryKind) bool {
	if expected.Absent {
		return !current.Present
	}
	if !current.Present || current.Kind != kind || current.ID != expected.ID {
		return false
	}
	if kind == domain.KindFile {
		return current.Rev == expected.Rev
	}
	return expected.Rev == ""
}

func localFileFP(size, mtime int64) domain.LocalFingerprint {
	return domain.LocalFingerprint{Present: true, Kind: domain.KindFile, Size: size, MtimeNS: mtime}
}

func localDirFP() domain.LocalFingerprint {
	return domain.LocalFingerprint{Present: true, Kind: domain.KindDir}
}

func remoteFileFP(id, rev string, size int64) domain.RemoteFingerprint {
	return domain.RemoteFingerprint{Present: true, Kind: domain.KindFile, ID: id, Rev: rev, Size: size, MtimeUS: size + 100}
}

func remoteDirFP(id string) domain.RemoteFingerprint {
	return domain.RemoteFingerprint{Present: true, Kind: domain.KindDir, ID: id, MtimeUS: 100}
}

func TestRunRootCycleDoesNotCommitUploadIfRemoteChangesAfterCopy(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, false)
	data := &fakeDataPlane{
		local:  map[string]domain.LocalFingerprint{"race.txt": localFileFP(4, 40)},
		remote: make(map[string]domain.RemoteFingerprint),
		uploadPostOverride: map[string]domain.RemoteFingerprint{
			"race.txt": remoteFileFP("remote:race.txt", "third-party-rev", 4),
		},
	}

	result, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{})
	if err == nil {
		t.Fatalf("concurrent post-upload remote change was accepted: result=%+v", result)
	}
	if _, ok, getErr := state.GetBaseline(ctx, root.ID, "race.txt"); getErr != nil || ok {
		t.Fatalf("baseline committed despite post-upload race: ok=%v err=%v", ok, getErr)
	}
	operations, listErr := state.ListOperations(ctx, root.ID)
	if listErr != nil {
		t.Fatal(listErr)
	}
	if len(operations) != 1 || operations[0].Phase != domain.OperationRecovering {
		t.Fatalf("post-upload race did not leave recoverable intent: %+v", operations)
	}
	if operations[0].Attempts != 1 {
		t.Fatalf("attempt count = %d, want 1", operations[0].Attempts)
	}
}

func TestRunRootCycleDoesNotCommitDownloadIfLocalChangesAfterReplace(t *testing.T) {
	ctx := context.Background()
	state, root := newCycleRoot(t, true, true)
	data := &fakeDataPlane{
		local:  make(map[string]domain.LocalFingerprint),
		remote: map[string]domain.RemoteFingerprint{"race.txt": remoteFileFP("remote:race.txt", "rev-1", 4)},
		downloadPostOverride: map[string]domain.LocalFingerprint{
			"race.txt": localFileFP(4, 999999),
		},
	}

	result, err := RunRootCycle(ctx, root.ID, state, data, reconcile.DeletePolicy{})
	if err == nil {
		t.Fatalf("concurrent post-download local change was accepted: result=%+v", result)
	}
	if _, ok, getErr := state.GetBaseline(ctx, root.ID, "race.txt"); getErr != nil || ok {
		t.Fatalf("baseline committed despite post-download race: ok=%v err=%v", ok, getErr)
	}
	operations, listErr := state.ListOperations(ctx, root.ID)
	if listErr != nil {
		t.Fatal(listErr)
	}
	if len(operations) != 1 || operations[0].Phase != domain.OperationRecovering {
		t.Fatalf("post-download race did not leave recoverable intent: %+v", operations)
	}
	if operations[0].Attempts != 1 {
		t.Fatalf("attempt count = %d, want 1", operations[0].Attempts)
	}
}
