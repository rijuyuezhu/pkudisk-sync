package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

func TestOpenCreatesSchemaAndSyncRootsRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	var version int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version = %d, want %d", version, schemaVersion)
	}

	root := createTestRoot(t, s)
	got, ok, err := s.GetSyncRoot(ctx, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("created sync root not found")
	}
	if got.UUID != root.UUID || got.LocalRoot != root.LocalRoot || got.RemoteName != root.RemoteName || got.RemoteRoot != root.RemoteRoot || got.Enabled != root.Enabled || got.Initialized != root.Initialized || got.PollIntervalSeconds != root.PollIntervalSeconds {
		t.Fatalf("sync root round trip mismatch: got %+v want %+v", got, root)
	}

	roots, err := s.ListSyncRoots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 1 || roots[0].ID != root.ID {
		t.Fatalf("ListSyncRoots() = %+v", roots)
	}
}

func TestDeleteSyncRootRequiresPausedIdleRootAndCascadesState(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	root := createTestRoot(t, s)
	if err := s.DeleteSyncRoot(ctx, root.ID); err == nil {
		t.Fatal("enabled sync root was removed")
	}
	if err := s.SetSyncRootEnabled(ctx, root.ID, false); err != nil {
		t.Fatal(err)
	}
	op, err := s.CreateOperation(ctx, domain.Operation{
		SyncRootID:     root.ID,
		Kind:           domain.OperationEnsureRemote,
		EntryKind:      domain.KindFile,
		SrcPath:        "pending.txt",
		ExpectedLocal:  localFile(1, 1),
		ExpectedRemote: domain.RemoteExpectation{Absent: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteSyncRoot(ctx, root.ID); err == nil {
		t.Fatal("sync root with pending operation was removed")
	}
	if err := s.DeleteOperation(ctx, op.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.PutBaseline(ctx, domain.Baseline{
		SyncRootID: root.ID,
		RelPath:    "kept.txt",
		Local:      localFile(1, 1),
		Remote:     remoteFile("doc", "rev", 1),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateConflict(ctx, domain.Conflict{
		SyncRootID: root.ID,
		RelPath:    "conflict.txt",
		Kind:       domain.ConflictBothModified,
		Local:      localFile(1, 1),
		Remote:     remoteFile("doc-conflict", "rev-conflict", 2),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteSyncRoot(ctx, root.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.GetSyncRoot(ctx, root.ID); err != nil || ok {
		t.Fatalf("removed root still present: ok=%v err=%v", ok, err)
	}
	if baselines, err := s.ListBaselines(ctx, root.ID); err != nil || len(baselines) != 0 {
		t.Fatalf("cascaded baselines = %+v err=%v", baselines, err)
	}
	if conflicts, err := s.ListConflicts(ctx, root.ID, false); err != nil || len(conflicts) != 0 {
		t.Fatalf("cascaded conflicts = %+v err=%v", conflicts, err)
	}
}

func TestBaselineRoundTripAndUpsert(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	root := createTestRoot(t, s)

	base := domain.Baseline{
		SyncRootID: root.ID,
		RelPath:    "docs/a.txt",
		Local:      localFile(10, 100),
		Remote:     remoteFile("doc-1", "rev-1", 10),
	}
	if err := s.PutBaseline(ctx, base); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.GetBaseline(ctx, root.ID, base.RelPath)
	if err != nil || !ok {
		t.Fatalf("GetBaseline() = %+v, %v, %v", got, ok, err)
	}
	assertBaselineEqual(t, got, base)

	base.Local = localFile(12, 200)
	base.Remote = remoteFile("doc-1", "rev-2", 12)
	if err := s.PutBaseline(ctx, base); err != nil {
		t.Fatal(err)
	}
	got, ok, err = s.GetBaseline(ctx, root.ID, base.RelPath)
	if err != nil || !ok {
		t.Fatalf("GetBaseline() after update = %+v, %v, %v", got, ok, err)
	}
	assertBaselineEqual(t, got, base)

	second := domain.Baseline{SyncRootID: root.ID, RelPath: "docs/sub", Local: localDir(), Remote: remoteDir("dir-1")}
	if err := s.PutBaseline(ctx, second); err != nil {
		t.Fatal(err)
	}
	all, err := s.ListBaselines(ctx, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all[0].RelPath != "docs/a.txt" || all[1].RelPath != "docs/sub" {
		t.Fatalf("ListBaselines() = %+v", all)
	}

	if err := s.PutBaseline(ctx, domain.Baseline{SyncRootID: root.ID, RelPath: "gone.txt"}); err == nil {
		t.Fatal("expected fully absent baseline to be rejected")
	}
}

func TestOperationRoundTripAndPhaseTransitions(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	root := createTestRoot(t, s)

	op := domain.Operation{
		SyncRootID:     root.ID,
		Kind:           domain.OperationEnsureRemote,
		EntryKind:      domain.KindFile,
		SrcPath:        "new.txt",
		ExpectedLocal:  localFile(3, 33),
		ExpectedRemote: domain.RemoteExpectation{Absent: true},
	}
	created, err := s.CreateOperation(ctx, op)
	if err != nil {
		t.Fatal(err)
	}
	if created.ID <= 0 || created.Phase != domain.OperationPlanned {
		t.Fatalf("created operation = %+v", created)
	}

	if err := s.SetOperationPhase(ctx, created.ID, domain.OperationRunning, "", true); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.GetOperation(ctx, created.ID)
	if err != nil || !ok {
		t.Fatalf("GetOperation() = %+v, %v, %v", got, ok, err)
	}
	if got.Phase != domain.OperationRunning || got.Attempts != 1 {
		t.Fatalf("running operation = %+v", got)
	}
	if got.ExpectedLocal != op.ExpectedLocal || got.ExpectedRemote != op.ExpectedRemote {
		t.Fatalf("operation expectations changed: %+v", got)
	}

	if err := s.SetOperationPhase(ctx, created.ID, domain.OperationRecovering, "outcome unknown", false); err != nil {
		t.Fatal(err)
	}
	operations, err := s.ListOperations(ctx, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(operations) != 1 || operations[0].Phase != domain.OperationRecovering || operations[0].LastError != "outcome unknown" || operations[0].Attempts != 1 {
		t.Fatalf("ListOperations() = %+v", operations)
	}
}

func TestDeleteOperationOnlyDeletesPlannedIntent(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	root := createTestRoot(t, s)
	op := domain.Operation{
		SyncRootID:     root.ID,
		Kind:           domain.OperationEnsureRemote,
		EntryKind:      domain.KindFile,
		SrcPath:        "new.txt",
		ExpectedLocal:  localFile(3, 33),
		ExpectedRemote: domain.RemoteExpectation{Absent: true},
	}
	planned, err := s.CreateOperation(ctx, op)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteOperation(ctx, planned.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.GetOperation(ctx, planned.ID); err != nil || ok {
		t.Fatalf("planned operation still exists or lookup failed: ok=%v err=%v", ok, err)
	}

	running, err := s.CreateOperation(ctx, op)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetOperationPhase(ctx, running.ID, domain.OperationRunning, "", true); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteOperation(ctx, running.ID); err == nil {
		t.Fatal("running operation was deleted through planned-only helper")
	}
	if got, ok, err := s.GetOperation(ctx, running.ID); err != nil || !ok || got.Phase != domain.OperationRunning {
		t.Fatalf("running operation changed after rejected delete: got=%+v ok=%v err=%v", got, ok, err)
	}
}

func TestCommitBaselineAndDeleteOperationIsAtomic(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	root := createTestRoot(t, s)

	oldBase := domain.Baseline{
		SyncRootID: root.ID,
		RelPath:    "a.txt",
		Local:      localFile(1, 10),
		Remote:     remoteFile("doc-1", "rev-1", 1),
	}
	if err := s.PutBaseline(ctx, oldBase); err != nil {
		t.Fatal(err)
	}
	op, err := s.CreateOperation(ctx, domain.Operation{
		SyncRootID:     root.ID,
		Kind:           domain.OperationEnsureRemote,
		EntryKind:      domain.KindFile,
		SrcPath:        "a.txt",
		ExpectedLocal:  localFile(2, 20),
		ExpectedRemote: domain.RemoteExpectation{ID: "doc-1", Rev: "rev-1"},
	})
	if err != nil {
		t.Fatal(err)
	}

	newBase := domain.Baseline{
		SyncRootID: root.ID,
		RelPath:    "a.txt",
		Local:      localFile(2, 20),
		Remote:     remoteFile("doc-1", "rev-2", 2),
	}
	if err := s.CommitBaselineAndDeleteOperation(ctx, newBase, op.ID+999); err == nil {
		t.Fatal("expected completion with missing operation to fail")
	}
	stillOld, ok, err := s.GetBaseline(ctx, root.ID, "a.txt")
	if err != nil || !ok {
		t.Fatalf("GetBaseline() after rollback = %+v, %v, %v", stillOld, ok, err)
	}
	assertBaselineEqual(t, stillOld, oldBase)
	if _, ok, err := s.GetOperation(ctx, op.ID); err != nil || !ok {
		t.Fatalf("operation should survive rollback: ok=%v err=%v", ok, err)
	}

	if err := s.CommitBaselineAndDeleteOperation(ctx, newBase, op.ID); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.GetBaseline(ctx, root.ID, "a.txt")
	if err != nil || !ok {
		t.Fatalf("GetBaseline() after completion = %+v, %v, %v", got, ok, err)
	}
	assertBaselineEqual(t, got, newBase)
	if _, ok, err := s.GetOperation(ctx, op.ID); err != nil || ok {
		t.Fatalf("completed operation still exists: ok=%v err=%v", ok, err)
	}
}

func TestDropBaselineAndDeleteOperation(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	root := createTestRoot(t, s)
	base := domain.Baseline{SyncRootID: root.ID, RelPath: "gone.txt", Local: localFile(1, 1), Remote: remoteFile("doc", "rev", 1)}
	if err := s.PutBaseline(ctx, base); err != nil {
		t.Fatal(err)
	}
	op, err := s.CreateOperation(ctx, domain.Operation{
		SyncRootID:     root.ID,
		Kind:           domain.OperationDeleteRemote,
		EntryKind:      domain.KindFile,
		SrcPath:        "gone.txt",
		ExpectedLocal:  domain.LocalFingerprint{},
		ExpectedRemote: domain.RemoteExpectation{ID: "doc", Rev: "rev"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DropBaselineAndDeleteOperation(ctx, root.ID, "gone.txt", op.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.GetBaseline(ctx, root.ID, "gone.txt"); err != nil || ok {
		t.Fatalf("dropped baseline still exists: ok=%v err=%v", ok, err)
	}
	if _, ok, err := s.GetOperation(ctx, op.ID); err != nil || ok {
		t.Fatalf("completed operation still exists: ok=%v err=%v", ok, err)
	}
}

func TestConflictRoundTripAndResolution(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	root := createTestRoot(t, s)

	created, err := s.CreateConflict(ctx, domain.Conflict{
		SyncRootID: root.ID,
		RelPath:    "a.txt",
		Kind:       domain.ConflictBothModified,
		Local:      localFile(2, 20),
		Remote:     remoteFile("doc", "rev-2", 2),
	})
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.GetConflict(ctx, created.ID)
	if err != nil || !ok || got.ID != created.ID || got.RelPath != created.RelPath || got.Resolved {
		t.Fatalf("GetConflict() = %+v ok=%v err=%v", got, ok, err)
	}
	if _, ok, err := s.GetConflict(ctx, created.ID+999); err != nil || ok {
		t.Fatalf("GetConflict(missing) ok=%v err=%v", ok, err)
	}
	unresolved, err := s.ListConflicts(ctx, root.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(unresolved) != 1 || unresolved[0].ID != created.ID || unresolved[0].Resolved {
		t.Fatalf("unresolved conflicts = %+v", unresolved)
	}

	if err := s.ResolveConflict(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	unresolved, err = s.ListConflicts(ctx, root.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(unresolved) != 0 {
		t.Fatalf("resolved conflict still listed unresolved: %+v", unresolved)
	}
	all, err := s.ListConflicts(ctx, root.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || !all[0].Resolved || all[0].ResolvedAt.IsZero() {
		t.Fatalf("resolved conflict = %+v", all)
	}
	got, ok, err = s.GetConflict(ctx, created.ID)
	if err != nil || !ok || !got.Resolved || got.ResolvedAt.IsZero() {
		t.Fatalf("GetConflict(resolved) = %+v ok=%v err=%v", got, ok, err)
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.sqlite3")
	s, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close() = %v", err)
		}
	})
	fixedNow := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	s.now = func() time.Time {
		fixedNow = fixedNow.Add(time.Nanosecond)
		return fixedNow
	}
	return s
}

func createTestRoot(t *testing.T, s *Store) domain.SyncRoot {
	t.Helper()
	root, err := s.CreateSyncRoot(context.Background(), domain.SyncRoot{
		UUID:                "root-uuid-1",
		LocalRoot:           "/tmp/pkudisk-sync-root",
		RemoteName:          "pkudisk",
		RemoteRoot:          "Personal/Sync",
		Enabled:             true,
		PollIntervalSeconds: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func localFile(size, mtime int64) domain.LocalFingerprint {
	return domain.LocalFingerprint{Present: true, Kind: domain.KindFile, Size: size, MtimeNS: mtime}
}

func remoteFile(id, rev string, size int64) domain.RemoteFingerprint {
	return domain.RemoteFingerprint{Present: true, Kind: domain.KindFile, ID: id, Rev: rev, Size: size}
}

func localDir() domain.LocalFingerprint {
	return domain.LocalFingerprint{Present: true, Kind: domain.KindDir}
}

func remoteDir(id string) domain.RemoteFingerprint {
	return domain.RemoteFingerprint{Present: true, Kind: domain.KindDir, ID: id}
}

func assertBaselineEqual(t *testing.T, got, want domain.Baseline) {
	t.Helper()
	if got != want {
		t.Fatalf("baseline mismatch:\n got: %+v\nwant: %+v", got, want)
	}
}

func TestMigrationV1ToV2KeepsExistingRootsUninitialized(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state-v1.sqlite3")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schemaV1); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO sync_roots(uuid, local_root, remote_name, remote_root, enabled, poll_interval_seconds, created_at_ns)
VALUES('old-root', '/tmp/old-root', 'pkudisk', 'Personal/Old', 1, 60, 1)`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA user_version = 1"); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	root, ok, err := s.GetSyncRoot(ctx, 1)
	if err != nil || !ok {
		t.Fatalf("GetSyncRoot() after migration = %+v, %v, %v", root, ok, err)
	}
	if root.Initialized {
		t.Fatal("pre-v2 root was incorrectly treated as fully initialized")
	}
	var version int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 2 {
		t.Fatalf("schema version after migration = %d, want 2", version)
	}
}
