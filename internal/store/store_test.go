package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
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

func TestOperationLocalTargetIsPinnedBeforeRunning(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	root := createTestRoot(t, s)
	target := filepath.Join(root.LocalRoot, "linked.txt")
	anchor := "test-anchor-linked"

	created, err := s.CreateOperation(ctx, domain.Operation{
		SyncRootID:     root.ID,
		Kind:           domain.OperationEnsureLocal,
		EntryKind:      domain.KindFile,
		SrcPath:        "linked.txt",
		ExpectedLocal:  domain.LocalFingerprint{},
		ExpectedRemote: domain.RemoteExpectation{ID: "doc", Rev: "rev"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if ok, detail, err := s.AuthorizeAndPinLocalMutation(ctx, created.ID, domain.LocalMutationTarget{Path: target, AnchorIdentity: anchor}); err != nil || !ok {
		t.Fatalf("authorize first local target = ok=%v detail=%q err=%v", ok, detail, err)
	}
	got, ok, err := s.GetOperation(ctx, created.ID)
	if err != nil || !ok {
		t.Fatalf("GetOperation() = %+v ok=%v err=%v", got, ok, err)
	}
	if got.LocalTargetPath != target {
		t.Fatalf("local target = %q, want %q", got.LocalTargetPath, target)
	}
	if got.LocalTargetIdentity != anchor {
		t.Fatalf("local target identity = %q, want %q", got.LocalTargetIdentity, anchor)
	}
	if ok, _, err := s.AuthorizeAndPinLocalMutation(ctx, created.ID, domain.LocalMutationTarget{Path: target + "-other", AnchorIdentity: anchor}); err == nil && ok {
		t.Fatal("operation local target was repinned")
	}
	if err := s.SetOperationPhase(ctx, created.ID, domain.OperationRunning, "", true); err != nil {
		t.Fatal(err)
	}
	if ok, _, err := s.AuthorizeAndPinLocalMutation(ctx, created.ID, domain.LocalMutationTarget{Path: target, AnchorIdentity: anchor}); err == nil && ok {
		t.Fatal("running operation accepted a local target update")
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

func TestRetryBlockedOperationOnlyTransitionsBlockedIntent(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	root := createTestRoot(t, s)
	blocked, err := s.CreateOperation(ctx, domain.Operation{
		SyncRootID:     root.ID,
		Kind:           domain.OperationDeleteRemote,
		EntryKind:      domain.KindFile,
		SrcPath:        "blocked.txt",
		ExpectedRemote: domain.RemoteExpectation{ID: "doc", Rev: "rev"},
		Phase:          domain.OperationBlocked,
		Attempts:       2,
		LastError:      "needs attention",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RetryBlockedOperation(ctx, blocked.ID, "manual retry"); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.GetOperation(ctx, blocked.ID)
	if err != nil || !ok {
		t.Fatalf("GetOperation() = %+v ok=%v err=%v", got, ok, err)
	}
	if got.Phase != domain.OperationRecovering || got.Attempts != 2 || got.LastError != "manual retry" {
		t.Fatalf("retried operation = %+v", got)
	}

	planned, err := s.CreateOperation(ctx, domain.Operation{
		SyncRootID:     root.ID,
		Kind:           domain.OperationDeleteRemote,
		EntryKind:      domain.KindFile,
		SrcPath:        "planned.txt",
		ExpectedRemote: domain.RemoteExpectation{ID: "doc2", Rev: "rev2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RetryBlockedOperation(ctx, planned.ID, "must fail"); err == nil {
		t.Fatal("RetryBlockedOperation accepted a non-blocked operation")
	}
	got, ok, err = s.GetOperation(ctx, planned.ID)
	if err != nil || !ok || got.Phase != domain.OperationPlanned {
		t.Fatalf("planned operation changed: %+v ok=%v err=%v", got, ok, err)
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
		LocalRoot:           filepath.Join(t.TempDir(), "pkudisk-sync-root"),
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

func TestMigrationV1ToCurrentKeepsExistingRootsUninitializedAndDefaultsSymlinkFollow(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	dbPath := filepath.Join(base, "state-v1.sqlite3")
	oldRoot := filepath.Join(base, "old-root")
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
VALUES('old-root', ?, 'pkudisk', 'Personal/Old', 1, 60, 1)`, oldRoot); err != nil {
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
	if root.EffectiveSymlinkMode() != domain.SymlinkFollow {
		t.Fatalf("migrated symlink mode = %q, want follow", root.EffectiveSymlinkMode())
	}
	var version int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version after migration = %d, want %d", version, schemaVersion)
	}
}

func TestMigrationV3ToCurrentAddsFollowedPhysicalClaimAuthority(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	dbPath := filepath.Join(base, "state-v3.sqlite3")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schemaV1); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	for _, statement := range []string{
		`ALTER TABLE sync_roots ADD COLUMN initialized INTEGER NOT NULL DEFAULT 0 CHECK (initialized IN (0, 1))`,
		`ALTER TABLE sync_roots ADD COLUMN symlink_mode TEXT NOT NULL DEFAULT 'follow' CHECK (symlink_mode IN ('follow', 'reject', 'ignore'))`,
		`ALTER TABLE operations ADD COLUMN local_target_path TEXT NOT NULL DEFAULT ''`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO sync_roots(uuid, local_root, remote_name, remote_root, enabled, initialized, symlink_mode, poll_interval_seconds, created_at_ns)
VALUES('v3-root', ?, 'pkudisk', 'Personal/V3', 1, 1, 'follow', 60, 1)`, filepath.Join(base, "root")); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA user_version = 3"); err != nil {
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
	boundaries, err := s.ListFollowedPhysicalClaims(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(boundaries) != 0 {
		t.Fatalf("v3 migration invented followed directory identities: %+v", boundaries)
	}
	var version int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version after v3 migration = %d, want %d", version, schemaVersion)
	}
}

func TestMigrationV6ToV7ExpandsCopyModeWithoutGuessingLegacyAuthority(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	dbPath := filepath.Join(base, "state-v6.sqlite3")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schemaV1); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	for _, statement := range []string{
		`ALTER TABLE sync_roots ADD COLUMN initialized INTEGER NOT NULL DEFAULT 0 CHECK (initialized IN (0, 1))`,
		`ALTER TABLE sync_roots ADD COLUMN symlink_mode TEXT NOT NULL DEFAULT 'follow' CHECK (symlink_mode IN ('follow', 'reject', 'ignore'))`,
		`ALTER TABLE operations ADD COLUMN local_target_path TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE operations ADD COLUMN local_target_identity TEXT NOT NULL DEFAULT ''`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	for _, row := range []struct {
		uuid, localRoot, remoteRoot, mode string
		enabled, initialized              int
	}{
		{"v6-follow", filepath.Join(base, "follow"), "Personal/V6-Follow", "follow", 1, 1},
		{"v6-reject", filepath.Join(base, "reject"), "Personal/V6-Reject", "reject", 1, 1},
		{"v6-copy-candidate", filepath.Join(base, "copy"), "Personal/V6-Copy", "follow", 0, 0},
	} {
		if _, err := db.ExecContext(ctx, `
INSERT INTO sync_roots(uuid, local_root, remote_name, remote_root, enabled, initialized, symlink_mode, poll_interval_seconds, created_at_ns)
VALUES(?, ?, 'pkudisk', ?, ?, ?, ?, 60, 1)`, row.uuid, row.localRoot, row.remoteRoot, row.enabled, row.initialized, row.mode); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	for _, legacy := range []struct {
		rootID   int64
		relPath  string
		phase    string
		attempts int
	}{
		{1, "followed-running.txt", "running", 1},
		{2, "lexical-running.txt", "running", 1},
		{1, "planned.txt", "planned", 0},
	} {
		if _, err := db.ExecContext(ctx, `
INSERT INTO operations(
    sync_root_id, kind, entry_kind, src_path, dst_path,
    expected_local_present, expected_local_kind, expected_local_size, expected_local_mtime_ns,
    expected_remote_absent, expected_remote_id, expected_remote_rev,
    phase, attempts, last_error, created_at_ns, updated_at_ns,
    local_target_path, local_target_identity
) VALUES(?, 'delete-local', 'file', ?, '', 1, 'file', 4, 40, 1, '', '', ?, ?, '', 1, 1, ?, ?)`,
			legacy.rootID, legacy.relPath, legacy.phase, legacy.attempts, filepath.Join(base, legacy.relPath), fmt.Sprintf("anchor-%s", legacy.relPath)); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, "PRAGMA user_version = 6"); err != nil {
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

	operations, err := s.ListOperations(ctx, 1)
	if err != nil || len(operations) != 2 {
		t.Fatalf("follow-root migrated operations = %+v err=%v", operations, err)
	}
	byPath := make(map[string]domain.Operation, len(operations))
	for _, op := range operations {
		byPath[op.SrcPath] = op
	}
	running := byPath["followed-running.txt"]
	if running.LocalTargetPath == "" || running.LocalTargetIdentity == "" || running.LocalTargetAuthority != "" || running.LocalSymlinkTarget != "" {
		t.Fatalf("started v6 follow operation invented or lost authority state: %+v", running)
	}
	planned := byPath["planned.txt"]
	if planned.LocalTargetPath != "" || planned.LocalTargetIdentity != "" || planned.LocalTargetAuthority != "" || planned.LocalSymlinkTarget != "" {
		t.Fatalf("unattempted v6 operation retained stale pin: %+v", planned)
	}

	operations, err = s.ListOperations(ctx, 2)
	if err != nil || len(operations) != 1 {
		t.Fatalf("non-follow migrated operations = %+v err=%v", operations, err)
	}
	running = operations[0]
	if running.LocalTargetPath == "" || running.LocalTargetIdentity == "" || running.LocalTargetAuthority != "" || running.LocalSymlinkTarget != "" {
		t.Fatalf("started v6 non-follow operation invented or lost authority state: %+v", running)
	}

	if err := s.SetSyncRootSymlinkMode(ctx, 3, domain.SymlinkCopy); err != nil {
		t.Fatalf("v7 copy mode rejected after migration: %v", err)
	}
	root, ok, err := s.GetSyncRoot(ctx, 3)
	if err != nil || !ok || root.EffectiveSymlinkMode() != domain.SymlinkCopy {
		t.Fatalf("copy-mode migrated root = %+v ok=%v err=%v", root, ok, err)
	}
	var version int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version after v6 migration = %d, want %d", version, schemaVersion)
	}
}

func TestMigrationV4ToV5PreservesDuplicateLegacyClaimsButBlocksBothOwners(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	dbPath := filepath.Join(base, "state-v4-duplicate.sqlite3")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schemaV1); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	for _, statement := range []string{
		`ALTER TABLE sync_roots ADD COLUMN initialized INTEGER NOT NULL DEFAULT 0 CHECK (initialized IN (0, 1))`,
		`ALTER TABLE sync_roots ADD COLUMN symlink_mode TEXT NOT NULL DEFAULT 'follow' CHECK (symlink_mode IN ('follow', 'reject', 'ignore'))`,
		`ALTER TABLE operations ADD COLUMN local_target_path TEXT NOT NULL DEFAULT ''`,
		`CREATE TABLE followed_directory_boundaries (
            sync_root_id INTEGER NOT NULL,
            rel_path TEXT NOT NULL,
            physical_identity TEXT NOT NULL,
            PRIMARY KEY(sync_root_id, rel_path),
            FOREIGN KEY(sync_root_id) REFERENCES sync_roots(id) ON DELETE CASCADE
        )`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	for _, row := range []struct {
		uuid, localRoot, remoteRoot string
	}{
		{"v4-root-a", filepath.Join(base, "root-a"), "Personal/V4-A"},
		{"v4-root-b", filepath.Join(base, "root-b"), "Personal/V4-B"},
	} {
		if _, err := db.ExecContext(ctx, `
INSERT INTO sync_roots(uuid, local_root, remote_name, remote_root, enabled, initialized, symlink_mode, poll_interval_seconds, created_at_ns)
VALUES(?, ?, 'pkudisk', ?, 1, 1, 'follow', 60, 1)`, row.uuid, row.localRoot, row.remoteRoot); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	for _, legacy := range []struct {
		phase    string
		attempts int
		path     string
	}{
		{phase: string(domain.OperationPlanned), attempts: 0, path: filepath.Join(base, "legacy-planned")},
		{phase: string(domain.OperationRunning), attempts: 1, path: filepath.Join(base, "legacy-running")},
	} {
		if _, err := db.ExecContext(ctx, `
INSERT INTO operations(
    sync_root_id, kind, entry_kind, src_path, dst_path,
    expected_local_present, expected_local_kind, expected_local_size, expected_local_mtime_ns,
    expected_remote_absent, expected_remote_id, expected_remote_rev,
    phase, attempts, last_error, created_at_ns, updated_at_ns, local_target_path
) VALUES(1, 'delete-local', 'file', ?, '', 1, 'file', 1, 1, 1, '', '', ?, ?, '', 1, 1, ?)`,
			"legacy-"+legacy.phase+".txt", legacy.phase, legacy.attempts, legacy.path); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	for rootID, relPath := range map[int64]string{1: "link-a", 2: "link-b"} {
		if _, err := db.ExecContext(ctx, `
INSERT INTO followed_directory_boundaries(sync_root_id, rel_path, physical_identity)
VALUES(?, ?, 'linux:49:shared')`, rootID, relPath); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, "PRAGMA user_version = 4"); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("v4 duplicate ownership migration should remain inspectable: %v", err)
	}
	defer func() { _ = s.Close() }()
	for rootID, relPath := range map[int64]string{1: "link-a", 2: "link-b"} {
		claims, err := s.ListFollowedPhysicalClaims(ctx, rootID)
		if err != nil {
			t.Fatal(err)
		}
		claim := claims[relPath]
		if claim.Kind != domain.KindDir || claim.Identity != "linux:49:shared" || claim.TargetPath != "" {
			t.Fatalf("migrated root %d claim = %+v", rootID, claim)
		}
		claim.TargetPath = filepath.Join(base, "shared-current")
		observed := map[string]domain.FollowedPhysicalClaim{relPath: claim}
		ok, detail, err := s.ReserveFollowedPhysicalClaims(ctx, rootID, observed)
		if err != nil {
			t.Fatal(err)
		}
		if ok || !strings.Contains(detail, "already owned by sync root") {
			t.Fatalf("legacy duplicate root %d was not blocked: ok=%v detail=%q", rootID, ok, detail)
		}
	}
	operations, err := s.ListOperations(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(operations) != 2 {
		t.Fatalf("migrated legacy operations = %+v", operations)
	}
	for _, op := range operations {
		switch op.Phase {
		case domain.OperationPlanned:
			if op.LocalTargetPath != "" || op.LocalTargetIdentity != "" || op.LocalTargetAuthority != "" {
				t.Fatalf("planned legacy mutation retained incomplete pin: %+v", op)
			}
		case domain.OperationRunning:
			if op.LocalTargetPath == "" || op.LocalTargetIdentity != "" || op.LocalTargetAuthority != "" {
				t.Fatalf("started legacy mutation migration lost inspectability or invented identity: %+v", op)
			}
		default:
			t.Fatalf("unexpected migrated operation phase: %+v", op)
		}
	}
	var version int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version after v4 migration = %d, want %d", version, schemaVersion)
	}
}
