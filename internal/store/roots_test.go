package store

import (
	"context"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

func TestCreateSyncRootRejectsOverlappingOwnership(t *testing.T) {
	ctx := context.Background()
	localBase := filepath.Join(t.TempDir(), "roots")

	tests := []struct {
		name      string
		existing  domain.SyncRoot
		candidate domain.SyncRoot
		wantErr   bool
	}{
		{
			name:      "sibling roots are independent",
			existing:  testSyncRoot("one", filepath.Join(localBase, "Data"), "pkudisk", "Personal/Data"),
			candidate: testSyncRoot("two", filepath.Join(localBase, "Work"), "pkudisk", "Personal/Work"),
		},
		{
			name:      "local child overlaps",
			existing:  testSyncRoot("one", filepath.Join(localBase, "Data"), "pkudisk", "Personal/Data"),
			candidate: testSyncRoot("two", filepath.Join(localBase, "Data", "sub"), "pkudisk", "Personal/Elsewhere"),
			wantErr:   true,
		},
		{
			name:      "local parent overlaps",
			existing:  testSyncRoot("one", filepath.Join(localBase, "Data", "sub"), "pkudisk", "Personal/Data"),
			candidate: testSyncRoot("two", filepath.Join(localBase, "Data"), "pkudisk", "Personal/Elsewhere"),
			wantErr:   true,
		},
		{
			name:      "remote child overlaps on same remote",
			existing:  testSyncRoot("one", filepath.Join(localBase, "Data"), "pkudisk", "Personal/Sync"),
			candidate: testSyncRoot("two", filepath.Join(localBase, "Work"), "pkudisk", "Personal/Sync/sub"),
			wantErr:   true,
		},
		{
			name:      "remote parent overlaps on same remote",
			existing:  testSyncRoot("one", filepath.Join(localBase, "Data"), "pkudisk", "Personal/Sync/sub"),
			candidate: testSyncRoot("two", filepath.Join(localBase, "Work"), "pkudisk", "Personal/Sync"),
			wantErr:   true,
		},
		{
			name:      "disabled root still owns its namespace",
			existing:  testSyncRoot("one", filepath.Join(localBase, "Data"), "pkudisk", "Personal/Sync"),
			candidate: testSyncRoot("two", filepath.Join(localBase, "Work"), "pkudisk", "Personal/Sync/sub"),
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestStore(t)
			tt.existing.Enabled = false
			if _, err := s.CreateSyncRoot(ctx, tt.existing); err != nil {
				t.Fatalf("create existing root: %v", err)
			}
			_, err := s.CreateSyncRoot(ctx, tt.candidate)
			if tt.wantErr && err == nil {
				t.Fatal("expected overlapping sync root to be rejected")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("independent sync root rejected: %v", err)
			}
		})
	}
}

func TestLocalRootsOverlapUsesPlatformNamespaceSemantics(t *testing.T) {
	base := filepath.Join(t.TempDir(), "Roots")
	caseChild := filepath.Join(filepath.Dir(base), "roots", "Child")
	if !localRootsOverlapForOS(base, caseChild, "darwin") {
		t.Fatal("darwin case alias was not treated as overlapping")
	}
	if !localRootsOverlapForOS(base, caseChild, "windows") {
		t.Fatal("windows case alias was not treated as overlapping")
	}
	if runtime.GOOS != "windows" && localRootsOverlapForOS(base, caseChild, "linux") {
		t.Fatal("linux distinct case spellings were treated as overlapping")
	}

	composed := filepath.Join(t.TempDir(), "caf\u00e9")
	decomposedChild := filepath.Join(filepath.Dir(composed), "cafe\u0301", "Child")
	if !localRootsOverlapForOS(composed, decomposedChild, "darwin") {
		t.Fatal("darwin normalization alias was not treated as overlapping")
	}
	if runtime.GOOS != "windows" && localRootsOverlapForOS(composed, decomposedChild, "linux") {
		t.Fatal("linux normalization-distinct spellings were treated as overlapping")
	}
}

func TestCreateSyncRootSerializesOwnershipAcrossStoreInstances(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	firstStore, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = firstStore.Close() }()
	secondStore, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = secondStore.Close() }()

	localBase := t.TempDir()
	first := testSyncRoot("one", filepath.Join(localBase, "one"), "pkudisk", "Personal/Shared")
	second := testSyncRoot("two", filepath.Join(localBase, "two"), "pkudisk", "Personal/Shared/Child")
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, attempt := range []struct {
		store *Store
		root  domain.SyncRoot
	}{{firstStore, first}, {secondStore, second}} {
		wg.Add(1)
		go func(attempt struct {
			store *Store
			root  domain.SyncRoot
		}) {
			defer wg.Done()
			<-start
			_, err := attempt.store.CreateSyncRoot(ctx, attempt.root)
			results <- err
		}(attempt)
	}
	close(start)
	wg.Wait()
	close(results)

	successes := 0
	failures := 0
	for err := range results {
		if err == nil {
			successes++
		} else {
			failures++
		}
	}
	if successes != 1 || failures != 1 {
		t.Fatalf("concurrent ownership results: successes=%d failures=%d", successes, failures)
	}
	roots, err := firstStore.ListSyncRoots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 1 {
		t.Fatalf("durable roots after concurrent overlap = %+v", roots)
	}
}

func TestPrepareSyncRootCreateRejectsOverlapWithoutMutation(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	base := filepath.Join(t.TempDir(), "roots")
	existing := testSyncRoot("one", filepath.Join(base, "Data"), "pkudisk", "Personal/Data")
	if _, err := s.CreateSyncRoot(ctx, existing); err != nil {
		t.Fatal(err)
	}
	candidate := testSyncRoot("two", filepath.Join(base, "Work"), "pkudisk", "Personal/Data/Child")
	reservation, err := s.PrepareSyncRootCreate(ctx, candidate)
	if err == nil {
		_ = reservation.Close()
		t.Fatal("overlapping candidate acquired ownership reservation")
	}
	roots, err := s.ListSyncRoots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 1 || roots[0].UUID != existing.UUID {
		t.Fatalf("candidate reservation mutated durable roots: %+v", roots)
	}
}

func TestListSyncRootsRejectsInvalidPersistedPollInterval(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	localRoot := filepath.Join(t.TempDir(), "Data")
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO sync_roots(uuid, local_root, remote_name, remote_root, enabled, initialized, poll_interval_seconds, created_at_ns)
VALUES(?, ?, 'pkudisk', 'Personal/Data', 1, 0, ?, 1)`, "bad-poll", localRoot, int64(^uint64(0)>>1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListSyncRoots(ctx); err == nil {
		t.Fatal("invalid persisted poll interval was accepted")
	}
}

func TestSetSyncRootEnabledPreservesSelection(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	root := createTestRoot(t, s)

	if err := s.SetSyncRootEnabled(ctx, root.ID, false); err != nil {
		t.Fatal(err)
	}
	paused, ok, err := s.GetSyncRoot(ctx, root.ID)
	if err != nil || !ok {
		t.Fatalf("get paused root = %+v, %v, %v", paused, ok, err)
	}
	if paused.Enabled {
		t.Fatal("root remained enabled after pause")
	}

	if err := s.SetSyncRootEnabled(ctx, root.ID, true); err != nil {
		t.Fatal(err)
	}
	resumed, ok, err := s.GetSyncRoot(ctx, root.ID)
	if err != nil || !ok {
		t.Fatalf("get resumed root = %+v, %v, %v", resumed, ok, err)
	}
	if !resumed.Enabled {
		t.Fatal("root remained disabled after resume")
	}
	if resumed.LocalRoot != root.LocalRoot || resumed.RemoteRoot != root.RemoteRoot {
		t.Fatalf("pause/resume changed root selection: got %+v want %+v", resumed, root)
	}
}

func testSyncRoot(uuid, localRoot, remoteName, remoteRoot string) domain.SyncRoot {
	return domain.SyncRoot{
		UUID:                uuid,
		LocalRoot:           localRoot,
		RemoteName:          remoteName,
		RemoteRoot:          remoteRoot,
		Enabled:             true,
		PollIntervalSeconds: 60,
	}
}

func TestSyncRootInitializationLifecycle(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	root := createTestRoot(t, s)
	if root.Initialized {
		t.Fatal("new sync root unexpectedly starts initialized")
	}

	if err := s.MarkSyncRootInitialized(ctx, root.ID); err != nil {
		t.Fatal(err)
	}
	initialized, ok, err := s.GetSyncRoot(ctx, root.ID)
	if err != nil || !ok {
		t.Fatalf("get initialized root = %+v, %v, %v", initialized, ok, err)
	}
	if !initialized.Initialized {
		t.Fatal("root did not persist initialized state")
	}

	if err := s.SetSyncRootEnabled(ctx, root.ID, false); err != nil {
		t.Fatal(err)
	}
	paused, ok, err := s.GetSyncRoot(ctx, root.ID)
	if err != nil || !ok {
		t.Fatalf("get paused initialized root = %+v, %v, %v", paused, ok, err)
	}
	if paused.Enabled || !paused.Initialized {
		t.Fatalf("pause changed initialization state: %+v", paused)
	}
}

func TestInitializeSyncRootRequiresReservedFollowedPhysicalClaims(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	root := createTestRoot(t, s)
	claimBase := t.TempDir()
	want := map[string]domain.FollowedPhysicalClaim{
		"linked":       {Kind: domain.KindDir, Identity: "linux:1:2", TargetPath: filepath.Join(claimBase, "linked")},
		"nested/alias": {Kind: domain.KindFile, Identity: "linux:1:3", TargetPath: filepath.Join(claimBase, "alias.txt")},
	}
	ok, detail, err := s.ReserveFollowedPhysicalClaims(ctx, root.ID, want)
	if err != nil || !ok {
		t.Fatalf("reserve followed physical claims = ok=%v detail=%q err=%v", ok, detail, err)
	}
	if err := s.InitializeSyncRoot(ctx, root.ID, want); err != nil {
		t.Fatal(err)
	}
	got, err := s.ListFollowedPhysicalClaims(ctx, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("followed boundaries = %+v, want %+v", got, want)
	}
	initialized, ok, err := s.GetSyncRoot(ctx, root.ID)
	if err != nil || !ok || !initialized.Initialized {
		t.Fatalf("initialized root = %+v ok=%v err=%v", initialized, ok, err)
	}
	if err := s.InitializeSyncRoot(ctx, root.ID, map[string]domain.FollowedPhysicalClaim{"linked": {Kind: domain.KindDir, Identity: "linux:9:9", TargetPath: filepath.Join(claimBase, "linked")}}); err == nil {
		t.Fatal("second initialization rewrote durable followed identities")
	}
	got, err = s.ListFollowedPhysicalClaims(ctx, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("failed second initialization changed boundaries = %+v", got)
	}
}

func TestReserveFollowedPhysicalClaimsRejectsCrossRootFileAndDirectoryOwnership(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	rootA := createTestRoot(t, s)
	rootB, err := s.CreateSyncRoot(ctx, domain.SyncRoot{
		UUID:                "root-uuid-2",
		LocalRoot:           filepath.Join(t.TempDir(), "pkudisk-sync-root-b"),
		RemoteName:          domain.AppRemoteName,
		RemoteRoot:          "Personal/Sync-B",
		Enabled:             true,
		PollIntervalSeconds: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimBase := t.TempDir()
	claimsA := map[string]domain.FollowedPhysicalClaim{
		"dir-link":  {Kind: domain.KindDir, Identity: "linux:49:100", TargetPath: filepath.Join(claimBase, "shared-dir")},
		"file-link": {Kind: domain.KindFile, Identity: "linux:49:101", TargetPath: filepath.Join(claimBase, "shared-file")},
	}
	if ok, detail, err := s.ReserveFollowedPhysicalClaims(ctx, rootA.ID, claimsA); err != nil || !ok {
		t.Fatalf("reserve root A claims = ok=%v detail=%q err=%v", ok, detail, err)
	}

	for name, claim := range map[string]domain.FollowedPhysicalClaim{
		"shared-dir":  {Kind: domain.KindDir, Identity: "linux:49:100", TargetPath: filepath.Join(t.TempDir(), "dir-alias")},
		"shared-file": {Kind: domain.KindFile, Identity: "linux:49:999", TargetPath: filepath.Join(claimBase, "shared-file")},
	} {
		t.Run(name, func(t *testing.T) {
			ok, detail, err := s.ReserveFollowedPhysicalClaims(ctx, rootB.ID, map[string]domain.FollowedPhysicalClaim{name: claim})
			if err != nil {
				t.Fatal(err)
			}
			if ok || !strings.Contains(detail, "already owned by sync root") {
				t.Fatalf("cross-root reservation = ok=%v detail=%q", ok, detail)
			}
		})
	}

	unique := map[string]domain.FollowedPhysicalClaim{"unique": {Kind: domain.KindFile, Identity: "linux:49:102", TargetPath: filepath.Join(claimBase, "unique-file")}}
	if ok, detail, err := s.ReserveFollowedPhysicalClaims(ctx, rootB.ID, unique); err != nil || !ok {
		t.Fatalf("reserve distinct root B claim = ok=%v detail=%q err=%v", ok, detail, err)
	}
}

func TestReserveFollowedFileClaimAllowsAtomicReplacementWithoutChangingOwner(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	rootA := createTestRoot(t, s)
	target := filepath.Join(t.TempDir(), "shared-file")
	oldClaim := domain.FollowedPhysicalClaim{Kind: domain.KindFile, Identity: "linux:49:200", TargetPath: target}
	if ok, detail, err := s.ReserveFollowedPhysicalClaims(ctx, rootA.ID, map[string]domain.FollowedPhysicalClaim{"link": oldClaim}); err != nil || !ok {
		t.Fatalf("reserve old file claim = ok=%v detail=%q err=%v", ok, detail, err)
	}
	if err := s.InitializeSyncRoot(ctx, rootA.ID, map[string]domain.FollowedPhysicalClaim{"link": oldClaim}); err != nil {
		t.Fatal(err)
	}

	newClaim := oldClaim
	newClaim.Identity = "linux:49:201"
	if ok, detail, err := s.ReserveFollowedPhysicalClaims(ctx, rootA.ID, map[string]domain.FollowedPhysicalClaim{"link": newClaim}); err != nil || !ok {
		t.Fatalf("refresh atomic-replaced file claim = ok=%v detail=%q err=%v", ok, detail, err)
	}
	got, err := s.ListFollowedPhysicalClaims(ctx, rootA.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got["link"] != newClaim {
		t.Fatalf("refreshed file claim = %+v, want %+v", got["link"], newClaim)
	}

	rootB, err := s.CreateSyncRoot(ctx, domain.SyncRoot{
		UUID:                "root-uuid-file-competitor",
		LocalRoot:           filepath.Join(t.TempDir(), "pkudisk-sync-root-c"),
		RemoteName:          domain.AppRemoteName,
		RemoteRoot:          "Personal/Sync-C",
		Enabled:             true,
		PollIntervalSeconds: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	competitor := domain.FollowedPhysicalClaim{Kind: domain.KindFile, Identity: "linux:49:202", TargetPath: target}
	ok, detail, err := s.ReserveFollowedPhysicalClaims(ctx, rootB.ID, map[string]domain.FollowedPhysicalClaim{"other": competitor})
	if err != nil {
		t.Fatal(err)
	}
	if ok || !strings.Contains(detail, "physical target") {
		t.Fatalf("same target path was not retained by original owner: ok=%v detail=%q", ok, detail)
	}
}

func TestAuthorizeAndPinLocalMutationRefreshesSameTargetFileIdentity(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	root := createTestRoot(t, s)
	targetPath := filepath.Join(t.TempDir(), "atomic-file")
	oldClaim := domain.FollowedPhysicalClaim{Kind: domain.KindFile, Identity: "linux:49:300", TargetPath: targetPath}
	if ok, detail, err := s.ReserveFollowedPhysicalClaims(ctx, root.ID, map[string]domain.FollowedPhysicalClaim{"link.txt": oldClaim}); err != nil || !ok {
		t.Fatalf("reserve old claim = ok=%v detail=%q err=%v", ok, detail, err)
	}
	op, err := s.CreateOperation(ctx, domain.Operation{
		SyncRootID:     root.ID,
		Kind:           domain.OperationEnsureLocal,
		EntryKind:      domain.KindFile,
		SrcPath:        "link.txt",
		ExpectedLocal:  domain.LocalFingerprint{Present: true, Kind: domain.KindFile, Size: 4, MtimeNS: 40},
		ExpectedRemote: domain.RemoteExpectation{ID: "doc", Rev: "rev"},
	})
	if err != nil {
		t.Fatal(err)
	}
	newClaim := oldClaim
	newClaim.Identity = "linux:49:301"
	ok, detail, err := s.AuthorizeAndPinLocalMutation(ctx, op.ID, domain.LocalMutationTarget{
		Path:           targetPath,
		AnchorIdentity: newClaim.Identity,
		FollowedClaims: map[string]domain.FollowedPhysicalClaim{"link.txt": newClaim},
	})
	if err != nil || !ok {
		t.Fatalf("authorize atomic replacement = ok=%v detail=%q err=%v", ok, detail, err)
	}
	gotOp, exists, err := s.GetOperation(ctx, op.ID)
	if err != nil || !exists || gotOp.LocalTargetPath != targetPath || gotOp.LocalTargetIdentity != newClaim.Identity {
		t.Fatalf("pinned operation = %+v exists=%v err=%v", gotOp, exists, err)
	}
	claims, err := s.ListFollowedPhysicalClaims(ctx, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := claims["link.txt"]; got != newClaim {
		t.Fatalf("mutation-time file identity = %+v, want %+v", got, newClaim)
	}
}

func TestAuthorizeAndPinLocalMutationRequiresDurableAuthorityForDanglingFile(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	root := createTestRoot(t, s)
	targetPath := filepath.Join(t.TempDir(), "dangling-file")
	durable := domain.FollowedPhysicalClaim{Kind: domain.KindFile, Identity: "linux:49:400", TargetPath: targetPath}
	if ok, detail, err := s.ReserveFollowedPhysicalClaims(ctx, root.ID, map[string]domain.FollowedPhysicalClaim{"known.txt": durable}); err != nil || !ok {
		t.Fatalf("reserve durable dangling owner = ok=%v detail=%q err=%v", ok, detail, err)
	}

	newOp := func(path string) domain.Operation {
		op, err := s.CreateOperation(ctx, domain.Operation{
			SyncRootID:     root.ID,
			Kind:           domain.OperationEnsureLocal,
			EntryKind:      domain.KindFile,
			SrcPath:        path,
			ExpectedLocal:  domain.LocalFingerprint{},
			ExpectedRemote: domain.RemoteExpectation{ID: "doc-" + path, Rev: "rev"},
		})
		if err != nil {
			t.Fatal(err)
		}
		return op
	}

	known := newOp("known.txt")
	ok, detail, err := s.AuthorizeAndPinLocalMutation(ctx, known.ID, domain.LocalMutationTarget{
		Path:           targetPath,
		AnchorIdentity: "known-parent-anchor",
		FollowedClaims: map[string]domain.FollowedPhysicalClaim{
			"known.txt": {Kind: domain.KindFile, TargetPath: targetPath},
		},
	})
	if err != nil || !ok {
		t.Fatalf("authorize durable dangling file = ok=%v detail=%q err=%v", ok, detail, err)
	}

	newTarget := filepath.Join(t.TempDir(), "new-dangling-file")
	unknown := newOp("unknown.txt")
	ok, detail, err = s.AuthorizeAndPinLocalMutation(ctx, unknown.ID, domain.LocalMutationTarget{
		Path:           newTarget,
		AnchorIdentity: "unknown-parent-anchor",
		FollowedClaims: map[string]domain.FollowedPhysicalClaim{
			"unknown.txt": {Kind: domain.KindFile, TargetPath: newTarget},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if ok || !strings.Contains(detail, "appeared after the authoritative scan") {
		t.Fatalf("new dangling boundary gained authority: ok=%v detail=%q", ok, detail)
	}
}

func TestCreateSyncRootRejectsPreinitializedRoot(t *testing.T) {
	root := testSyncRoot("preinitialized", filepath.Join(t.TempDir(), "Data"), "pkudisk", "Personal/Data")
	root.Initialized = true
	if _, err := openTestStore(t).CreateSyncRoot(context.Background(), root); err == nil {
		t.Fatal("expected preinitialized new root to be rejected")
	}
}
