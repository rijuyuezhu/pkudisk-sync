package store

import (
	"context"
	"path/filepath"
	"runtime"
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

func TestCreateSyncRootRejectsPreinitializedRoot(t *testing.T) {
	root := testSyncRoot("preinitialized", filepath.Join(t.TempDir(), "Data"), "pkudisk", "Personal/Data")
	root.Initialized = true
	if _, err := openTestStore(t).CreateSyncRoot(context.Background(), root); err == nil {
		t.Fatal("expected preinitialized new root to be rejected")
	}
}
