package daemon

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
	"github.com/rijuyuezhu/pkudisk-sync/internal/rootmarker"
	"github.com/rijuyuezhu/pkudisk-sync/internal/store"
)

func TestSetupRootCreatesMarkerAndDurableRoot(t *testing.T) {
	ctx := context.Background()
	state := openDaemonTestStore(t)
	root := daemonTestRoot(t, "root-1", "Personal/Data")

	stored, err := SetupRoot(ctx, state, root)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ID <= 0 {
		t.Fatalf("stored root ID = %d", stored.ID)
	}
	if err := rootmarker.Check(root.LocalRoot, root.UUID); err != nil {
		t.Fatalf("marker check: %v", err)
	}
	got, ok, err := state.GetSyncRoot(ctx, stored.ID)
	if err != nil || !ok {
		t.Fatalf("GetSyncRoot() = %+v, %v, %v", got, ok, err)
	}
}

func TestSetupRootReservesOwnershipBeforeCreatingMarker(t *testing.T) {
	ctx := context.Background()
	state := openDaemonTestStore(t)
	first := daemonTestRoot(t, "root-1", "Personal/Data")
	if _, err := SetupRoot(ctx, state, first); err != nil {
		t.Fatal(err)
	}

	second := daemonTestRoot(t, "root-2", "Personal/Data/Child")
	if _, err := SetupRoot(ctx, state, second); err == nil {
		t.Fatal("overlapping remote root was accepted")
	}
	if _, err := os.Lstat(filepath.Join(second.LocalRoot, rootmarker.FileName)); !os.IsNotExist(err) {
		t.Fatalf("ownership preflight created a marker for a rejected root: %v", err)
	}
}

func TestSetupRootPreservesPreexistingMatchingMarkerOnConflict(t *testing.T) {
	ctx := context.Background()
	state := openDaemonTestStore(t)
	first := daemonTestRoot(t, "root-1", "Personal/Data")
	if _, err := SetupRoot(ctx, state, first); err != nil {
		t.Fatal(err)
	}

	second := daemonTestRoot(t, "root-2", "Personal/Data/Child")
	if err := rootmarker.Ensure(second.LocalRoot, second.UUID); err != nil {
		t.Fatal(err)
	}
	if _, err := SetupRoot(ctx, state, second); err == nil {
		t.Fatal("overlapping remote root was accepted")
	}
	if err := rootmarker.Check(second.LocalRoot, second.UUID); err != nil {
		t.Fatalf("preexisting matching marker was removed: %v", err)
	}
}

func TestSetupRootRefusesUnexpectedReservedMarker(t *testing.T) {
	ctx := context.Background()
	state := openDaemonTestStore(t)
	root := daemonTestRoot(t, "root-1", "Personal/Data")
	marker := filepath.Join(root.LocalRoot, rootmarker.FileName)
	if err := os.WriteFile(marker, []byte("someone-else\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := SetupRoot(ctx, state, root); err == nil {
		t.Fatal("unexpected reserved marker was accepted")
	}
	roots, err := state.ListSyncRoots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 0 {
		t.Fatalf("rejected setup created durable roots: %+v", roots)
	}
}

func TestSetupRootSerializesConcurrentPairing(t *testing.T) {
	ctx := context.Background()
	state := openDaemonTestStore(t)
	root := daemonTestRoot(t, "same-root", "Personal/Same")

	const attempts = 20
	var wg sync.WaitGroup
	errs := make(chan error, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := SetupRoot(ctx, state, root)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	successes := 0
	for err := range errs {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent setup successes = %d, want 1", successes)
	}
	if err := rootmarker.Check(root.LocalRoot, root.UUID); err != nil {
		t.Fatalf("winning setup lost its marker: %v", err)
	}
	roots, err := state.ListSyncRoots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 1 || roots[0].UUID != root.UUID {
		t.Fatalf("durable roots after concurrent setup = %+v", roots)
	}
}

func TestRemoveRootUnregistersPausedIdlePairWithoutDeletingUserData(t *testing.T) {
	ctx := context.Background()
	state := openDaemonTestStore(t)
	root := daemonTestRoot(t, "remove-root", "Personal/Remove")
	stored, err := SetupRoot(ctx, state, root)
	if err != nil {
		t.Fatal(err)
	}
	userFile := filepath.Join(root.LocalRoot, "keep.txt")
	if err := os.WriteFile(userFile, []byte("keep me"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := state.SetSyncRootEnabled(ctx, stored.ID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := state.CreateConflict(ctx, domain.Conflict{
		SyncRootID: stored.ID,
		RelPath:    "conflict.txt",
		Kind:       domain.ConflictBothModified,
		Local:      domain.LocalFingerprint{Present: true, Kind: domain.KindFile, Size: 1, MtimeNS: 1},
		Remote:     domain.RemoteFingerprint{Present: true, Kind: domain.KindFile, ID: "doc", Rev: "rev", Size: 1},
	}); err != nil {
		t.Fatal(err)
	}

	removed, err := RemoveRoot(ctx, state, stored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if removed.ID != stored.ID || removed.UUID != stored.UUID {
		t.Fatalf("removed root = %+v, want %+v", removed, stored)
	}
	if _, ok, err := state.GetSyncRoot(ctx, stored.ID); err != nil || ok {
		t.Fatalf("removed root still durable: ok=%v err=%v", ok, err)
	}
	if _, err := os.Lstat(filepath.Join(root.LocalRoot, rootmarker.FileName)); !os.IsNotExist(err) {
		t.Fatalf("root marker remains after detach: %v", err)
	}
	contents, err := os.ReadFile(userFile)
	if err != nil || string(contents) != "keep me" {
		t.Fatalf("user data changed by detach: contents=%q err=%v", contents, err)
	}
}

func TestRemoveRootRequiresPausedIdleRootAndPreservesMarkerOnRejection(t *testing.T) {
	ctx := context.Background()
	state := openDaemonTestStore(t)
	root := daemonTestRoot(t, "busy-root", "Personal/Busy")
	stored, err := SetupRoot(ctx, state, root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RemoveRoot(ctx, state, stored.ID); err == nil {
		t.Fatal("enabled root was removed")
	}
	if err := rootmarker.Check(root.LocalRoot, root.UUID); err != nil {
		t.Fatalf("enabled-root rejection lost marker: %v", err)
	}
	if err := state.SetSyncRootEnabled(ctx, stored.ID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := state.CreateOperation(ctx, domain.Operation{
		SyncRootID:     stored.ID,
		Kind:           domain.OperationEnsureRemote,
		EntryKind:      domain.KindFile,
		SrcPath:        "pending.txt",
		ExpectedLocal:  domain.LocalFingerprint{Present: true, Kind: domain.KindFile, Size: 1, MtimeNS: 1},
		ExpectedRemote: domain.RemoteExpectation{Absent: true},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := RemoveRoot(ctx, state, stored.ID); err == nil {
		t.Fatal("root with pending operation was removed")
	}
	if err := rootmarker.Check(root.LocalRoot, root.UUID); err != nil {
		t.Fatalf("busy-root rejection lost marker: %v", err)
	}
}

func openDaemonTestStore(t *testing.T) *store.Store {
	t.Helper()
	state, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	return state
}

func daemonTestRoot(t *testing.T, uuid, remoteRoot string) domain.SyncRoot {
	t.Helper()
	return domain.SyncRoot{
		UUID:                uuid,
		LocalRoot:           t.TempDir(),
		RemoteName:          "pkudisk",
		RemoteRoot:          remoteRoot,
		Enabled:             true,
		PollIntervalSeconds: 60,
	}
}
