package store

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

func TestPhysicalOwnershipPathsOverlapUsesDirectorySubtreeSemantics(t *testing.T) {
	base := filepath.Join(t.TempDir(), "shared")
	childDir := filepath.Join(base, "subdir")
	childFile := filepath.Join(base, "file.txt")
	sibling := filepath.Join(filepath.Dir(base), "shared2")

	if !physicalOwnershipPathsOverlap(base, domain.KindDir, childDir, domain.KindDir) {
		t.Fatal("directory parent did not own descendant directory")
	}
	if !physicalOwnershipPathsOverlap(base, domain.KindDir, childFile, domain.KindFile) {
		t.Fatal("directory parent did not own descendant file")
	}
	if !physicalOwnershipPathsOverlap(childDir, domain.KindDir, base, domain.KindDir) {
		t.Fatal("directory overlap was not symmetric")
	}
	if physicalOwnershipPathsOverlap(base, domain.KindDir, sibling, domain.KindDir) {
		t.Fatal("similarly prefixed sibling path was treated as a descendant")
	}
	if physicalOwnershipPathsOverlap(childFile, domain.KindFile, filepath.Join(childFile, "child"), domain.KindFile) {
		t.Fatal("file claim incorrectly owned a lexical descendant")
	}
}

func TestReserveFollowedPhysicalClaimsRejectsCrossRootSubtreeOverlap(t *testing.T) {
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "shared")
	tests := []struct {
		name         string
		first        domain.FollowedPhysicalClaim
		second       domain.FollowedPhysicalClaim
		wantConflict bool
	}{
		{
			name:         "directory parent then directory child",
			first:        domain.FollowedPhysicalClaim{Kind: domain.KindDir, Identity: "linux:49:parent", TargetPath: base},
			second:       domain.FollowedPhysicalClaim{Kind: domain.KindDir, Identity: "linux:49:child", TargetPath: filepath.Join(base, "subdir")},
			wantConflict: true,
		},
		{
			name:         "directory parent then file child",
			first:        domain.FollowedPhysicalClaim{Kind: domain.KindDir, Identity: "linux:49:parent", TargetPath: base},
			second:       domain.FollowedPhysicalClaim{Kind: domain.KindFile, Identity: "linux:49:file", TargetPath: filepath.Join(base, "file.txt")},
			wantConflict: true,
		},
		{
			name:         "directory child then directory parent",
			first:        domain.FollowedPhysicalClaim{Kind: domain.KindDir, Identity: "linux:49:child", TargetPath: filepath.Join(base, "subdir")},
			second:       domain.FollowedPhysicalClaim{Kind: domain.KindDir, Identity: "linux:49:parent", TargetPath: base},
			wantConflict: true,
		},
		{
			name:         "file child then directory parent",
			first:        domain.FollowedPhysicalClaim{Kind: domain.KindFile, Identity: "linux:49:file", TargetPath: filepath.Join(base, "file.txt")},
			second:       domain.FollowedPhysicalClaim{Kind: domain.KindDir, Identity: "linux:49:parent", TargetPath: base},
			wantConflict: true,
		},
		{
			name:   "directory siblings remain independent",
			first:  domain.FollowedPhysicalClaim{Kind: domain.KindDir, Identity: "linux:49:left", TargetPath: filepath.Join(base, "left")},
			second: domain.FollowedPhysicalClaim{Kind: domain.KindDir, Identity: "linux:49:right", TargetPath: filepath.Join(base, "right")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestStore(t)
			rootA := createTestRoot(t, s)
			rootB, err := s.CreateSyncRoot(ctx, domain.SyncRoot{
				UUID:                "subtree-root-b",
				LocalRoot:           filepath.Join(t.TempDir(), "root-b"),
				RemoteName:          domain.AppRemoteName,
				RemoteRoot:          "Personal/Subtree-B",
				Enabled:             true,
				PollIntervalSeconds: 60,
			})
			if err != nil {
				t.Fatal(err)
			}
			if ok, detail, err := s.ReserveFollowedPhysicalClaims(ctx, rootA.ID, map[string]domain.FollowedPhysicalClaim{"first": tt.first}); err != nil || !ok {
				t.Fatalf("reserve first claim = ok=%v detail=%q err=%v", ok, detail, err)
			}
			ok, detail, err := s.ReserveFollowedPhysicalClaims(ctx, rootB.ID, map[string]domain.FollowedPhysicalClaim{"second": tt.second})
			if err != nil {
				t.Fatal(err)
			}
			if tt.wantConflict {
				if ok || !strings.Contains(detail, "overlapping target") {
					t.Fatalf("subtree overlap accepted: ok=%v detail=%q", ok, detail)
				}
			} else if !ok {
				t.Fatalf("independent subtree rejected: detail=%q", detail)
			}
		})
	}
}

func TestReserveFollowedPhysicalClaimsRejectsSameRootSubtreeAliases(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	root := createTestRoot(t, s)
	base := filepath.Join(t.TempDir(), "shared")
	_, _, err := s.ReserveFollowedPhysicalClaims(ctx, root.ID, map[string]domain.FollowedPhysicalClaim{
		"parent": {Kind: domain.KindDir, Identity: "linux:49:parent", TargetPath: base},
		"child":  {Kind: domain.KindFile, Identity: "linux:49:child", TargetPath: filepath.Join(base, "child.txt")},
	})
	if err == nil || !strings.Contains(err.Error(), "overlapping physical target subtrees") {
		t.Fatalf("same-root subtree alias validation error = %v", err)
	}
}

func TestFollowedDirectoryAndConfiguredRootRejectBidirectionalSubtreeOverlap(t *testing.T) {
	ctx := context.Background()

	t.Run("configured root exists before followed parent", func(t *testing.T) {
		s := openTestStore(t)
		rootA := createTestRoot(t, s)
		shared := filepath.Join(t.TempDir(), "shared")
		_, err := s.CreateSyncRoot(ctx, domain.SyncRoot{
			UUID:                "configured-child",
			LocalRoot:           filepath.Join(shared, "peer-root"),
			RemoteName:          domain.AppRemoteName,
			RemoteRoot:          "Personal/Configured-Child",
			Enabled:             true,
			PollIntervalSeconds: 60,
		})
		if err != nil {
			t.Fatal(err)
		}
		ok, detail, err := s.ReserveFollowedPhysicalClaims(ctx, rootA.ID, map[string]domain.FollowedPhysicalClaim{
			"link": {Kind: domain.KindDir, Identity: "linux:49:shared", TargetPath: shared},
		})
		if err != nil {
			t.Fatal(err)
		}
		if ok || !strings.Contains(detail, "overlapping configured sync root") {
			t.Fatalf("followed parent containing configured root accepted: ok=%v detail=%q", ok, detail)
		}
	})

	for _, tt := range []struct {
		name      string
		claim     domain.FollowedPhysicalClaim
		localRoot func(string) string
	}{
		{
			name:      "new configured child under followed directory",
			claim:     domain.FollowedPhysicalClaim{Kind: domain.KindDir, Identity: "linux:49:shared-dir"},
			localRoot: func(shared string) string { return filepath.Join(shared, "peer-root") },
		},
		{
			name:      "new configured parent containing followed file",
			claim:     domain.FollowedPhysicalClaim{Kind: domain.KindFile, Identity: "linux:49:shared-file"},
			localRoot: func(shared string) string { return shared },
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestStore(t)
			rootA := createTestRoot(t, s)
			shared := filepath.Join(t.TempDir(), "shared")
			claim := tt.claim
			if claim.Kind == domain.KindDir {
				claim.TargetPath = shared
			} else {
				claim.TargetPath = filepath.Join(shared, "file.txt")
			}
			if ok, detail, err := s.ReserveFollowedPhysicalClaims(ctx, rootA.ID, map[string]domain.FollowedPhysicalClaim{"link": claim}); err != nil || !ok {
				t.Fatalf("reserve followed claim = ok=%v detail=%q err=%v", ok, detail, err)
			}
			_, err := s.CreateSyncRoot(ctx, domain.SyncRoot{
				UUID:                "candidate-root",
				LocalRoot:           tt.localRoot(shared),
				RemoteName:          domain.AppRemoteName,
				RemoteRoot:          "Personal/Candidate",
				Enabled:             true,
				PollIntervalSeconds: 60,
			})
			if err == nil || !strings.Contains(err.Error(), "overlaps followed") {
				t.Fatalf("configured root overlapping followed claim error = %v", err)
			}
		})
	}
}

func TestReserveFollowedPhysicalClaimsSerializesSubtreeOwnershipAcrossStoreInstances(t *testing.T) {
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

	rootA, err := firstStore.CreateSyncRoot(ctx, testSyncRoot("concurrent-claim-a", filepath.Join(t.TempDir(), "root-a"), domain.AppRemoteName, "Personal/Concurrent-A"))
	if err != nil {
		t.Fatal(err)
	}
	rootB, err := firstStore.CreateSyncRoot(ctx, testSyncRoot("concurrent-claim-b", filepath.Join(t.TempDir(), "root-b"), domain.AppRemoteName, "Personal/Concurrent-B"))
	if err != nil {
		t.Fatal(err)
	}
	shared := filepath.Join(t.TempDir(), "shared")
	type reserveResult struct {
		ok     bool
		detail string
		err    error
	}
	start := make(chan struct{})
	results := make(chan reserveResult, 2)
	go func() {
		<-start
		ok, detail, err := firstStore.ReserveFollowedPhysicalClaims(ctx, rootA.ID, map[string]domain.FollowedPhysicalClaim{
			"parent": {Kind: domain.KindDir, Identity: "linux:49:concurrent-parent", TargetPath: shared},
		})
		results <- reserveResult{ok: ok, detail: detail, err: err}
	}()
	go func() {
		<-start
		ok, detail, err := secondStore.ReserveFollowedPhysicalClaims(ctx, rootB.ID, map[string]domain.FollowedPhysicalClaim{
			"child": {Kind: domain.KindDir, Identity: "linux:49:concurrent-child", TargetPath: filepath.Join(shared, "subdir")},
		})
		results <- reserveResult{ok: ok, detail: detail, err: err}
	}()
	close(start)
	first := <-results
	second := <-results
	for _, result := range []reserveResult{first, second} {
		if result.err != nil {
			t.Fatalf("concurrent reservation returned database error: %v", result.err)
		}
	}
	if first.ok == second.ok {
		t.Fatalf("concurrent parent/child reservations must have exactly one winner: first=%+v second=%+v", first, second)
	}
	loser := first
	if first.ok {
		loser = second
	}
	if !strings.Contains(loser.detail, "overlapping target") {
		t.Fatalf("losing reservation did not report subtree ownership: %+v", loser)
	}
}

func TestRootCreateAndFollowedReservationSerializeSubtreeOwnership(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	claimStore, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = claimStore.Close() }()
	rootStore, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rootStore.Close() }()
	rootA, err := claimStore.CreateSyncRoot(ctx, testSyncRoot("claim-root", filepath.Join(t.TempDir(), "root-a"), domain.AppRemoteName, "Personal/Claim"))
	if err != nil {
		t.Fatal(err)
	}
	shared := filepath.Join(t.TempDir(), "shared")
	candidate := testSyncRoot("candidate-root", filepath.Join(shared, "peer-root"), domain.AppRemoteName, "Personal/Candidate")
	type claimResult struct {
		ok     bool
		detail string
		err    error
	}
	claimCh := make(chan claimResult, 1)
	rootCh := make(chan error, 1)
	start := make(chan struct{})
	go func() {
		<-start
		ok, detail, err := claimStore.ReserveFollowedPhysicalClaims(ctx, rootA.ID, map[string]domain.FollowedPhysicalClaim{
			"parent": {Kind: domain.KindDir, Identity: "linux:49:claim-parent", TargetPath: shared},
		})
		claimCh <- claimResult{ok: ok, detail: detail, err: err}
	}()
	go func() {
		<-start
		_, err := rootStore.CreateSyncRoot(ctx, candidate)
		rootCh <- err
	}()
	close(start)
	claim := <-claimCh
	rootErr := <-rootCh
	if claim.err != nil {
		t.Fatalf("claim reservation returned database error: %v", claim.err)
	}
	rootCreated := rootErr == nil
	if claim.ok == rootCreated {
		t.Fatalf("claim reservation and overlapping root creation must have exactly one winner: claim=%+v rootErr=%v", claim, rootErr)
	}
	if !claim.ok && !strings.Contains(claim.detail, "overlapping configured sync root") {
		t.Fatalf("losing claim did not report configured-root subtree conflict: %+v", claim)
	}
	if !rootCreated && !strings.Contains(rootErr.Error(), "overlaps followed") {
		t.Fatalf("losing root creation did not report followed subtree conflict: %v", rootErr)
	}
}

func newCopyPhysicalDeleteOperation(t *testing.T, ctx context.Context, s *Store, rootID int64, relPath string) domain.Operation {
	t.Helper()
	op, err := s.CreateOperation(ctx, domain.Operation{
		SyncRootID: rootID,
		Kind:       domain.OperationDeleteLocal,
		EntryKind:  domain.KindFile,
		SrcPath:    relPath,
		ExpectedLocal: domain.LocalFingerprint{
			Present: true,
			Kind:    domain.KindFile,
			Size:    4,
			MtimeNS: 40,
		},
		ExpectedRemote: domain.RemoteExpectation{Absent: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func createCopyModeTestRoot(t *testing.T, ctx context.Context, s *Store, uuid, localRoot, remoteRoot string) domain.SyncRoot {
	t.Helper()
	root := testSyncRoot(uuid, localRoot, domain.AppRemoteName, remoteRoot)
	root.SymlinkMode = domain.SymlinkCopy
	created, err := s.CreateSyncRoot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	return created
}

func TestCopyPhysicalPinBlocksOverlappingRootCreateUntilOperationEnds(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	rootA := createCopyModeTestRoot(t, ctx, s, "copy-pin-owner", t.TempDir(), "Personal/CopyPinOwner")
	futurePeer := t.TempDir()
	target := filepath.Join(futurePeer, "x.txt")
	op := newCopyPhysicalDeleteOperation(t, ctx, s, rootA.ID, "alias/x.txt")

	ok, detail, err := s.AuthorizeAndPinLocalMutation(ctx, op.ID, domain.LocalMutationTarget{
		Path:           target,
		AnchorIdentity: "copy-pin-anchor",
		Authority:      domain.LocalMutationCopyPhysical,
	})
	if err != nil || !ok {
		t.Fatalf("pin copy-physical operation = ok=%v detail=%q err=%v", ok, detail, err)
	}
	candidate := testSyncRoot("copy-pin-peer", futurePeer, domain.AppRemoteName, "Personal/CopyPinPeer")
	if _, err := s.CreateSyncRoot(ctx, candidate); err == nil || !strings.Contains(err.Error(), "copy-physical") {
		t.Fatalf("overlapping root creation was not fenced by in-flight copy pin: %v", err)
	}
	if err := s.DeleteOperation(ctx, op.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSyncRoot(ctx, candidate); err != nil {
		t.Fatalf("completed copy operation kept root-lifetime ownership: %v", err)
	}
}

func TestExistingRootBlocksOverlappingCopyPhysicalPin(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	rootA := createCopyModeTestRoot(t, ctx, s, "copy-pin-after-root", t.TempDir(), "Personal/CopyPinAfterRoot")
	peer := t.TempDir()
	if _, err := s.CreateSyncRoot(ctx, testSyncRoot("copy-existing-peer", peer, domain.AppRemoteName, "Personal/CopyExistingPeer")); err != nil {
		t.Fatal(err)
	}
	op := newCopyPhysicalDeleteOperation(t, ctx, s, rootA.ID, "alias/x.txt")
	ok, detail, err := s.AuthorizeAndPinLocalMutation(ctx, op.ID, domain.LocalMutationTarget{
		Path:           filepath.Join(peer, "x.txt"),
		AnchorIdentity: "copy-pin-anchor",
		Authority:      domain.LocalMutationCopyPhysical,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ok || !strings.Contains(detail, "configured sync root") {
		t.Fatalf("copy pin crossed existing peer root: ok=%v detail=%q", ok, detail)
	}
}

func TestCopyPhysicalDirectoryPinBlocksDescendantRootCreate(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	rootA := createCopyModeTestRoot(t, ctx, s, "copy-dir-pin-owner", t.TempDir(), "Personal/CopyDirPinOwner")
	shared := t.TempDir()
	op, err := s.CreateOperation(ctx, domain.Operation{
		SyncRootID:     rootA.ID,
		Kind:           domain.OperationDeleteLocal,
		EntryKind:      domain.KindDir,
		SrcPath:        "alias/subdir",
		ExpectedLocal:  domain.LocalFingerprint{Present: true, Kind: domain.KindDir},
		ExpectedRemote: domain.RemoteExpectation{Absent: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	ok, detail, err := s.AuthorizeAndPinLocalMutation(ctx, op.ID, domain.LocalMutationTarget{
		Path:           shared,
		AnchorIdentity: "copy-dir-anchor",
		Authority:      domain.LocalMutationCopyPhysical,
	})
	if err != nil || !ok {
		t.Fatalf("pin copy-physical directory = ok=%v detail=%q err=%v", ok, detail, err)
	}
	candidatePath := filepath.Join(shared, "nested-root")
	if err := os.Mkdir(candidatePath, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSyncRoot(ctx, testSyncRoot("copy-dir-descendant-peer", candidatePath, domain.AppRemoteName, "Personal/CopyDirDescendantPeer")); err == nil || !strings.Contains(err.Error(), "copy-physical") {
		t.Fatalf("root inside copy-physical directory pin was not fenced: %v", err)
	}
}

func TestExistingDescendantRootBlocksParentCopyPhysicalDirectoryPin(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	rootA := createCopyModeTestRoot(t, ctx, s, "copy-dir-pin-after-root", t.TempDir(), "Personal/CopyDirPinAfterRoot")
	shared := t.TempDir()
	peer := filepath.Join(shared, "nested-root")
	if err := os.Mkdir(peer, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSyncRoot(ctx, testSyncRoot("copy-existing-descendant-peer", peer, domain.AppRemoteName, "Personal/CopyExistingDescendantPeer")); err != nil {
		t.Fatal(err)
	}
	op, err := s.CreateOperation(ctx, domain.Operation{
		SyncRootID:     rootA.ID,
		Kind:           domain.OperationDeleteLocal,
		EntryKind:      domain.KindDir,
		SrcPath:        "alias/subdir",
		ExpectedLocal:  domain.LocalFingerprint{Present: true, Kind: domain.KindDir},
		ExpectedRemote: domain.RemoteExpectation{Absent: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	ok, detail, err := s.AuthorizeAndPinLocalMutation(ctx, op.ID, domain.LocalMutationTarget{
		Path:           shared,
		AnchorIdentity: "copy-parent-dir-anchor",
		Authority:      domain.LocalMutationCopyPhysical,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ok || !strings.Contains(detail, "configured sync root") {
		t.Fatalf("parent copy directory pin crossed existing descendant root: ok=%v detail=%q", ok, detail)
	}
}

func TestCopyPhysicalPinAndRootCreateSerializeWithExactlyOneWinner(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	pinStore, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pinStore.Close() }()
	rootStore, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rootStore.Close() }()

	rootA := createCopyModeTestRoot(t, ctx, pinStore, "copy-concurrent-owner", t.TempDir(), "Personal/CopyConcurrentOwner")
	futurePeer := t.TempDir()
	target := filepath.Join(futurePeer, "x.txt")
	op := newCopyPhysicalDeleteOperation(t, ctx, pinStore, rootA.ID, "alias/x.txt")
	candidate := testSyncRoot("copy-concurrent-peer", futurePeer, domain.AppRemoteName, "Personal/CopyConcurrentPeer")

	type pinResult struct {
		ok     bool
		detail string
		err    error
	}
	pinCh := make(chan pinResult, 1)
	rootCh := make(chan error, 1)
	start := make(chan struct{})
	go func() {
		<-start
		ok, detail, err := pinStore.AuthorizeAndPinLocalMutation(ctx, op.ID, domain.LocalMutationTarget{
			Path:           target,
			AnchorIdentity: "copy-concurrent-anchor",
			Authority:      domain.LocalMutationCopyPhysical,
		})
		pinCh <- pinResult{ok: ok, detail: detail, err: err}
	}()
	go func() {
		<-start
		_, err := rootStore.CreateSyncRoot(ctx, candidate)
		rootCh <- err
	}()
	close(start)
	pin := <-pinCh
	rootErr := <-rootCh
	if pin.err != nil {
		t.Fatalf("copy pin returned database error: %v", pin.err)
	}
	rootCreated := rootErr == nil
	if pin.ok == rootCreated {
		t.Fatalf("copy pin and overlapping root creation must have exactly one winner: pin=%+v rootErr=%v", pin, rootErr)
	}
	if !pin.ok && !strings.Contains(pin.detail, "configured sync root") {
		t.Fatalf("losing copy pin did not report configured-root conflict: %+v", pin)
	}
	if !rootCreated && !strings.Contains(rootErr.Error(), "copy-physical") {
		t.Fatalf("losing root creation did not report copy-pin conflict: %v", rootErr)
	}
}

func TestReserveFollowedPhysicalClaimsBackfillsLegacyDirectoryTargetPath(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	root := createTestRoot(t, s)
	target := filepath.Join(t.TempDir(), "legacy-target")
	claim := domain.FollowedPhysicalClaim{Kind: domain.KindDir, Identity: "linux:49:legacy-dir", TargetPath: target}
	if ok, detail, err := s.ReserveFollowedPhysicalClaims(ctx, root.ID, map[string]domain.FollowedPhysicalClaim{"link": claim}); err != nil || !ok {
		t.Fatalf("reserve original claim = ok=%v detail=%q err=%v", ok, detail, err)
	}
	if err := s.InitializeSyncRoot(ctx, root.ID, map[string]domain.FollowedPhysicalClaim{"link": claim}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE followed_physical_claims SET physical_target_path = '' WHERE sync_root_id = ? AND rel_path = 'link'`, root.ID); err != nil {
		t.Fatal(err)
	}

	if ok, detail, err := s.ReserveFollowedPhysicalClaims(ctx, root.ID, map[string]domain.FollowedPhysicalClaim{"link": claim}); err != nil || !ok {
		t.Fatalf("backfill legacy target = ok=%v detail=%q err=%v", ok, detail, err)
	}
	got, err := s.ListFollowedPhysicalClaims(ctx, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got["link"] != claim {
		t.Fatalf("backfilled claim = %+v, want %+v", got["link"], claim)
	}
}

func TestReserveFollowedPhysicalClaimsBlocksUnavailableLegacyDirectoryTargetPath(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	root := createTestRoot(t, s)
	target := filepath.Join(t.TempDir(), "legacy-target")
	claim := domain.FollowedPhysicalClaim{Kind: domain.KindDir, Identity: "linux:49:legacy-dir-unavailable", TargetPath: target}
	if ok, detail, err := s.ReserveFollowedPhysicalClaims(ctx, root.ID, map[string]domain.FollowedPhysicalClaim{"link": claim}); err != nil || !ok {
		t.Fatalf("reserve original claim = ok=%v detail=%q err=%v", ok, detail, err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE followed_physical_claims SET physical_target_path = '' WHERE sync_root_id = ? AND rel_path = 'link'`, root.ID); err != nil {
		t.Fatal(err)
	}

	ok, detail, err := s.ReserveFollowedPhysicalClaims(ctx, root.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ok || !strings.Contains(detail, "unknown physical target") {
		t.Fatalf("unavailable legacy boundary was not fail-closed: ok=%v detail=%q", ok, detail)
	}
}
