package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
	"github.com/rijuyuezhu/pkudisk-sync/internal/rootmarker"
)

func TestScanLocalIncludesOrdinaryTreeAndExcludesRootMarker(t *testing.T) {
	root := t.TempDir()
	ordinaryPrefixFile := tempNamePrefix + "notes"
	if err := os.Mkdir(filepath.Join(root, "docs"), 0o755); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "docs", "a.txt")
	if err := os.WriteFile(file, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, rootmarker.FileName), []byte("uuid\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "docs", ordinaryPrefixFile), []byte("user"), 0o600); err != nil {
		t.Fatal(err)
	}

	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	got, excluded, _, _, err := exec.ScanLocal(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(excluded) != 0 {
		t.Fatalf("unexpected exclusions: %v", excluded)
	}
	if len(got) != 3 {
		t.Fatalf("ScanLocal() returned %d entries, want 3: %+v", len(got), got)
	}
	if got["docs"].Kind != domain.KindDir || !got["docs"].Present {
		t.Fatalf("directory fingerprint = %+v", got["docs"])
	}
	if got["docs/a.txt"].Kind != domain.KindFile || got["docs/a.txt"].Size != 5 {
		t.Fatalf("file fingerprint = %+v", got["docs/a.txt"])
	}
	if _, ok := got[rootmarker.FileName]; ok {
		t.Fatal("root marker leaked into snapshot")
	}
	if got["docs/"+ordinaryPrefixFile].Kind != domain.KindFile {
		t.Fatalf("ordinary prefix file was silently filtered: %+v", got)
	}
}

func TestScanLocalRejectsStaleInternalTempFile(t *testing.T) {
	root := t.TempDir()
	name := tempNamePrefix + "0123456789abcdef01234567"
	if err := os.WriteFile(filepath.Join(root, name), []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	if _, _, _, _, err := exec.ScanLocal(context.Background(), nil, nil); err == nil {
		t.Fatal("stale internal temp file was silently omitted from a complete snapshot")
	}
}

func TestScanLocalSkipsJournaledOperationTempFile(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target.txt")
	name := operationPhysicalTempPath(target, 17, "recovery")
	if err := os.WriteFile(name, []byte("preserved"), 0o600); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{ID: 1, LocalRoot: root}}
	operations := []domain.Operation{{
		ID:              17,
		SyncRootID:      1,
		Kind:            domain.OperationDeleteLocal,
		LocalTargetPath: target,
		Phase:           domain.OperationRecovering,
		Attempts:        1,
	}}
	got, excluded, _, _, err := exec.ScanLocal(context.Background(), operations, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 || len(excluded) != 0 {
		t.Fatalf("journaled operation temp leaked into snapshot: got=%+v excluded=%v", got, excluded)
	}
}

func TestScanLocalPlannedOperationDoesNotOwnTempArtifact(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target.txt")
	name := operationPhysicalTempPath(target, 17, "recovery")
	if err := os.WriteFile(name, []byte("user"), 0o600); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{ID: 1, LocalRoot: root}}
	operations := []domain.Operation{{
		ID:              17,
		SyncRootID:      1,
		Kind:            domain.OperationDeleteLocal,
		LocalTargetPath: target,
		Phase:           domain.OperationPlanned,
		Attempts:        1,
	}}
	if _, _, _, _, err := exec.ScanLocal(context.Background(), operations, nil); err == nil {
		t.Fatal("planned operation incorrectly owned an operation-shaped artifact")
	}
}

func TestScanLocalRejectsUnownedOperationShapedFile(t *testing.T) {
	root := t.TempDir()
	name := operationPhysicalTempPath(filepath.Join(root, "target.txt"), 17, "recovery")
	if err := os.WriteFile(name, []byte("user"), 0o600); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	if _, _, _, _, err := exec.ScanLocal(context.Background(), nil, nil); err == nil {
		t.Fatal("operation-shaped user file was silently hidden without journal ownership")
	}
}

func TestScanLocalRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "target"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("target", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkReject}}
	if _, _, _, _, err := exec.ScanLocal(context.Background(), nil, nil); err == nil {
		t.Fatal("reject policy accepted a symlink")
	}
}

func TestScanLocalDefaultFollowsFileSymlinkOutsideRoot(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	target := filepath.Join(outside, "target.txt")
	if err := os.WriteFile(target, []byte("outside"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "link.txt")); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	got, excluded, claims, _, err := exec.ScanLocal(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(excluded) != 0 {
		t.Fatalf("unexpected exclusions: %v", excluded)
	}
	if got["link.txt"].Kind != domain.KindFile || got["link.txt"].Size != int64(len("outside")) {
		t.Fatalf("followed file fingerprint = %+v", got["link.txt"])
	}
	resolvedTarget, err := filepath.EvalSymlinks(target)
	if err != nil {
		t.Fatal(err)
	}
	if claim := claims["link.txt"]; claim.Kind != domain.KindFile || claim.Identity == "" || claim.TargetPath != resolvedTarget || len(claims) != 1 {
		t.Fatalf("followed file claim = %+v all=%+v", claim, claims)
	}
}

func TestScanLocalDefaultFollowsDirectorySymlinkOutsideRoot(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "note.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	got, excluded, boundaries, _, err := exec.ScanLocal(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(excluded) != 0 {
		t.Fatalf("unexpected exclusions: %v", excluded)
	}
	if got["linked"].Kind != domain.KindDir || got["linked/note.txt"].Size != 5 {
		t.Fatalf("followed directory snapshot = %+v", got)
	}
	resolvedOutside, err := filepath.EvalSymlinks(outside)
	if err != nil {
		t.Fatal(err)
	}
	if claim := boundaries["linked"]; claim.Kind != domain.KindDir || claim.Identity == "" || claim.TargetPath != resolvedOutside || len(boundaries) != 1 {
		t.Fatalf("followed directory claims = %+v", boundaries)
	}
}

func TestScanLocalFollowExcludesSelfAndMutualCycles(t *testing.T) {
	root := t.TempDir()
	if err := os.Symlink("self", filepath.Join(root, "self")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("b", filepath.Join(root, "a")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("a", filepath.Join(root, "b")); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	got, excluded, _, _, err := exec.ScanLocal(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("cycle leaked entries: %+v", got)
	}
	want := map[string]bool{"a": true, "b": true, "self": true}
	for _, rel := range excluded {
		delete(want, rel)
	}
	if len(want) != 0 {
		t.Fatalf("missing cycle exclusions %v; got %v", want, excluded)
	}
}

func TestScanLocalFollowExcludesLinkToPhysicalParent(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	// From root/sub, ../.. resolves to the parent of the sync root. Descending
	// there would re-enter the sync root and recurse forever through ordinary dirs.
	if err := os.Symlink(filepath.Join("..", ".."), filepath.Join(root, "sub", "up")); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	got, excluded, _, _, err := exec.ScanLocal(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got["sub"].Kind != domain.KindDir {
		t.Fatalf("ordinary parent dir missing: %+v", got)
	}
	if len(excluded) != 1 || excluded[0] != "sub/up" {
		t.Fatalf("parent-link exclusions = %v, want [sub/up]", excluded)
	}
}

func TestScanLocalFollowExcludesResolvableDanglingFinalSymlink(t *testing.T) {
	root := t.TempDir()
	if err := os.Symlink("missing.txt", filepath.Join(root, "link.txt")); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	got, excluded, _, _, err := exec.ScanLocal(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 || len(excluded) != 1 || excluded[0] != "link.txt" {
		t.Fatalf("dangling final symlink snapshot=%+v excluded=%v", got, excluded)
	}
}

func TestScanLocalFollowRejectsDuplicatePhysicalOwnership(t *testing.T) {
	root := t.TempDir()
	real := filepath.Join(root, "real")
	if err := os.Mkdir(real, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(real, "x.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, filepath.Join(root, "alias")); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	if _, _, _, _, err := exec.ScanLocal(context.Background(), nil, nil); err == nil {
		t.Fatal("follow policy allowed two logical paths to own the same physical directory")
	}
}

func TestScanLocalCopyAllowsDuplicateAndInternalProjections(t *testing.T) {
	root := t.TempDir()
	real := filepath.Join(root, "real")
	if err := os.Mkdir(real, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(real, "x.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, filepath.Join(root, "internal")); err != nil {
		t.Fatal(err)
	}

	external := t.TempDir()
	if err := os.WriteFile(filepath.Join(external, "note.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(root, "alias-a")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(root, "alias-b")); err != nil {
		t.Fatal(err)
	}
	fileTarget := filepath.Join(external, "note.txt")
	if err := os.Symlink(fileTarget, filepath.Join(root, "file-a.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(fileTarget, filepath.Join(root, "file-b.txt")); err != nil {
		t.Fatal(err)
	}

	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkCopy}}
	got, excluded, claims, evidence, err := exec.ScanLocal(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(excluded) != 0 || len(claims) != 0 {
		t.Fatalf("copy exclusions=%v claims=%+v", excluded, claims)
	}
	for _, rel := range []string{"real/x.txt", "internal/x.txt", "alias-a/note.txt", "alias-b/note.txt", "file-a.txt", "file-b.txt"} {
		if !got[rel].Present || got[rel].Kind != domain.KindFile {
			t.Fatalf("copy projection %q = %+v; snapshot=%+v", rel, got[rel], got)
		}
	}
	for _, rel := range []string{"internal", "alias-a", "alias-b"} {
		item, ok := evidence[rel]
		if !ok || item.Kind != domain.KindDir || item.Identity == "" {
			t.Fatalf("copy directory evidence %q = %+v ok=%v", rel, item, ok)
		}
	}
	for _, rel := range []string{"file-a.txt", "file-b.txt"} {
		item, ok := evidence[rel]
		if !ok || item.Kind != domain.KindFile || item.Identity == "" || !samePhysicalDestination(item.TargetPath, fileTarget, false) {
			t.Fatalf("copy file evidence %q = %+v ok=%v", rel, item, ok)
		}
	}
}

func TestScanLocalCopyRetainsPeerRootFencing(t *testing.T) {
	root := t.TempDir()
	peer := t.TempDir()
	if err := os.WriteFile(filepath.Join(peer, "x.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(peer, filepath.Join(root, "peer")); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkCopy}}
	if _, _, _, _, err := exec.ScanLocal(context.Background(), nil, []string{peer}); err == nil {
		t.Fatal("copy projection crossed into configured peer root")
	}

	root2 := t.TempDir()
	container := t.TempDir()
	containedPeer := filepath.Join(container, "peer")
	if err := os.Mkdir(containedPeer, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(container, filepath.Join(root2, "container")); err != nil {
		t.Fatal(err)
	}
	exec2 := &RootExecutor{root: domain.SyncRoot{LocalRoot: root2, SymlinkMode: domain.SymlinkCopy}}
	if _, _, _, _, err := exec2.ScanLocal(context.Background(), nil, []string{containedPeer}); err == nil {
		t.Fatal("copy projection directory was allowed to contain a configured peer root")
	}
}

func TestScanLocalCopyExcludesDanglingAndCycles(t *testing.T) {
	root := t.TempDir()
	if err := os.Symlink("missing", filepath.Join(root, "dangling")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join("..", ".."), filepath.Join(root, "sub", "up")); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkCopy}}
	got, excluded, claims, _, err := exec.ScanLocal(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 0 || got["sub"].Kind != domain.KindDir {
		t.Fatalf("copy snapshot=%+v claims=%+v", got, claims)
	}
	want := map[string]bool{"dangling": true, "sub/up": true}
	for _, rel := range excluded {
		delete(want, rel)
	}
	if len(want) != 0 {
		t.Fatalf("copy exclusions missing %v; got %v", want, excluded)
	}
}

func TestCopyScanEvidenceDetectsSameFingerprintRetargetBeforePin(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	targetA := t.TempDir()
	targetB := t.TempDir()
	fileA := filepath.Join(targetA, "x.txt")
	fileB := filepath.Join(targetB, "x.txt")
	for _, name := range []string{fileA, fileB} {
		if err := os.WriteFile(name, []byte("same"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	infoA, err := os.Stat(fileA)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(fileB, infoA.ModTime(), infoA.ModTime()); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(targetA, alias); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkCopy}}
	local, _, _, scanned, err := exec.ScanLocal(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	expected := local["alias/x.txt"]
	if !expected.Present || expected.Kind != domain.KindFile {
		t.Fatalf("scan fingerprint = %+v", expected)
	}
	scanBoundary, ok := scanned["alias"]
	if !ok || scanBoundary.Kind != domain.KindDir {
		t.Fatalf("scan copy evidence = %+v", scanned)
	}
	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(targetB, alias); err != nil {
		t.Fatal(err)
	}
	target, err := exec.ResolveLocalMutationTarget(ctx, "alias/x.txt", expected, domain.KindFile, nil)
	if err != nil {
		t.Fatal(err)
	}
	currentBoundary, ok := target.CopyProjectionEvidence["alias"]
	if !ok {
		t.Fatalf("pin resolver omitted copy evidence: %+v", target)
	}
	if scanBoundary == currentBoundary {
		t.Fatalf("scan-to-pin retarget kept identical authority evidence: scan=%+v current=%+v", scanBoundary, currentBoundary)
	}
	if !samePhysicalDestination(target.Path, fileB, false) {
		t.Fatalf("resolver did not observe retargeted pathname: got %q want %q", target.Path, fileB)
	}
}

func TestScanLocalCopyRejectsForeignRootMarker(t *testing.T) {
	root := t.TempDir()
	foreign := t.TempDir()
	if err := os.WriteFile(filepath.Join(foreign, rootmarker.FileName), []byte("foreign\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(foreign, "x.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(foreign, filepath.Join(root, "foreign")); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkCopy}}
	if _, _, _, _, err := exec.ScanLocal(context.Background(), nil, nil); err == nil {
		t.Fatal("copy projection crossed a foreign sync-root marker")
	}
}

func TestScanLocalFollowRejectsTargetInsideForeignSyncRoot(t *testing.T) {
	root := t.TempDir()
	foreign := t.TempDir()
	if err := os.WriteFile(filepath.Join(foreign, rootmarker.FileName), []byte("foreign\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(foreign, "x.txt")
	if err := os.WriteFile(target, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "link.txt")); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	if _, _, _, _, err := exec.ScanLocal(context.Background(), nil, nil); err == nil {
		t.Fatal("follow policy crossed into another configured sync root")
	}
}

func TestScanLocalFollowRejectsConfiguredPeerRootWithoutMarker(t *testing.T) {
	root := t.TempDir()
	peer := t.TempDir()
	target := filepath.Join(peer, "x.txt")
	if err := os.WriteFile(target, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "link.txt")); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	if _, _, _, _, err := exec.ScanLocal(context.Background(), nil, []string{peer}); err == nil {
		t.Fatal("follow policy crossed into a configured peer root without relying on its marker")
	}
}

func TestScanLocalIgnoreExcludesSymlinkPrefix(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "note.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkIgnore}}
	got, excluded, boundaries, _, err := exec.ScanLocal(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 || len(excluded) != 1 || excluded[0] != "linked" || len(boundaries) != 0 {
		t.Fatalf("ignore snapshot=%+v excluded=%v boundaries=%+v", got, excluded, boundaries)
	}
}

func TestScanLocalIgnoreDoesNotResolvePeerRootSymlink(t *testing.T) {
	root := t.TempDir()
	peer := t.TempDir()
	if err := os.Symlink(peer, filepath.Join(root, "ignored")); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkIgnore}}
	got, excluded, boundaries, _, err := exec.ScanLocal(context.Background(), nil, []string{peer})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 || len(excluded) != 1 || excluded[0] != "ignored" || len(boundaries) != 0 {
		t.Fatalf("ignore peer snapshot=%+v excluded=%v boundaries=%+v", got, excluded, boundaries)
	}
}

func TestScanLocalIgnoreDoesNotResolveDanglingSymlinkWithMissingPeer(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "root")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(base, "missing-target"), filepath.Join(root, "ignored")); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkIgnore}}
	_, excluded, boundaries, _, err := exec.ScanLocal(context.Background(), nil, []string{filepath.Join(base, "missing-peer")})
	if err != nil {
		t.Fatal(err)
	}
	if len(excluded) != 1 || excluded[0] != "ignored" || len(boundaries) != 0 {
		t.Fatalf("ignore dangling exclusions=%v boundaries=%+v", excluded, boundaries)
	}
}

func TestScanLocalFollowIgnoresUnavailableUnrelatedPeerRoot(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "root")
	outside := filepath.Join(base, "outside")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "x.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	got, excluded, boundaries, _, err := exec.ScanLocal(context.Background(), nil, []string{filepath.Join(base, "missing-peer")})
	if err != nil {
		t.Fatal(err)
	}
	if len(excluded) != 0 || got["linked/x.txt"].Size != 1 || boundaries["linked"].Identity == "" {
		t.Fatalf("follow with unavailable peer snapshot=%+v excluded=%v boundaries=%+v", got, excluded, boundaries)
	}
}

func TestScanLocalRejectsMarkerDirectory(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, rootmarker.FileName), 0o755); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	if _, _, _, _, err := exec.ScanLocal(context.Background(), nil, nil); err == nil {
		t.Fatal("reserved marker directory accepted")
	}
}

func TestScanLocalRejectsInternalTempDirectory(t *testing.T) {
	root := t.TempDir()
	name := tempNamePrefix + "0123456789abcdef01234567"
	if err := os.Mkdir(filepath.Join(root, name), 0o755); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	if _, _, _, _, err := exec.ScanLocal(context.Background(), nil, nil); err == nil {
		t.Fatal("reserved internal temp directory accepted")
	}
}

func TestScanRemoteReportsMissingSelectedRootWithoutClaimingCompleteness(t *testing.T) {
	exec := &RootExecutor{remote: &listOnlyFS{list: func(_ context.Context, dir string) (fs.DirEntries, error) {
		if dir != "" {
			t.Fatalf("unexpected list dir %q", dir)
		}
		return nil, fs.ErrorDirNotFound
	}}}

	got, present, err := exec.ScanRemote(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if present {
		t.Fatal("missing selected remote root reported present")
	}
	if len(got) != 0 {
		t.Fatalf("missing selected remote root returned entries: %+v", got)
	}
}

func TestScanRemoteStillFailsWhenListedChildDisappears(t *testing.T) {
	child := fs.NewDir("child", time.Unix(1, 0)).SetID("child-id")
	exec := &RootExecutor{remote: &listOnlyFS{list: func(_ context.Context, dir string) (fs.DirEntries, error) {
		switch dir {
		case "":
			return fs.DirEntries{child}, nil
		case "child":
			return nil, fs.ErrorDirNotFound
		default:
			t.Fatalf("unexpected list dir %q", dir)
			return nil, nil
		}
	}}}

	got, present, err := exec.ScanRemote(context.Background())
	if err == nil || !errors.Is(err, fs.ErrorDirNotFound) {
		t.Fatalf("ScanRemote() error = %v, want wrapped ErrorDirNotFound", err)
	}
	if !present {
		t.Fatal("existing selected root reported absent after child disappeared")
	}
	if got != nil {
		t.Fatalf("incomplete remote scan returned a snapshot: %+v", got)
	}
}

func TestScanRemoteRejectsOperationReservedNamespace(t *testing.T) {
	reserved := fs.NewDir(".pkudisk-sync-tmp-op-12-recovery", time.Unix(1, 0)).SetID("reserved-id")
	exec := &RootExecutor{remote: &listOnlyFS{list: func(_ context.Context, dir string) (fs.DirEntries, error) {
		if dir != "" {
			t.Fatalf("unexpected list dir %q", dir)
		}
		return fs.DirEntries{reserved}, nil
	}}}
	if _, _, err := exec.ScanRemote(context.Background()); err == nil {
		t.Fatal("remote operation-reserved namespace was accepted")
	}
}

type listOnlyFS struct {
	fs.Fs
	list func(context.Context, string) (fs.DirEntries, error)
}

func (f *listOnlyFS) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	return f.list(ctx, dir)
}
