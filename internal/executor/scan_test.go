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
	got, excluded, err := exec.ScanLocal(context.Background())
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
	if _, _, err := exec.ScanLocal(context.Background()); err == nil {
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
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	got, excluded, err := exec.ScanLocal(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 || len(excluded) != 0 {
		t.Fatalf("journaled operation temp leaked into snapshot: got=%+v excluded=%v", got, excluded)
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
	if _, _, err := exec.ScanLocal(context.Background()); err == nil {
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
	got, excluded, err := exec.ScanLocal(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(excluded) != 0 {
		t.Fatalf("unexpected exclusions: %v", excluded)
	}
	if got["link.txt"].Kind != domain.KindFile || got["link.txt"].Size != int64(len("outside")) {
		t.Fatalf("followed file fingerprint = %+v", got["link.txt"])
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
	got, excluded, err := exec.ScanLocal(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(excluded) != 0 {
		t.Fatalf("unexpected exclusions: %v", excluded)
	}
	if got["linked"].Kind != domain.KindDir || got["linked/note.txt"].Size != 5 {
		t.Fatalf("followed directory snapshot = %+v", got)
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
	got, excluded, err := exec.ScanLocal(context.Background())
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
	got, excluded, err := exec.ScanLocal(context.Background())
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

func TestScanLocalFollowTreatsResolvableDanglingFinalSymlinkAsAbsence(t *testing.T) {
	root := t.TempDir()
	if err := os.Symlink("missing.txt", filepath.Join(root, "link.txt")); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	got, excluded, err := exec.ScanLocal(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 || len(excluded) != 0 {
		t.Fatalf("dangling final symlink snapshot=%+v excluded=%v", got, excluded)
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
	got, excluded, err := exec.ScanLocal(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 || len(excluded) != 1 || excluded[0] != "linked" {
		t.Fatalf("ignore snapshot=%+v excluded=%v", got, excluded)
	}
}

func TestScanLocalRejectsMarkerDirectory(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, rootmarker.FileName), 0o755); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	if _, _, err := exec.ScanLocal(context.Background()); err == nil {
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
	if _, _, err := exec.ScanLocal(context.Background()); err == nil {
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

type listOnlyFS struct {
	fs.Fs
	list func(context.Context, string) (fs.DirEntries, error)
}

func (f *listOnlyFS) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	return f.list(ctx, dir)
}
