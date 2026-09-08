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
	got, err := exec.ScanLocal(context.Background())
	if err != nil {
		t.Fatal(err)
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
	if _, err := exec.ScanLocal(context.Background()); err == nil {
		t.Fatal("stale internal temp file was silently omitted from a complete snapshot")
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
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	if _, err := exec.ScanLocal(context.Background()); err == nil {
		t.Fatal("symlink was silently omitted from a complete snapshot")
	}
}

func TestScanLocalRejectsMarkerDirectory(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, rootmarker.FileName), 0o755); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	if _, err := exec.ScanLocal(context.Background()); err == nil {
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
	if _, err := exec.ScanLocal(context.Background()); err == nil {
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
