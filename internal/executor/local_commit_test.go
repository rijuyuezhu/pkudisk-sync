package executor

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

func TestInstallDownloadedTempAbsentNeverReplacesConcurrentCreate(t *testing.T) {
	root := t.TempDir()
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	tempRel := tempNamePrefix + "0123456789abcdef01234567"
	if err := os.WriteFile(filepath.Join(root, tempRel), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("concurrent-local"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := exec.installDownloadedTemp(context.Background(), "file.txt", tempRel, domain.LocalFingerprint{}); err == nil {
		t.Fatal("download commit replaced a concurrent local create")
	}
	got, err := os.ReadFile(filepath.Join(root, "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "concurrent-local" {
		t.Fatalf("concurrent local create changed to %q", got)
	}
	if _, err := os.Stat(filepath.Join(root, tempRel)); err != nil {
		t.Fatalf("download temp should remain available to caller after failed install: %v", err)
	}
}

func TestInstallDownloadedTempPresentRestoresUnexpectedActualTarget(t *testing.T) {
	root := t.TempDir()
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("baseline"), 0o600); err != nil {
		t.Fatal(err)
	}
	expected, err := exec.ObserveLocalEntry(ctx, "file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("concurrent-local-write"), 0o600); err != nil {
		t.Fatal(err)
	}
	tempRel := tempNamePrefix + "0123456789abcdef01234567"
	if err := os.WriteFile(filepath.Join(root, tempRel), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := exec.installDownloadedTemp(ctx, "file.txt", tempRel, expected); err == nil {
		t.Fatal("download commit accepted a changed path target")
	}
	got, err := os.ReadFile(filepath.Join(root, "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "concurrent-local-write" {
		t.Fatalf("changed local target was not restored: %q", got)
	}
}

func TestInstallDownloadedTempPresentReplacesValidatedTarget(t *testing.T) {
	root := t.TempDir()
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("baseline"), 0o600); err != nil {
		t.Fatal(err)
	}
	expected, err := exec.ObserveLocalEntry(ctx, "file.txt")
	if err != nil {
		t.Fatal(err)
	}
	tempRel := tempNamePrefix + "0123456789abcdef01234567"
	if err := os.WriteFile(filepath.Join(root, tempRel), []byte("remote-new"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := exec.installDownloadedTemp(ctx, "file.txt", tempRel, expected); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "remote-new" {
		t.Fatalf("installed content = %q", got)
	}
	if _, err := os.Stat(filepath.Join(root, tempRel)); !os.IsNotExist(err) {
		t.Fatalf("download temp still exists after successful install: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(root, tempNamePrefix+"*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("successful install left internal recovery files: %v", matches)
	}
}
