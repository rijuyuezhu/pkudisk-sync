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

func TestInstallDownloadedPhysicalTempPreservesFollowedFileSymlink(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	target := filepath.Join(outside, "target.txt")
	if err := os.WriteFile(target, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link.txt")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkFollow}}
	expected, err := exec.ObserveLocalEntry(context.Background(), "link.txt")
	if err != nil {
		t.Fatal(err)
	}
	temp := filepath.Join(outside, "staged")
	if err := os.WriteFile(temp, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	recovery := filepath.Join(outside, "recovery")
	if err := exec.installDownloadedPhysicalTemp(context.Background(), "link.txt", target, temp, recovery, expected); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(link)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("remote update replaced the symlink object")
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Fatalf("followed target content = %q, want new", got)
	}
}

func TestDeleteLocalPreservesSymlinkAndDeletesPinnedTarget(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	target := filepath.Join(outside, "target.txt")
	if err := os.WriteFile(target, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link.txt")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkFollow}}
	expected, err := exec.ObserveLocalEntry(context.Background(), "link.txt")
	if err != nil {
		t.Fatal(err)
	}
	op := domain.Operation{ID: 41, Kind: domain.OperationDeleteLocal, EntryKind: domain.KindFile, SrcPath: "link.txt", LocalTargetPath: target, ExpectedLocal: expected}
	if err := exec.DeleteLocal(context.Background(), op); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(link)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("remote delete removed the symlink object")
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("followed target still exists: %v", err)
	}
	if _, err := os.Lstat(operationPhysicalTempPath(target, op.ID, "recovery")); !os.IsNotExist(err) {
		t.Fatalf("successful delete left recovery artifact: %v", err)
	}
}

func TestDanglingFollowedSymlinkCanRecreateTargetWithoutReplacingLink(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	target := filepath.Join(outside, "missing.txt")
	link := filepath.Join(root, "link.txt")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkFollow}}
	resolved, err := exec.ResolveLocalMutationTarget(context.Background(), "link.txt", domain.LocalFingerprint{})
	if err != nil {
		t.Fatal(err)
	}
	if !samePhysicalDestination(resolved, target, false) {
		t.Fatalf("dangling symlink target = %q, want %q", resolved, target)
	}
	temp := filepath.Join(outside, "staged")
	if err := os.WriteFile(temp, []byte("restored"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := exec.installDownloadedPhysicalTemp(context.Background(), "link.txt", resolved, temp, filepath.Join(outside, "unused-recovery"), domain.LocalFingerprint{}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(link)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("recreate replaced dangling symlink")
	}
	got, err := os.ReadFile(link)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "restored" {
		t.Fatalf("recreated target content = %q", got)
	}
}

func TestDeleteLocalRejectsSymlinkRetargetEvenWithEquivalentContent(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	targetA := filepath.Join(outside, "a.txt")
	targetB := filepath.Join(outside, "b.txt")
	for _, name := range []string{targetA, targetB} {
		if err := os.WriteFile(name, []byte("same"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// Match mtimes so the normal local fingerprint cannot distinguish targets.
	infoA, err := os.Stat(targetA)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(targetB, infoA.ModTime(), infoA.ModTime()); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link.txt")
	if err := os.Symlink(targetA, link); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkFollow}}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(targetB, link); err != nil {
		t.Fatal(err)
	}
	// Filesystems may round mtimes differently; make the expected fingerprint
	// exactly match the newly targeted file to isolate physical-identity fencing.
	expected, err := exec.ObserveLocalEntry(context.Background(), "link.txt")
	if err != nil {
		t.Fatal(err)
	}
	op := domain.Operation{ID: 42, Kind: domain.OperationDeleteLocal, EntryKind: domain.KindFile, SrcPath: "link.txt", LocalTargetPath: targetA, ExpectedLocal: expected}
	if err := exec.DeleteLocal(context.Background(), op); err == nil {
		t.Fatal("delete accepted a retargeted symlink")
	}
	for _, name := range []string{targetA, targetB} {
		if _, err := os.Stat(name); err != nil {
			t.Fatalf("retarget race mutated %q: %v", name, err)
		}
	}
}

func TestLocalRecoveryArtifactFindsPinnedOutsideRootSlot(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	target := filepath.Join(outside, "target.txt")
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkFollow}}
	op := domain.Operation{
		ID:              77,
		Kind:            domain.OperationDeleteLocal,
		EntryKind:       domain.KindFile,
		SrcPath:         "link.txt",
		LocalTargetPath: target,
		ExpectedLocal:   domain.LocalFingerprint{Present: true, Kind: domain.KindFile, Size: 3, MtimeNS: 1},
	}
	artifact := operationPhysicalTempPath(target, op.ID, "recovery")
	if err := os.WriteFile(artifact, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}

	got, present, err := exec.LocalRecoveryArtifact(context.Background(), op)
	if err != nil {
		t.Fatal(err)
	}
	if !present || got != artifact {
		t.Fatalf("recovery artifact = %q present=%v, want %q true", got, present, artifact)
	}
}

func TestRestoreLocalRecoveryArtifactRestoresMatchingArtifactNoReplace(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	target := filepath.Join(root, "target.txt")
	if err := os.WriteFile(target, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	expected, err := exec.ObserveLocalEntry(ctx, "target.txt")
	if err != nil {
		t.Fatal(err)
	}
	op := domain.Operation{
		ID:              91,
		Kind:            domain.OperationDeleteLocal,
		EntryKind:       domain.KindFile,
		SrcPath:         "target.txt",
		LocalTargetPath: target,
		ExpectedLocal:   expected,
		Phase:           domain.OperationBlocked,
		Attempts:        1,
	}
	artifact, _ := RecoveryArtifactPath(op)
	if err := movePathNoReplace(target, artifact); err != nil {
		t.Fatal(err)
	}
	if _, err := RestoreLocalRecoveryArtifact(ctx, op); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(target)
	if err != nil || string(got) != "old" {
		t.Fatalf("restored target = %q err=%v", got, err)
	}
	if _, err := os.Lstat(artifact); !os.IsNotExist(err) {
		t.Fatalf("recovery artifact remains after restore: %v", err)
	}
}

func TestRestoreLocalRecoveryArtifactRefusesExistingTarget(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	target := filepath.Join(root, "target.txt")
	if err := os.WriteFile(target, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	expected, err := exec.ObserveLocalEntry(ctx, "target.txt")
	if err != nil {
		t.Fatal(err)
	}
	op := domain.Operation{ID: 92, Kind: domain.OperationDeleteLocal, EntryKind: domain.KindFile, SrcPath: "target.txt", LocalTargetPath: target, ExpectedLocal: expected, Phase: domain.OperationBlocked, Attempts: 1}
	artifact, _ := RecoveryArtifactPath(op)
	if err := os.WriteFile(artifact, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(artifact, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if _, err := RestoreLocalRecoveryArtifact(ctx, op); err == nil {
		t.Fatal("restore overwrote an existing pinned target")
	}
	for _, path := range []string{target, artifact} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("restore refusal removed %q: %v", path, err)
		}
	}
}
