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
	if err := exec.installDownloadedPhysicalTemp(context.Background(), "link.txt", target, temp, recovery, expected, ""); err != nil {
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
	op := domain.Operation{
		ID:                  41,
		Kind:                domain.OperationDeleteLocal,
		EntryKind:           domain.KindFile,
		SrcPath:             "link.txt",
		LocalTargetPath:     target,
		LocalTargetIdentity: physicalIdentityForTest(t, target),
		ExpectedLocal:       expected,
	}
	if err := exec.DeleteLocal(context.Background(), op, nil); err != nil {
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

func TestCopyFileSymlinkRemoteUpdateMaterializesLexicalPath(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	external := t.TempDir()
	target := filepath.Join(external, "target.txt")
	if err := os.WriteFile(target, []byte("external-old"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link.txt")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkCopy}}
	expected, err := exec.ObserveLocalEntry(ctx, "link.txt")
	if err != nil {
		t.Fatal(err)
	}
	pin, err := exec.ResolveLocalMutationTarget(ctx, "link.txt", expected, domain.KindFile, nil)
	if err != nil {
		t.Fatal(err)
	}
	if pin.Path != link || pin.Authority != domain.LocalMutationLexical || pin.SymlinkTarget == "" {
		t.Fatalf("copy file-symlink pin = %+v", pin)
	}
	exec.observeRemoteFn = func(context.Context, string) (domain.RemoteFingerprint, error) {
		return domain.RemoteFingerprint{Present: true, Kind: domain.KindFile, ID: "doc", Rev: "rev", Size: 10}, nil
	}
	op := domain.Operation{
		ID:                   45,
		Kind:                 domain.OperationEnsureLocal,
		EntryKind:            domain.KindFile,
		SrcPath:              "link.txt",
		LocalTargetPath:      pin.Path,
		LocalTargetIdentity:  pin.AnchorIdentity,
		LocalTargetAuthority: pin.Authority,
		LocalSymlinkTarget:   pin.SymlinkTarget,
		ExpectedLocal:        expected,
		ExpectedRemote:       domain.RemoteExpectation{ID: "doc", Rev: "rev"},
	}
	temp := operationPhysicalTempPath(link, op.ID, "download")
	if err := os.WriteFile(temp, []byte("remote-new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := exec.commitDownloadedPhysicalTemp(ctx, op, temp, nil); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(link)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("copy update left lexical path as %s", info.Mode())
	}
	if got, err := os.ReadFile(link); err != nil || string(got) != "remote-new" {
		t.Fatalf("materialized content=%q err=%v", got, err)
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "external-old" {
		t.Fatalf("copy update changed former referent: content=%q err=%v", got, err)
	}
}

func TestCopyFileSymlinkRemoteDeleteRemovesOnlyLexicalObject(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	external := t.TempDir()
	target := filepath.Join(external, "target.txt")
	if err := os.WriteFile(target, []byte("external"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link.txt")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkCopy}}
	expected, err := exec.ObserveLocalEntry(ctx, "link.txt")
	if err != nil {
		t.Fatal(err)
	}
	pin, err := exec.ResolveLocalMutationTarget(ctx, "link.txt", expected, domain.KindFile, nil)
	if err != nil {
		t.Fatal(err)
	}
	op := domain.Operation{
		ID:                   46,
		Kind:                 domain.OperationDeleteLocal,
		EntryKind:            domain.KindFile,
		SrcPath:              "link.txt",
		LocalTargetPath:      pin.Path,
		LocalTargetIdentity:  pin.AnchorIdentity,
		LocalTargetAuthority: pin.Authority,
		LocalSymlinkTarget:   pin.SymlinkTarget,
		ExpectedLocal:        expected,
		ExpectedRemote:       domain.RemoteExpectation{Absent: true},
	}
	if err := exec.DeleteLocal(ctx, op, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(link); !os.IsNotExist(err) {
		t.Fatalf("copy delete retained lexical symlink: %v", err)
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "external" {
		t.Fatalf("copy delete changed referent: content=%q err=%v", got, err)
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
	resolved, err := exec.ResolveLocalMutationTarget(context.Background(), "link.txt", domain.LocalFingerprint{}, domain.KindFile, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !samePhysicalDestination(resolved.Path, target, false) {
		t.Fatalf("dangling symlink target = %q, want %q", resolved.Path, target)
	}
	temp := filepath.Join(outside, "staged")
	if err := os.WriteFile(temp, []byte("restored"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := exec.installDownloadedPhysicalTemp(context.Background(), "link.txt", resolved.Path, temp, filepath.Join(outside, "unused-recovery"), domain.LocalFingerprint{}, ""); err != nil {
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
	if err := exec.DeleteLocal(context.Background(), op, nil); err == nil {
		t.Fatal("delete accepted a retargeted symlink")
	}
	for _, name := range []string{targetA, targetB} {
		if _, err := os.Stat(name); err != nil {
			t.Fatalf("retarget race mutated %q: %v", name, err)
		}
	}
}

func TestCopyDescendantDeleteRejectsRetargetBeforeFirstSideEffect(t *testing.T) {
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
	expected, err := exec.ObserveLocalEntry(context.Background(), "alias/x.txt")
	if err != nil {
		t.Fatal(err)
	}
	target, err := exec.ResolveLocalMutationTarget(context.Background(), "alias/x.txt", expected, domain.KindFile, nil)
	if err != nil {
		t.Fatal(err)
	}
	if target.Authority != domain.LocalMutationCopyPhysical || target.Path != fileA {
		t.Fatalf("copy descendant pin = %+v, want copy-physical %q", target, fileA)
	}

	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(targetB, alias); err != nil {
		t.Fatal(err)
	}
	op := domain.Operation{
		ID:                   43,
		Kind:                 domain.OperationDeleteLocal,
		EntryKind:            domain.KindFile,
		SrcPath:              "alias/x.txt",
		LocalTargetPath:      target.Path,
		LocalTargetIdentity:  target.AnchorIdentity,
		LocalTargetAuthority: target.Authority,
		LocalSymlinkTarget:   target.SymlinkTarget,
		ExpectedLocal:        expected,
		ExpectedRemote:       domain.RemoteExpectation{Absent: true},
	}
	if err := exec.DeleteLocal(context.Background(), op, nil); err == nil {
		t.Fatal("copy descendant delete accepted retarget before first side effect")
	}
	for _, name := range []string{fileA, fileB} {
		if _, err := os.Stat(name); err != nil {
			t.Fatalf("retarget race mutated %q: %v", name, err)
		}
	}
}

func TestCopyDescendantMutationPinRejectsRetargetIntoPeerRoot(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	initial := t.TempDir()
	peer := t.TempDir()
	for _, dir := range []string{initial, peer} {
		if err := os.WriteFile(filepath.Join(dir, "x.txt"), []byte("same"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	initialInfo, err := os.Stat(filepath.Join(initial, "x.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(peer, "x.txt"), initialInfo.ModTime(), initialInfo.ModTime()); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(initial, alias); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkCopy}}
	expected, err := exec.ObserveLocalEntry(ctx, "alias/x.txt")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(peer, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := exec.ResolveLocalMutationTarget(ctx, "alias/x.txt", expected, domain.KindFile, []string{peer}); err == nil {
		t.Fatal("copy mutation pin accepted scan-to-pin retarget into peer root")
	}
	if got, err := os.ReadFile(filepath.Join(peer, "x.txt")); err != nil || string(got) != "same" {
		t.Fatalf("failed pin mutated peer content=%q err=%v", got, err)
	}
}

func TestCopyDescendantDeleteRecoveryUsesPinnedReferentAfterRetarget(t *testing.T) {
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
	expected, err := exec.ObserveLocalEntry(context.Background(), "alias/x.txt")
	if err != nil {
		t.Fatal(err)
	}
	target, err := exec.ResolveLocalMutationTarget(context.Background(), "alias/x.txt", expected, domain.KindFile, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(targetB, alias); err != nil {
		t.Fatal(err)
	}
	op := domain.Operation{
		ID:                   44,
		Kind:                 domain.OperationDeleteLocal,
		EntryKind:            domain.KindFile,
		SrcPath:              "alias/x.txt",
		LocalTargetPath:      target.Path,
		LocalTargetIdentity:  target.AnchorIdentity,
		LocalTargetAuthority: target.Authority,
		ExpectedLocal:        expected,
		ExpectedRemote:       domain.RemoteExpectation{Absent: true},
		Phase:                domain.OperationRecovering,
		Attempts:             1,
	}
	if err := exec.DeleteLocal(context.Background(), op, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(fileA); !os.IsNotExist(err) {
		t.Fatalf("recovery did not delete pinned referent %q: %v", fileA, err)
	}
	got, err := os.ReadFile(fileB)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "same" {
		t.Fatalf("recovery changed retargeted referent content to %q", got)
	}
	resolved, err := os.Readlink(alias)
	if err != nil {
		t.Fatal(err)
	}
	if resolved != targetB {
		t.Fatalf("recovery changed lexical alias target to %q, want %q", resolved, targetB)
	}
}

func TestDeleteLocalRejectsSamePathDirectoryReplacementAfterPin(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "dir")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	expected, err := exec.ObserveLocalEntry(context.Background(), "dir")
	if err != nil {
		t.Fatal(err)
	}
	pinnedIdentity := physicalIdentityForTest(t, target)
	oldTarget := filepath.Join(root, "old-dir")
	if err := os.Rename(target, oldTarget); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	op := domain.Operation{
		ID:                  43,
		Kind:                domain.OperationDeleteLocal,
		EntryKind:           domain.KindDir,
		SrcPath:             "dir",
		LocalTargetPath:     target,
		LocalTargetIdentity: pinnedIdentity,
		ExpectedLocal:       expected,
	}
	if err := exec.DeleteLocal(context.Background(), op, nil); err == nil {
		t.Fatal("delete accepted a same-path directory replacement after pin")
	}
	for _, name := range []string{target, oldTarget} {
		if info, err := os.Stat(name); err != nil || !info.IsDir() {
			t.Fatalf("identity fence mutated directory %q: info=%v err=%v", name, info, err)
		}
	}
}

func TestEnsureLocalDirRejectsParentReplacementAfterPin(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "root")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "new-dir")
	pinnedParentIdentity := physicalIdentityForTest(t, root)
	oldRoot := filepath.Join(base, "old-root")
	if err := os.Rename(root, oldRoot); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	op := domain.Operation{
		ID:                  44,
		Kind:                domain.OperationEnsureLocal,
		EntryKind:           domain.KindDir,
		SrcPath:             "new-dir",
		LocalTargetPath:     target,
		LocalTargetIdentity: pinnedParentIdentity,
		ExpectedLocal:       domain.LocalFingerprint{},
	}
	if err := exec.EnsureLocalDir(context.Background(), op, nil); err == nil {
		t.Fatal("directory create accepted a replaced physical parent after pin")
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("identity fence created target in replacement parent: %v", err)
	}
	if info, err := os.Stat(oldRoot); err != nil || !info.IsDir() {
		t.Fatalf("original pinned parent lost: info=%v err=%v", info, err)
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

func TestCleanupLocalRecoveryArtifactRemovesOnlyPinnedOperationSlot(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target.txt")
	op := domain.Operation{
		ID:              78,
		Kind:            domain.OperationDeleteLocal,
		EntryKind:       domain.KindFile,
		SrcPath:         "target.txt",
		LocalTargetPath: target,
	}
	artifact := operationPhysicalTempPath(target, op.ID, "recovery")
	if err := os.WriteFile(artifact, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	op.LocalTargetIdentity = physicalIdentityForTest(t, artifact)
	neighbor := filepath.Join(root, ".pkudisk-sync-tmp-op-79-recovery")
	if err := os.WriteFile(neighbor, []byte("other"), 0o600); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	if err := exec.CleanupLocalRecoveryArtifact(context.Background(), op); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(artifact); !os.IsNotExist(err) {
		t.Fatalf("owned recovery artifact remains: %v", err)
	}
	if _, err := os.Stat(neighbor); err != nil {
		t.Fatalf("cleanup touched unrelated artifact-shaped file: %v", err)
	}
	if err := exec.CleanupLocalRecoveryArtifact(context.Background(), op); err != nil {
		t.Fatalf("cleanup was not idempotent for absent artifact: %v", err)
	}
}

func TestCleanupLocalRecoveryArtifactRefusesReplacementIdentity(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target.txt")
	op := domain.Operation{
		ID:                  80,
		Kind:                domain.OperationDeleteLocal,
		EntryKind:           domain.KindFile,
		SrcPath:             "target.txt",
		LocalTargetPath:     target,
		LocalTargetIdentity: "not-the-artifact-identity",
	}
	artifact := operationPhysicalTempPath(target, op.ID, "recovery")
	if err := os.WriteFile(artifact, []byte("unrelated"), 0o600); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	if err := exec.CleanupLocalRecoveryArtifact(context.Background(), op); err == nil {
		t.Fatal("cleanup deleted a recovery-slot replacement without matching its pinned identity")
	}
	if got, err := os.ReadFile(artifact); err != nil || string(got) != "unrelated" {
		t.Fatalf("cleanup refusal did not preserve replacement artifact: content=%q err=%v", got, err)
	}
}

func TestPreserveExpectedLocalEntryRejectsWrongPinnedIdentityAndRestores(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	target := filepath.Join(root, "target.txt")
	recovery := filepath.Join(root, "recovery.txt")
	if err := os.WriteFile(target, []byte("same"), 0o600); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	expected, err := exec.ObserveLocalEntry(ctx, "target.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := preserveExpectedLocalEntryAt(ctx, "target.txt", target, recovery, expected, "definitely-not-the-current-identity"); err == nil {
		t.Fatal("preserve accepted an object whose moved identity did not match the durable pin")
	}
	got, err := os.ReadFile(target)
	if err != nil || string(got) != "same" {
		t.Fatalf("identity mismatch did not restore target: content=%q err=%v", got, err)
	}
	if _, err := os.Lstat(recovery); !os.IsNotExist(err) {
		t.Fatalf("identity mismatch left recovery slot behind: %v", err)
	}
}

func TestRestoreLocalRecoveryArtifactRejectsReplacementWithMatchingFingerprint(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	target := filepath.Join(root, "target.txt")
	if err := os.WriteFile(target, []byte("same"), 0o600); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	expected, err := exec.ObserveLocalEntry(ctx, "target.txt")
	if err != nil {
		t.Fatal(err)
	}
	pinnedIdentity := physicalIdentityForTest(t, target)
	op := domain.Operation{
		ID:                  90,
		Kind:                domain.OperationDeleteLocal,
		EntryKind:           domain.KindFile,
		SrcPath:             "target.txt",
		LocalTargetPath:     target,
		LocalTargetIdentity: pinnedIdentity,
		ExpectedLocal:       expected,
		Phase:               domain.OperationBlocked,
		Attempts:            1,
	}
	artifact, _ := RecoveryArtifactPath(op)
	original := filepath.Join(root, "original-preserved.txt")
	if err := os.Rename(target, original); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(artifact, []byte("same"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(original)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(artifact, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if _, err := RestoreLocalRecoveryArtifact(ctx, op); err == nil {
		t.Fatal("restore accepted a replacement recovery artifact with an equivalent fingerprint")
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("rejected replacement artifact unexpectedly restored target: %v", err)
	}
	for _, path := range []string{artifact, original} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("restore refusal removed %q: %v", path, err)
		}
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
		ID:                  91,
		Kind:                domain.OperationDeleteLocal,
		EntryKind:           domain.KindFile,
		SrcPath:             "target.txt",
		LocalTargetPath:     target,
		LocalTargetIdentity: physicalIdentityForTest(t, target),
		ExpectedLocal:       expected,
		Phase:               domain.OperationBlocked,
		Attempts:            1,
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

func physicalIdentityForTest(t *testing.T, path string) string {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := physicalObjectIdentity(path, info)
	if err != nil {
		t.Fatal(err)
	}
	return identity
}
