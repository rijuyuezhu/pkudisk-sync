package executor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/rclone/rclone/fs"
	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

// ObserveLocalEntry returns the ordinary-file/directory state visible through
// the configured symlink policy. In follow mode it fingerprints the resolved
// target; dangling links are absent from the synchronized virtual namespace.
func (e *RootExecutor) ObserveLocalEntry(ctx context.Context, relPath string) (domain.LocalFingerprint, error) {
	if err := domain.ValidateRelPath(relPath); err != nil {
		return domain.LocalFingerprint{}, err
	}
	if err := ctx.Err(); err != nil {
		return domain.LocalFingerprint{}, err
	}
	fullPath := filepath.Join(e.root.LocalRoot, filepath.FromSlash(relPath))
	stat := os.Lstat
	if e.root.EffectiveSymlinkMode() == domain.SymlinkFollow {
		stat = os.Stat
	}
	info, err := stat(fullPath)
	if errors.Is(err, os.ErrNotExist) {
		return domain.LocalFingerprint{}, nil
	}
	if err != nil {
		return domain.LocalFingerprint{}, fmt.Errorf("stat local entry %q: %w", relPath, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return domain.LocalFingerprint{}, fmt.Errorf("local symlink %q is not addressable under %s policy", relPath, e.root.EffectiveSymlinkMode())
	}
	if info.IsDir() {
		return domain.LocalFingerprint{Present: true, Kind: domain.KindDir}, nil
	}
	if !info.Mode().IsRegular() {
		return domain.LocalFingerprint{}, fmt.Errorf("unsupported local file type %q (%s)", relPath, info.Mode().Type())
	}
	return localFileFingerprint(info), nil
}

// ObserveRemoteEntry resolves one path through its parent listing, so it can
// observe both files and directories and preserve PKU Disk object identity.
func (e *RootExecutor) ObserveRemoteEntry(ctx context.Context, relPath string) (domain.RemoteFingerprint, error) {
	if err := domain.ValidateRelPath(relPath); err != nil {
		return domain.RemoteFingerprint{}, err
	}
	if e.observeRemoteFn != nil {
		return e.observeRemoteFn(ctx, relPath)
	}
	parent, _ := path.Split(relPath)
	parent = strings.TrimSuffix(parent, "/")
	entries, err := e.remote.List(ctx, parent)
	if errors.Is(err, fs.ErrorDirNotFound) {
		return domain.RemoteFingerprint{}, nil
	}
	if err != nil {
		return domain.RemoteFingerprint{}, fmt.Errorf("list remote parent %q: %w", parent, err)
	}
	for _, entry := range entries {
		if entry.Remote() != relPath {
			continue
		}
		switch typed := entry.(type) {
		case fs.Object:
			fingerprint, err := remoteFileFingerprint(ctx, typed)
			if err != nil {
				return domain.RemoteFingerprint{}, fmt.Errorf("observe remote file %q: %w", relPath, err)
			}
			return fingerprint, nil
		case fs.Directory:
			id := strings.TrimSpace(typed.ID())
			if id == "" {
				return domain.RemoteFingerprint{}, fmt.Errorf("remote directory %q has no object ID", relPath)
			}
			return domain.RemoteFingerprint{
				Present: true,
				Kind:    domain.KindDir,
				ID:      id,
				MtimeUS: typed.ModTime(ctx).UnixMicro(),
			}, nil
		default:
			return domain.RemoteFingerprint{}, fmt.Errorf("remote entry %q has unsupported type %T", relPath, entry)
		}
	}
	return domain.RemoteFingerprint{}, nil
}

// EnsureLocalDir creates exactly one planned physical target directory.
// A dangling followed directory symlink is preserved and its target is created.
func (e *RootExecutor) EnsureLocalDir(ctx context.Context, op domain.Operation) error {
	if op.Kind != domain.OperationEnsureLocal || op.EntryKind != domain.KindDir || op.LocalTargetPath == "" {
		return fmt.Errorf("invalid pinned local directory operation")
	}
	if err := op.ExpectedLocal.Validate(); err != nil {
		return fmt.Errorf("expected local state: %w", err)
	}
	if op.ExpectedLocal.Present {
		return fmt.Errorf("local directory create requires an absent expectation")
	}
	current, err := e.ObserveLocalEntry(ctx, op.SrcPath)
	if err != nil {
		return err
	}
	if current.Present {
		return fmt.Errorf("local directory create precondition failed for %q", op.SrcPath)
	}
	currentTarget, err := e.ResolveLocalMutationTarget(ctx, op.SrcPath, op.ExpectedLocal)
	if err != nil {
		return err
	}
	if !samePhysicalDestination(op.LocalTargetPath, currentTarget, false) {
		return fmt.Errorf("local symlink target changed before directory create for %q", op.SrcPath)
	}
	if err := os.Mkdir(op.LocalTargetPath, 0o755); err != nil {
		return fmt.Errorf("create local directory %q: %w", op.SrcPath, err)
	}
	return nil
}

// EnsureRemoteDir creates a planned directory only when the pre-scan observed
// absence. A same-directory concurrent create after this check is harmless: the
// operation is "ensure directory", and no bytes are overwritten.
func (e *RootExecutor) EnsureRemoteDir(ctx context.Context, relPath string, expected domain.RemoteExpectation) error {
	if err := domain.ValidateRelPath(relPath); err != nil {
		return err
	}
	if err := expected.Validate(); err != nil {
		return fmt.Errorf("expected remote state: %w", err)
	}
	if !expected.Absent {
		return fmt.Errorf("remote directory create requires an absent expectation")
	}
	current, err := e.ObserveRemoteEntry(ctx, relPath)
	if err != nil {
		return err
	}
	if current.Present {
		return fmt.Errorf("remote directory create precondition failed for %q", relPath)
	}
	if err := e.remote.Mkdir(ctx, relPath); err != nil {
		return fmt.Errorf("create remote directory %q: %w", relPath, err)
	}
	return nil
}

// EnsureLocalFile downloads the exact planned remote revision next to the
// resolved physical destination and only then commits it under local/remote
// preconditions. A followed symlink is preserved; only its target is replaced.
func (e *RootExecutor) EnsureLocalFile(ctx context.Context, op domain.Operation) (domain.LocalFingerprint, error) {
	if op.Kind != domain.OperationEnsureLocal || op.EntryKind != domain.KindFile || op.LocalTargetPath == "" || op.ID <= 0 {
		return domain.LocalFingerprint{}, fmt.Errorf("invalid pinned local file operation")
	}
	if err := op.ExpectedLocal.Validate(); err != nil {
		return domain.LocalFingerprint{}, fmt.Errorf("expected local state: %w", err)
	}
	if err := validateFileRemoteExpectation(op.ExpectedRemote); err != nil || op.ExpectedRemote.Absent {
		if err != nil {
			return domain.LocalFingerprint{}, err
		}
		return domain.LocalFingerprint{}, fmt.Errorf("download requires a present remote expectation")
	}
	current, err := e.ObserveLocalEntry(ctx, op.SrcPath)
	if err != nil {
		return domain.LocalFingerprint{}, err
	}
	if !domain.LocalEquivalent(current, op.ExpectedLocal) {
		return domain.LocalFingerprint{}, fmt.Errorf("local download precondition failed for %q", op.SrcPath)
	}
	currentTarget, err := e.ResolveLocalMutationTarget(ctx, op.SrcPath, op.ExpectedLocal)
	if err != nil {
		return domain.LocalFingerprint{}, err
	}
	if !samePhysicalDestination(op.LocalTargetPath, currentTarget, op.ExpectedLocal.Present) {
		return domain.LocalFingerprint{}, fmt.Errorf("local symlink target changed before download for %q", op.SrcPath)
	}
	tempPath := operationPhysicalTempPath(op.LocalTargetPath, op.ID, "download")
	_ = os.Remove(tempPath)
	defer func() { _ = os.Remove(tempPath) }()
	if err := e.downloadToPhysicalTemp(ctx, op.SrcPath, tempPath, op.ExpectedRemote); err != nil {
		return domain.LocalFingerprint{}, err
	}
	return e.commitDownloadedPhysicalTemp(ctx, op, tempPath)
}

// DeleteLocal removes exactly the expected target file or empty target
// directory. In follow mode a symlink object is retained and becomes dangling
// after its target is deleted.
func (e *RootExecutor) DeleteLocal(ctx context.Context, op domain.Operation) error {
	if op.Kind != domain.OperationDeleteLocal || op.LocalTargetPath == "" || op.ID <= 0 {
		return fmt.Errorf("invalid pinned local delete operation")
	}
	if err := op.ExpectedLocal.Validate(); err != nil {
		return fmt.Errorf("expected local state: %w", err)
	}
	if !op.ExpectedLocal.Present {
		return fmt.Errorf("local delete requires a present expectation")
	}
	current, err := e.ObserveLocalEntry(ctx, op.SrcPath)
	if err != nil {
		return err
	}
	if !domain.LocalEquivalent(current, op.ExpectedLocal) {
		return fmt.Errorf("local delete precondition failed for %q", op.SrcPath)
	}
	currentTarget, err := e.ResolveLocalMutationTarget(ctx, op.SrcPath, op.ExpectedLocal)
	if err != nil {
		return err
	}
	if !samePhysicalDestination(op.LocalTargetPath, currentTarget, true) {
		return fmt.Errorf("local symlink target changed before delete for %q", op.SrcPath)
	}
	recoveryPath := operationPhysicalTempPath(op.LocalTargetPath, op.ID, "recovery")
	preserved, err := preserveExpectedLocalEntryAt(ctx, op.SrcPath, op.LocalTargetPath, recoveryPath, op.ExpectedLocal)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return restorePreservedLocalEntry(preserved, op.SrcPath, err)
	}
	if err := os.Remove(preserved.recoveryPath); err != nil {
		return restorePreservedLocalEntry(preserved, op.SrcPath, fmt.Errorf("delete local %s %q: %w", op.ExpectedLocal.Kind, op.SrcPath, err))
	}
	return nil
}

type preservedLocalEntry struct {
	targetPath   string
	recoveryPath string
}

// preserveExpectedLocalEntryAt atomically moves the resolved physical target
// aside before validating what was actually moved. The symlink path itself is
// never renamed or unlinked.
func preserveExpectedLocalEntryAt(ctx context.Context, relPath, targetPath, recoveryPath string, expected domain.LocalFingerprint) (preservedLocalEntry, error) {
	if err := movePathNoReplace(targetPath, recoveryPath); err != nil {
		return preservedLocalEntry{}, fmt.Errorf("preserve local entry %q before mutation: %w", relPath, err)
	}
	preserved := preservedLocalEntry{targetPath: targetPath, recoveryPath: recoveryPath}
	observed, observeErr := observePhysicalLocalEntry(ctx, recoveryPath)
	if observeErr == nil && domain.LocalEquivalent(observed, expected) {
		return preserved, nil
	}
	primary := observeErr
	if primary == nil {
		primary = fmt.Errorf("local mutation precondition failed for %q after preserving the actual path target", relPath)
	}
	if restoreErr := movePathNoReplace(recoveryPath, targetPath); restoreErr != nil {
		return preservedLocalEntry{}, errors.Join(primary, fmt.Errorf("local data is preserved at %q because restoring %q failed: %w", recoveryPath, relPath, restoreErr))
	}
	return preservedLocalEntry{}, primary
}

func restorePreservedLocalEntry(preserved preservedLocalEntry, relPath string, primary error) error {
	if restoreErr := movePathNoReplace(preserved.recoveryPath, preserved.targetPath); restoreErr != nil {
		return errors.Join(primary, fmt.Errorf("local data is preserved at %q because restoring %q failed: %w", preserved.recoveryPath, relPath, restoreErr))
	}
	return primary
}

func (e *RootExecutor) installDownloadedPhysicalTemp(ctx context.Context, relPath, targetPath, tempPath, recoveryPath string, expectedLocal domain.LocalFingerprint) error {
	if !expectedLocal.Present {
		if err := movePathNoReplace(tempPath, targetPath); err != nil {
			return fmt.Errorf("install downloaded file %q without replacing a concurrent local create: %w", relPath, err)
		}
		return nil
	}
	preserved, err := preserveExpectedLocalEntryAt(ctx, relPath, targetPath, recoveryPath, expectedLocal)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return restorePreservedLocalEntry(preserved, relPath, err)
	}
	if err := movePathNoReplace(tempPath, targetPath); err != nil {
		return restorePreservedLocalEntry(preserved, relPath, fmt.Errorf("install downloaded file %q without replacing a concurrent local write: %w", relPath, err))
	}
	if err := os.Remove(preserved.recoveryPath); err != nil {
		return fmt.Errorf("download installed at %q but could not remove preserved prior file %q: %w", relPath, preserved.recoveryPath, err)
	}
	return nil
}

// installDownloadedTemp retains the existing test/helper surface for lexical
// temporary paths while routing the actual commit through physical targets.
func (e *RootExecutor) installDownloadedTemp(ctx context.Context, relPath, tempRelPath string, expectedLocal domain.LocalFingerprint) error {
	tempPath, err := e.resolveLocalMutationPath(tempRelPath, true)
	if err != nil {
		return err
	}
	targetPath, err := e.resolveLocalMutationPath(relPath, expectedLocal.Present)
	if err != nil {
		return err
	}
	recoveryPath, err := newPhysicalTempPath(targetPath)
	if err != nil {
		return err
	}
	return e.installDownloadedPhysicalTemp(ctx, relPath, targetPath, tempPath, recoveryPath, expectedLocal)
}

func (e *RootExecutor) commitDownloadedPhysicalTemp(ctx context.Context, op domain.Operation, tempPath string) (domain.LocalFingerprint, error) {
	currentLocal, err := e.ObserveLocalEntry(ctx, op.SrcPath)
	if err != nil {
		return domain.LocalFingerprint{}, err
	}
	if !domain.LocalEquivalent(currentLocal, op.ExpectedLocal) {
		return domain.LocalFingerprint{}, fmt.Errorf("local download precondition failed for %q", op.SrcPath)
	}
	currentTarget, err := e.ResolveLocalMutationTarget(ctx, op.SrcPath, op.ExpectedLocal)
	if err != nil {
		return domain.LocalFingerprint{}, err
	}
	if !samePhysicalDestination(op.LocalTargetPath, currentTarget, op.ExpectedLocal.Present) {
		return domain.LocalFingerprint{}, fmt.Errorf("local symlink target changed during download for %q", op.SrcPath)
	}
	currentRemote, err := e.ObserveRemoteEntry(ctx, op.SrcPath)
	if err != nil {
		return domain.LocalFingerprint{}, err
	}
	if !remoteMatchesExpectation(currentRemote, op.ExpectedRemote, domain.KindFile) {
		return domain.LocalFingerprint{}, fmt.Errorf("remote download precondition failed for %q", op.SrcPath)
	}
	temp, err := observePhysicalLocalEntry(ctx, tempPath)
	if err != nil {
		return domain.LocalFingerprint{}, err
	}
	if !temp.Present || temp.Kind != domain.KindFile {
		return domain.LocalFingerprint{}, fmt.Errorf("download temp %q is not a regular file", tempPath)
	}
	recoveryPath := operationPhysicalTempPath(op.LocalTargetPath, op.ID, "recovery")
	if err := e.installDownloadedPhysicalTemp(ctx, op.SrcPath, op.LocalTargetPath, tempPath, recoveryPath, op.ExpectedLocal); err != nil {
		return domain.LocalFingerprint{}, fmt.Errorf("replace local file %q: %w", op.SrcPath, err)
	}
	return temp, nil
}

func (e *RootExecutor) resolveLocalMutationPath(relPath string, present bool) (string, error) {
	if err := domain.ValidateRelPath(relPath); err != nil {
		return "", err
	}
	lexical := filepath.Join(e.root.LocalRoot, filepath.FromSlash(relPath))
	if e.root.EffectiveSymlinkMode() != domain.SymlinkFollow {
		return lexical, nil
	}
	resolved, resolvedPresent, err := resolveFollowedLocalPath(lexical, !present)
	if err != nil {
		return "", fmt.Errorf("resolve followed local entry %q: %w", relPath, err)
	}
	if present && !resolvedPresent {
		return "", fmt.Errorf("followed local entry %q disappeared", relPath)
	}
	return filepath.Clean(resolved), nil
}

func (e *RootExecutor) ResolveLocalMutationTarget(ctx context.Context, relPath string, expected domain.LocalFingerprint) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	current, err := e.ObserveLocalEntry(ctx, relPath)
	if err != nil {
		return "", err
	}
	if !domain.LocalEquivalent(current, expected) {
		return "", fmt.Errorf("local mutation target precondition failed for %q", relPath)
	}
	return e.resolveLocalMutationPath(relPath, expected.Present)
}

func samePhysicalDestination(a, b string, present bool) bool {
	if present {
		ai, aErr := os.Stat(a)
		bi, bErr := os.Stat(b)
		return aErr == nil && bErr == nil && os.SameFile(ai, bi)
	}
	if filepath.Base(a) != filepath.Base(b) {
		return false
	}
	ai, aErr := os.Stat(filepath.Dir(a))
	bi, bErr := os.Stat(filepath.Dir(b))
	return aErr == nil && bErr == nil && os.SameFile(ai, bi)
}

func observePhysicalLocalEntry(ctx context.Context, fullPath string) (domain.LocalFingerprint, error) {
	if err := ctx.Err(); err != nil {
		return domain.LocalFingerprint{}, err
	}
	info, err := os.Lstat(fullPath)
	if errors.Is(err, os.ErrNotExist) {
		return domain.LocalFingerprint{}, nil
	}
	if err != nil {
		return domain.LocalFingerprint{}, fmt.Errorf("stat physical local entry %q: %w", fullPath, err)
	}
	if info.IsDir() {
		return domain.LocalFingerprint{Present: true, Kind: domain.KindDir}, nil
	}
	if !info.Mode().IsRegular() {
		return domain.LocalFingerprint{}, fmt.Errorf("unsupported physical local file type %q (%s)", fullPath, info.Mode().Type())
	}
	return localFileFingerprint(info), nil
}

func newPhysicalTempPath(targetPath string) (string, error) {
	name, err := newTempName()
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(targetPath), name), nil
}

func operationPhysicalTempPath(targetPath string, operationID int64, role string) string {
	return filepath.Join(filepath.Dir(targetPath), fmt.Sprintf("%sop-%d-%s", tempNamePrefix, operationID, role))
}

// LocalRecoveryArtifact reports the deterministic preserved-target slot for a
// journaled local mutation. Recovery code uses this to fail closed rather than
// forgetting data moved outside the lexical sync root by a followed symlink.
func (e *RootExecutor) LocalRecoveryArtifact(ctx context.Context, op domain.Operation) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	if op.LocalTargetPath == "" || op.ID <= 0 {
		return "", false, nil
	}
	recoveryPath := operationPhysicalTempPath(op.LocalTargetPath, op.ID, "recovery")
	_, err := os.Lstat(recoveryPath)
	if errors.Is(err, os.ErrNotExist) {
		return recoveryPath, false, nil
	}
	if err != nil {
		return recoveryPath, false, fmt.Errorf("stat local recovery artifact %q: %w", recoveryPath, err)
	}
	return recoveryPath, true, nil
}

// CleanupLocalRecoveryArtifact removes an operation-owned stale recovery slot
// only after the caller has independently proven the desired operation
// postcondition. It never follows or derives a path from current symlink state;
// the durable pinned LocalTargetPath is the sole authority.
func (e *RootExecutor) CleanupLocalRecoveryArtifact(ctx context.Context, op domain.Operation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	recoveryPath, ok := RecoveryArtifactPath(op)
	if !ok {
		return nil
	}
	if err := os.Remove(recoveryPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale local recovery artifact %q: %w", recoveryPath, err)
	}
	return nil
}

// RecoveryArtifactPath returns the deterministic recovery-slot path for a
// journaled local mutation without inspecting the filesystem.
func RecoveryArtifactPath(op domain.Operation) (string, bool) {
	if op.ID <= 0 || op.LocalTargetPath == "" || (op.Kind != domain.OperationEnsureLocal && op.Kind != domain.OperationDeleteLocal) {
		return "", false
	}
	return operationPhysicalTempPath(op.LocalTargetPath, op.ID, "recovery"), true
}

// RestoreLocalRecoveryArtifact restores a preserved pre-mutation local object
// to its pinned physical target using no-replace semantics. It never overwrites
// a current target and only restores an artifact that still matches the
// operation's expected pre-mutation local fingerprint.
func RestoreLocalRecoveryArtifact(ctx context.Context, op domain.Operation) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if op.Phase != domain.OperationBlocked {
		return "", fmt.Errorf("operation %d is %s, not blocked", op.ID, op.Phase)
	}
	if !op.ExpectedLocal.Present {
		return "", fmt.Errorf("operation %d has no expected prior local object to restore", op.ID)
	}
	recoveryPath, ok := RecoveryArtifactPath(op)
	if !ok {
		return "", fmt.Errorf("operation %d has no pinned local recovery artifact path", op.ID)
	}
	artifact, err := observePhysicalLocalEntry(ctx, recoveryPath)
	if err != nil {
		return recoveryPath, err
	}
	if !artifact.Present {
		return recoveryPath, fmt.Errorf("local recovery artifact %q does not exist", recoveryPath)
	}
	if !domain.LocalEquivalent(artifact, op.ExpectedLocal) {
		return recoveryPath, fmt.Errorf("local recovery artifact %q no longer matches operation %d expected local state", recoveryPath, op.ID)
	}
	current, err := observePhysicalLocalEntry(ctx, op.LocalTargetPath)
	if err != nil {
		return recoveryPath, err
	}
	if current.Present {
		return recoveryPath, fmt.Errorf("refusing to restore %q because pinned local target %q already exists", recoveryPath, op.LocalTargetPath)
	}
	if err := movePathNoReplace(recoveryPath, op.LocalTargetPath); err != nil {
		return recoveryPath, fmt.Errorf("restore local recovery artifact %q: %w", recoveryPath, err)
	}
	restored, err := observePhysicalLocalEntry(ctx, op.LocalTargetPath)
	if err != nil {
		return recoveryPath, err
	}
	if !domain.LocalEquivalent(restored, op.ExpectedLocal) {
		return recoveryPath, fmt.Errorf("restored local target %q does not match operation %d expected local state", op.LocalTargetPath, op.ID)
	}
	return recoveryPath, nil
}

// CommitDownloadedTemp performs the local half of conditional download commit.
// Both the canonical local path and current remote identity are revalidated
// immediately before replacing the local file. The returned fingerprint is the
// temp file state that was actually moved into the canonical path.
func (e *RootExecutor) CommitDownloadedTemp(ctx context.Context, relPath, tempRelPath string, expectedLocal domain.LocalFingerprint, expectedRemote domain.RemoteExpectation) (domain.LocalFingerprint, error) {
	if err := domain.ValidateRelPath(relPath); err != nil {
		return domain.LocalFingerprint{}, err
	}
	if err := domain.ValidateRelPath(tempRelPath); err != nil {
		return domain.LocalFingerprint{}, fmt.Errorf("temporary path: %w", err)
	}
	if path.Dir(relPath) != path.Dir(tempRelPath) {
		return domain.LocalFingerprint{}, fmt.Errorf("download temp must be in the same directory as %q", relPath)
	}
	if err := expectedLocal.Validate(); err != nil {
		return domain.LocalFingerprint{}, fmt.Errorf("expected local state: %w", err)
	}
	if err := validateFileRemoteExpectation(expectedRemote); err != nil || expectedRemote.Absent {
		if err != nil {
			return domain.LocalFingerprint{}, err
		}
		return domain.LocalFingerprint{}, fmt.Errorf("download commit requires a present remote expectation")
	}

	currentLocal, err := e.ObserveLocalEntry(ctx, relPath)
	if err != nil {
		return domain.LocalFingerprint{}, err
	}
	if !domain.LocalEquivalent(currentLocal, expectedLocal) {
		return domain.LocalFingerprint{}, fmt.Errorf("local download precondition failed for %q", relPath)
	}
	currentRemote, err := e.ObserveRemoteEntry(ctx, relPath)
	if err != nil {
		return domain.LocalFingerprint{}, err
	}
	if !remoteMatchesExpectation(currentRemote, expectedRemote, domain.KindFile) {
		return domain.LocalFingerprint{}, fmt.Errorf("remote download precondition failed for %q", relPath)
	}
	temp, err := e.ObserveLocalEntry(ctx, tempRelPath)
	if err != nil {
		return domain.LocalFingerprint{}, err
	}
	if !temp.Present || temp.Kind != domain.KindFile {
		return domain.LocalFingerprint{}, fmt.Errorf("download temp %q is not a regular file", tempRelPath)
	}

	if err := e.installDownloadedTemp(ctx, relPath, tempRelPath, expectedLocal); err != nil {
		return domain.LocalFingerprint{}, fmt.Errorf("replace local file %q: %w", relPath, err)
	}
	return temp, nil
}

// CompareFileContent obtains the exact planned remote revision through the same
// guarded download path and compares it byte-for-byte with an unchanged local
// file. The temporary file is always client-owned and excluded from snapshots.
func (e *RootExecutor) CompareFileContent(ctx context.Context, relPath string, expectedLocal domain.LocalFingerprint, expectedRemote domain.RemoteExpectation) (bool, error) {
	if err := validateFilePathAndLocalExpectation(relPath, expectedLocal); err != nil {
		return false, err
	}
	if err := validateFileRemoteExpectation(expectedRemote); err != nil || expectedRemote.Absent {
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("content comparison requires a present remote expectation")
	}
	before, err := e.ObserveLocalEntry(ctx, relPath)
	if err != nil {
		return false, err
	}
	if !domain.LocalEquivalent(before, expectedLocal) {
		return false, fmt.Errorf("local content comparison precondition failed for %q", relPath)
	}

	tempRel, err := newTempRelPath(relPath)
	if err != nil {
		return false, err
	}
	defer e.removeLocalTemp(tempRel)
	if err := e.DownloadToTemp(ctx, relPath, tempRel, expectedRemote); err != nil {
		return false, err
	}

	localPath := filepath.Join(e.root.LocalRoot, filepath.FromSlash(relPath))
	tempPath := filepath.Join(e.root.LocalRoot, filepath.FromSlash(tempRel))
	equal, err := filesEqual(localPath, tempPath)
	if err != nil {
		return false, err
	}
	after, err := e.ObserveLocalEntry(ctx, relPath)
	if err != nil {
		return false, err
	}
	if !domain.LocalEquivalent(after, expectedLocal) {
		return false, fmt.Errorf("local file changed during content comparison for %q", relPath)
	}
	remote, err := e.ObserveRemoteEntry(ctx, relPath)
	if err != nil {
		return false, err
	}
	if !remoteMatchesExpectation(remote, expectedRemote, domain.KindFile) {
		return false, fmt.Errorf("remote file changed during content comparison for %q", relPath)
	}
	return equal, nil
}

func newTempName() (string, error) {
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate temporary file name: %w", err)
	}
	return tempNamePrefix + hex.EncodeToString(random[:]), nil
}

func newTempRelPath(relPath string) (string, error) {
	if err := domain.ValidateRelPath(relPath); err != nil {
		return "", err
	}
	name, err := newTempName()
	if err != nil {
		return "", err
	}
	dir := path.Dir(relPath)
	if dir == "." {
		return name, nil
	}
	return path.Join(dir, name), nil
}

func (e *RootExecutor) removeLocalTemp(tempRelPath string) {
	if err := domain.ValidateRelPath(tempRelPath); err != nil {
		return
	}
	_ = os.Remove(filepath.Join(e.root.LocalRoot, filepath.FromSlash(tempRelPath)))
}

func filesEqual(a, b string) (bool, error) {
	left, err := os.Open(a)
	if err != nil {
		return false, fmt.Errorf("open local comparison file: %w", err)
	}
	defer func() { _ = left.Close() }()
	right, err := os.Open(b)
	if err != nil {
		return false, fmt.Errorf("open downloaded comparison file: %w", err)
	}
	defer func() { _ = right.Close() }()

	leftBuf := make([]byte, 256*1024)
	rightBuf := make([]byte, 256*1024)
	for {
		ln, le := io.ReadFull(left, leftBuf)
		rn, re := io.ReadFull(right, rightBuf)
		if ln != rn || string(leftBuf[:ln]) != string(rightBuf[:rn]) {
			return false, nil
		}
		if le == io.EOF || le == io.ErrUnexpectedEOF || re == io.EOF || re == io.ErrUnexpectedEOF {
			return (le == io.EOF || le == io.ErrUnexpectedEOF) && (re == io.EOF || re == io.ErrUnexpectedEOF), nil
		}
		if le != nil {
			return false, fmt.Errorf("read local comparison file: %w", le)
		}
		if re != nil {
			return false, fmt.Errorf("read downloaded comparison file: %w", re)
		}
	}
}

func remoteMatchesExpectation(current domain.RemoteFingerprint, expected domain.RemoteExpectation, kind domain.EntryKind) bool {
	if expected.Absent {
		return !current.Present
	}
	if !current.Present || current.Kind != kind || current.ID != expected.ID {
		return false
	}
	if kind == domain.KindFile {
		return current.Rev == expected.Rev
	}
	return true
}
