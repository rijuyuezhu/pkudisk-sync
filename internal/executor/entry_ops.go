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
	"runtime"
	"strings"

	"github.com/rclone/rclone/fs"
	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

// ObserveLocalEntry returns the logical ordinary-file/directory state visible
// through the configured symlink policy. Follow and copy both dereference for
// reconciliation observation; lexical symlink identity is inspected by
// mutation-specific helpers instead.
func (e *RootExecutor) ObserveLocalEntry(ctx context.Context, relPath string) (domain.LocalFingerprint, error) {
	if err := domain.ValidateRelPath(relPath); err != nil {
		return domain.LocalFingerprint{}, err
	}
	if err := ctx.Err(); err != nil {
		return domain.LocalFingerprint{}, err
	}
	fullPath := filepath.Join(e.root.LocalRoot, filepath.FromSlash(relPath))
	stat := os.Lstat
	mode := e.root.EffectiveSymlinkMode()
	if mode == domain.SymlinkFollow || mode == domain.SymlinkCopy {
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

// ObservePinnedLocalEntry observes the durable physical destination selected
// before a local mutation started. It never re-resolves the current lexical
// symlink chain. ordinary is false while the pinned path is still a symlink or
// another non-ordinary object, so such a state cannot prove materialization.
func (e *RootExecutor) ObservePinnedLocalEntry(ctx context.Context, op domain.Operation) (domain.LocalFingerprint, bool, error) {
	if err := validatePinnedLocalOperation(op); err != nil {
		return domain.LocalFingerprint{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return domain.LocalFingerprint{}, false, err
	}
	info, err := os.Lstat(op.LocalTargetPath)
	if errors.Is(err, os.ErrNotExist) {
		return domain.LocalFingerprint{}, true, nil
	}
	if err != nil {
		return domain.LocalFingerprint{}, false, fmt.Errorf("lstat pinned local target %q: %w", op.LocalTargetPath, err)
	}
	if info.IsDir() {
		return domain.LocalFingerprint{Present: true, Kind: domain.KindDir}, true, nil
	}
	if info.Mode().IsRegular() {
		return localFileFingerprint(info), true, nil
	}
	return domain.LocalFingerprint{}, false, nil
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

// EnsureLocalDir creates exactly one planned physical target directory. The
// operation's pinned parent identity prevents a same-path parent replacement
// from redirecting the create after authority was established.
func (e *RootExecutor) EnsureLocalDir(ctx context.Context, op domain.Operation, peerLocalRoots []string, beginSideEffect func() error) error {
	if op.Kind != domain.OperationEnsureLocal || op.EntryKind != domain.KindDir || op.LocalTargetPath == "" {
		return fmt.Errorf("invalid pinned local directory operation")
	}
	if err := op.ExpectedLocal.Validate(); err != nil {
		return fmt.Errorf("expected local state: %w", err)
	}
	if op.ExpectedLocal.Present {
		return fmt.Errorf("local directory create requires an absent expectation")
	}
	if operationMayHaveStarted(op) {
		holds, err := e.PinnedLocalPreconditionHolds(ctx, op)
		if err != nil {
			return err
		}
		if !holds {
			return fmt.Errorf("pinned local directory create precondition failed for %q", op.SrcPath)
		}
	} else {
		current, err := e.ObserveLocalEntry(ctx, op.SrcPath)
		if err != nil {
			return err
		}
		if current.Present {
			return fmt.Errorf("local directory create precondition failed for %q", op.SrcPath)
		}
		currentTarget, err := e.ResolveLocalMutationTarget(ctx, op.SrcPath, op.ExpectedLocal, op.EntryKind, peerLocalRoots)
		if err != nil {
			return err
		}
		if !localMutationTargetMatchesPin(op, currentTarget) {
			return fmt.Errorf("local symlink target changed before directory create for %q", op.SrcPath)
		}
	}
	if err := verifyPinnedLocalMutationAnchor(op); err != nil {
		return err
	}
	if beginSideEffect == nil {
		return fmt.Errorf("local directory mutation requires a side-effect start callback")
	}
	if err := beginSideEffect(); err != nil {
		return fmt.Errorf("record local directory side-effect start for %q: %w", op.SrcPath, err)
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
func (e *RootExecutor) EnsureLocalFile(ctx context.Context, op domain.Operation, peerLocalRoots []string, beginSideEffect func() error) (domain.LocalFingerprint, error) {
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
	if operationMayHaveStarted(op) {
		holds, err := e.PinnedLocalPreconditionHolds(ctx, op)
		if err != nil {
			return domain.LocalFingerprint{}, err
		}
		if !holds {
			return domain.LocalFingerprint{}, fmt.Errorf("pinned local download precondition failed for %q", op.SrcPath)
		}
	} else {
		current, err := e.ObserveLocalEntry(ctx, op.SrcPath)
		if err != nil {
			return domain.LocalFingerprint{}, err
		}
		if !domain.LocalEquivalent(current, op.ExpectedLocal) {
			return domain.LocalFingerprint{}, fmt.Errorf("local download precondition failed for %q", op.SrcPath)
		}
		currentTarget, err := e.ResolveLocalMutationTarget(ctx, op.SrcPath, op.ExpectedLocal, op.EntryKind, peerLocalRoots)
		if err != nil {
			return domain.LocalFingerprint{}, err
		}
		if !localMutationTargetMatchesPin(op, currentTarget) {
			return domain.LocalFingerprint{}, fmt.Errorf("local symlink target changed before download for %q", op.SrcPath)
		}
	}
	if err := verifyPinnedLocalMutationAnchor(op); err != nil {
		return domain.LocalFingerprint{}, err
	}
	tempPath := operationPhysicalTempPath(op.LocalTargetPath, op.ID, "download")
	_ = os.Remove(tempPath)
	defer func() { _ = os.Remove(tempPath) }()
	if err := e.downloadToPhysicalTemp(ctx, op.SrcPath, tempPath, op.ExpectedRemote); err != nil {
		return domain.LocalFingerprint{}, err
	}
	return e.commitDownloadedPhysicalTemp(ctx, op, tempPath, peerLocalRoots, beginSideEffect)
}

// DeleteLocal removes exactly the expected target file or empty target
// directory. In follow mode a symlink object is retained and becomes dangling
// after its target is deleted.
func (e *RootExecutor) DeleteLocal(ctx context.Context, op domain.Operation, peerLocalRoots []string, beginSideEffect func() error) error {
	if op.Kind != domain.OperationDeleteLocal || op.LocalTargetPath == "" || op.ID <= 0 {
		return fmt.Errorf("invalid pinned local delete operation")
	}
	if err := op.ExpectedLocal.Validate(); err != nil {
		return fmt.Errorf("expected local state: %w", err)
	}
	if !op.ExpectedLocal.Present {
		return fmt.Errorf("local delete requires a present expectation")
	}
	if operationMayHaveStarted(op) {
		holds, err := e.PinnedLocalPreconditionHolds(ctx, op)
		if err != nil {
			return err
		}
		if !holds {
			return fmt.Errorf("pinned local delete precondition failed for %q", op.SrcPath)
		}
	} else {
		current, err := e.ObserveLocalEntry(ctx, op.SrcPath)
		if err != nil {
			return err
		}
		if !domain.LocalEquivalent(current, op.ExpectedLocal) {
			return fmt.Errorf("local delete precondition failed for %q", op.SrcPath)
		}
		currentTarget, err := e.ResolveLocalMutationTarget(ctx, op.SrcPath, op.ExpectedLocal, op.EntryKind, peerLocalRoots)
		if err != nil {
			return err
		}
		if !localMutationTargetMatchesPin(op, currentTarget) {
			return fmt.Errorf("local symlink target changed before delete for %q", op.SrcPath)
		}
	}
	if err := verifyPinnedLocalMutationAnchor(op); err != nil {
		return err
	}
	if beginSideEffect == nil {
		return fmt.Errorf("local delete requires a side-effect start callback")
	}
	if err := beginSideEffect(); err != nil {
		return fmt.Errorf("record local delete side-effect start for %q: %w", op.SrcPath, err)
	}
	recoveryPath := operationPhysicalTempPath(op.LocalTargetPath, op.ID, "recovery")
	if op.LocalSymlinkTarget != "" {
		preserved, err := preserveExpectedCopySymlinkAt(ctx, op.SrcPath, op.LocalTargetPath, recoveryPath, op.LocalSymlinkTarget, op.ExpectedLocal)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return restorePreservedLocalEntry(preserved, op.SrcPath, err)
		}
		if err := os.Remove(preserved.recoveryPath); err != nil {
			return restorePreservedLocalEntry(preserved, op.SrcPath, fmt.Errorf("delete copy projection %q: %w", op.SrcPath, err))
		}
		return nil
	}
	preserved, err := preserveExpectedLocalEntryAt(ctx, op.SrcPath, op.LocalTargetPath, recoveryPath, op.ExpectedLocal, op.LocalTargetIdentity)
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
func preserveExpectedLocalEntryAt(ctx context.Context, relPath, targetPath, recoveryPath string, expected domain.LocalFingerprint, expectedIdentity string) (preservedLocalEntry, error) {
	if err := movePathNoReplace(targetPath, recoveryPath); err != nil {
		return preservedLocalEntry{}, fmt.Errorf("preserve local entry %q before mutation: %w", relPath, err)
	}
	preserved := preservedLocalEntry{targetPath: targetPath, recoveryPath: recoveryPath}
	observed, observeErr := observePhysicalLocalEntry(ctx, recoveryPath)
	if observeErr == nil && domain.LocalEquivalent(observed, expected) {
		if expectedIdentity == "" {
			return preserved, nil
		}
		info, statErr := os.Stat(recoveryPath)
		if statErr == nil {
			identity, identityErr := physicalObjectIdentity(recoveryPath, info)
			if identityErr == nil && identity == expectedIdentity {
				return preserved, nil
			}
			if identityErr != nil {
				observeErr = identityErr
			} else {
				observeErr = fmt.Errorf("physical identity changed for %q before mutation", relPath)
			}
		} else {
			observeErr = statErr
		}
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
func preserveExpectedCopySymlinkAt(ctx context.Context, relPath, targetPath, recoveryPath, expectedLinkTarget string, expected domain.LocalFingerprint) (preservedLocalEntry, error) {
	info, err := os.Lstat(targetPath)
	if err != nil {
		return preservedLocalEntry{}, fmt.Errorf("lstat copy projection %q before mutation: %w", relPath, err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return preservedLocalEntry{}, fmt.Errorf("copy projection %q is no longer a symlink", relPath)
	}
	linkTarget, err := os.Readlink(targetPath)
	if err != nil {
		return preservedLocalEntry{}, fmt.Errorf("read copy projection %q before mutation: %w", relPath, err)
	}
	if linkTarget != expectedLinkTarget {
		return preservedLocalEntry{}, fmt.Errorf("copy projection %q retargeted before mutation", relPath)
	}
	if err := movePathNoReplace(targetPath, recoveryPath); err != nil {
		return preservedLocalEntry{}, fmt.Errorf("preserve copy projection %q before mutation: %w", relPath, err)
	}
	preserved := preservedLocalEntry{targetPath: targetPath, recoveryPath: recoveryPath}
	recoveryInfo, err := os.Lstat(recoveryPath)
	if err == nil && recoveryInfo.Mode()&os.ModeSymlink != 0 {
		movedTarget, readErr := os.Readlink(recoveryPath)
		if readErr == nil && movedTarget == expectedLinkTarget {
			projected, observeErr := observeProjectedPinnedSymlink(ctx, recoveryPath)
			if observeErr == nil && domain.LocalEquivalent(projected, expected) {
				return preserved, nil
			}
			if observeErr != nil {
				err = observeErr
			} else {
				err = fmt.Errorf("preserved copy projection %q has changed projected referent state", relPath)
			}
		} else if readErr != nil {
			err = readErr
		} else {
			err = fmt.Errorf("preserved copy projection %q has unexpected link target", relPath)
		}
	} else if err == nil {
		err = fmt.Errorf("preserved copy projection %q is no longer a symlink", relPath)
	}
	if restoreErr := movePathNoReplace(recoveryPath, targetPath); restoreErr != nil {
		return preservedLocalEntry{}, errors.Join(err, fmt.Errorf("copy projection is preserved at %q because restoring %q failed: %w", recoveryPath, relPath, restoreErr))
	}
	return preservedLocalEntry{}, err
}

func (e *RootExecutor) installDownloadedPhysicalTemp(ctx context.Context, relPath, targetPath, tempPath, recoveryPath string, expectedLocal domain.LocalFingerprint, expectedIdentity string) error {
	if !expectedLocal.Present {
		if err := movePathNoReplace(tempPath, targetPath); err != nil {
			return fmt.Errorf("install downloaded file %q without replacing a concurrent local create: %w", relPath, err)
		}
		return nil
	}
	preserved, err := preserveExpectedLocalEntryAt(ctx, relPath, targetPath, recoveryPath, expectedLocal, expectedIdentity)
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
	return e.installDownloadedPhysicalTemp(ctx, relPath, targetPath, tempPath, recoveryPath, expectedLocal, "")
}

func (e *RootExecutor) commitDownloadedPhysicalTemp(ctx context.Context, op domain.Operation, tempPath string, peerLocalRoots []string, beginSideEffect func() error) (domain.LocalFingerprint, error) {
	if operationMayHaveStarted(op) {
		holds, err := e.PinnedLocalPreconditionHolds(ctx, op)
		if err != nil {
			return domain.LocalFingerprint{}, err
		}
		if !holds {
			return domain.LocalFingerprint{}, fmt.Errorf("pinned local download precondition changed for %q", op.SrcPath)
		}
	} else {
		currentLocal, err := e.ObserveLocalEntry(ctx, op.SrcPath)
		if err != nil {
			return domain.LocalFingerprint{}, err
		}
		if !domain.LocalEquivalent(currentLocal, op.ExpectedLocal) {
			return domain.LocalFingerprint{}, fmt.Errorf("local download precondition failed for %q", op.SrcPath)
		}
		currentTarget, err := e.ResolveLocalMutationTarget(ctx, op.SrcPath, op.ExpectedLocal, op.EntryKind, peerLocalRoots)
		if err != nil {
			return domain.LocalFingerprint{}, err
		}
		if !localMutationTargetMatchesPin(op, currentTarget) {
			return domain.LocalFingerprint{}, fmt.Errorf("local symlink target changed during download for %q", op.SrcPath)
		}
	}
	if err := verifyPinnedLocalMutationAnchor(op); err != nil {
		return domain.LocalFingerprint{}, err
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
	if beginSideEffect == nil {
		return domain.LocalFingerprint{}, fmt.Errorf("local file mutation requires a side-effect start callback")
	}
	if err := beginSideEffect(); err != nil {
		return domain.LocalFingerprint{}, fmt.Errorf("record local file side-effect start for %q: %w", op.SrcPath, err)
	}
	recoveryPath := operationPhysicalTempPath(op.LocalTargetPath, op.ID, "recovery")
	if op.LocalSymlinkTarget != "" {
		preserved, err := preserveExpectedCopySymlinkAt(ctx, op.SrcPath, op.LocalTargetPath, recoveryPath, op.LocalSymlinkTarget, op.ExpectedLocal)
		if err != nil {
			return domain.LocalFingerprint{}, fmt.Errorf("replace copy projection %q: %w", op.SrcPath, err)
		}
		if err := movePathNoReplace(tempPath, op.LocalTargetPath); err != nil {
			return domain.LocalFingerprint{}, restorePreservedLocalEntry(preserved, op.SrcPath, fmt.Errorf("install downloaded file %q: %w", op.SrcPath, err))
		}
		if err := os.Remove(preserved.recoveryPath); err != nil {
			return domain.LocalFingerprint{}, fmt.Errorf("download installed at %q but could not remove preserved copy projection %q: %w", op.SrcPath, preserved.recoveryPath, err)
		}
		return temp, nil
	}
	if err := e.installDownloadedPhysicalTemp(ctx, op.SrcPath, op.LocalTargetPath, tempPath, recoveryPath, op.ExpectedLocal, op.LocalTargetIdentity); err != nil {
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

func (e *RootExecutor) ResolveLocalMutationTarget(ctx context.Context, relPath string, expected domain.LocalFingerprint, entryKind domain.EntryKind, peerLocalRoots []string) (domain.LocalMutationTarget, error) {
	if err := ctx.Err(); err != nil {
		return domain.LocalMutationTarget{}, err
	}
	current, err := e.ObserveLocalEntry(ctx, relPath)
	if err != nil {
		return domain.LocalMutationTarget{}, err
	}
	if !domain.LocalEquivalent(current, expected) {
		return domain.LocalMutationTarget{}, fmt.Errorf("local mutation target precondition failed for %q", relPath)
	}
	target, err := e.resolveLocalMutationAuthority(relPath, expected.Present, entryKind, peerLocalRoots)
	if err != nil {
		return domain.LocalMutationTarget{}, err
	}
	anchorPath := target.Path
	if !expected.Present || target.SymlinkTarget != "" {
		anchorPath = filepath.Dir(target.Path)
	}
	anchorInfo, err := os.Stat(anchorPath)
	if err != nil {
		return domain.LocalMutationTarget{}, fmt.Errorf("stat local mutation physical anchor %q: %w", anchorPath, err)
	}
	if (!expected.Present || target.SymlinkTarget != "") && !anchorInfo.IsDir() {
		return domain.LocalMutationTarget{}, fmt.Errorf("local mutation physical anchor %q is not a directory", anchorPath)
	}
	identity, err := physicalObjectIdentity(anchorPath, anchorInfo)
	if err != nil {
		return domain.LocalMutationTarget{}, fmt.Errorf("identify local mutation physical anchor %q: %w", anchorPath, err)
	}
	target.AnchorIdentity = identity
	return target, nil
}

// resolveLocalMutationAuthority resolves one logical mutation path to either
// lexical authority, strict follow physical authority, or copy-mode
// operation-local physical authority.
func (e *RootExecutor) resolveLocalMutationAuthority(relPath string, present bool, entryKind domain.EntryKind, peerLocalRoots []string) (domain.LocalMutationTarget, error) {
	if err := domain.ValidateRelPath(relPath); err != nil {
		return domain.LocalMutationTarget{}, err
	}
	lexical := filepath.Join(e.root.LocalRoot, filepath.FromSlash(relPath))
	mode := e.root.EffectiveSymlinkMode()
	if mode == domain.SymlinkCopy {
		return e.resolveCopyLocalMutationAuthority(relPath, present, peerLocalRoots)
	}
	if mode != domain.SymlinkFollow {
		return domain.LocalMutationTarget{Path: filepath.Clean(lexical), Authority: domain.LocalMutationLexical}, nil
	}

	parts := strings.Split(relPath, "/")
	physicalDir := e.root.LocalRoot
	claims := make(map[string]domain.FollowedPhysicalClaim)
	for i, part := range parts {
		logical := strings.Join(parts[:i+1], "/")
		candidate := filepath.Join(physicalDir, filepath.FromSlash(part))
		info, err := os.Lstat(candidate)
		if errors.Is(err, os.ErrNotExist) {
			if i == len(parts)-1 && !present {
				authority := domain.LocalMutationLexical
				if len(claims) != 0 {
					authority = domain.LocalMutationFollowPhysical
				}
				return domain.LocalMutationTarget{Path: filepath.Clean(candidate), Authority: authority, FollowedClaims: claims}, nil
			}
			return domain.LocalMutationTarget{}, fmt.Errorf("local mutation path component %q disappeared", logical)
		}
		if err != nil {
			return domain.LocalMutationTarget{}, fmt.Errorf("lstat local mutation path %q: %w", logical, err)
		}

		if info.Mode()&os.ModeSymlink != 0 {
			resolved, resolvedPresent, err := resolveFollowedLocalPath(candidate, i == len(parts)-1 && !present)
			if err != nil {
				return domain.LocalMutationTarget{}, fmt.Errorf("resolve followed local mutation boundary %q: %w", logical, err)
			}
			if !resolvedPresent {
				if i != len(parts)-1 || present || (entryKind != domain.KindFile && entryKind != domain.KindDir) {
					return domain.LocalMutationTarget{}, fmt.Errorf("cannot resolve dangling followed symlink %q for local mutation", logical)
				}
				claims[logical] = domain.FollowedPhysicalClaim{Kind: entryKind, TargetPath: filepath.Clean(resolved)}
				return domain.LocalMutationTarget{Path: filepath.Clean(resolved), Authority: domain.LocalMutationFollowPhysical, FollowedClaims: claims}, nil
			}
			resolvedInfo, err := os.Stat(resolved)
			if err != nil {
				return domain.LocalMutationTarget{}, fmt.Errorf("stat followed local mutation boundary %q: %w", logical, err)
			}
			var kind domain.EntryKind
			switch {
			case resolvedInfo.IsDir():
				kind = domain.KindDir
			case resolvedInfo.Mode().IsRegular():
				kind = domain.KindFile
			default:
				return domain.LocalMutationTarget{}, fmt.Errorf("unsupported followed local mutation boundary %q (%s)", logical, resolvedInfo.Mode().Type())
			}
			identity, err := physicalObjectIdentity(resolved, resolvedInfo)
			if err != nil {
				return domain.LocalMutationTarget{}, fmt.Errorf("identify followed local mutation boundary %q: %w", logical, err)
			}
			claims[logical] = domain.FollowedPhysicalClaim{Kind: kind, Identity: identity, TargetPath: filepath.Clean(resolved)}
			if i == len(parts)-1 {
				return domain.LocalMutationTarget{Path: filepath.Clean(resolved), Authority: domain.LocalMutationFollowPhysical, FollowedClaims: claims}, nil
			}
			if kind != domain.KindDir {
				return domain.LocalMutationTarget{}, fmt.Errorf("followed file %q cannot contain descendants", logical)
			}
			physicalDir = filepath.Clean(resolved)
			continue
		}

		if i == len(parts)-1 {
			authority := domain.LocalMutationLexical
			if len(claims) != 0 {
				authority = domain.LocalMutationFollowPhysical
			}
			return domain.LocalMutationTarget{Path: filepath.Clean(candidate), Authority: authority, FollowedClaims: claims}, nil
		}
		if !info.IsDir() {
			return domain.LocalMutationTarget{}, fmt.Errorf("local mutation path component %q is not a directory", logical)
		}
		physicalDir = candidate
	}
	return domain.LocalMutationTarget{}, fmt.Errorf("failed to resolve local mutation target %q", relPath)
}

// resolveCopyLocalMutationAuthority treats a final symlink as the lexical
// projection object, while resolving directory symlinks in ancestor position
// to an operation-local physical destination. No global ownership is acquired.
func (e *RootExecutor) resolveCopyLocalMutationAuthority(relPath string, present bool, peerLocalRoots []string) (domain.LocalMutationTarget, error) {
	parts := strings.Split(relPath, "/")
	physicalDir := e.root.LocalRoot
	throughCopyDir := false
	copyEvidence := make(map[string]domain.CopyProjectionEvidence)
	for i, part := range parts {
		logical := strings.Join(parts[:i+1], "/")
		candidate := filepath.Join(physicalDir, filepath.FromSlash(part))
		info, err := os.Lstat(candidate)
		if errors.Is(err, os.ErrNotExist) {
			if i == len(parts)-1 && !present {
				authority := domain.LocalMutationLexical
				if throughCopyDir {
					authority = domain.LocalMutationCopyPhysical
				}
				return domain.LocalMutationTarget{Path: filepath.Clean(candidate), Authority: authority, CopyProjectionEvidence: copyEvidence}, nil
			}
			return domain.LocalMutationTarget{}, fmt.Errorf("local copy mutation path component %q disappeared", logical)
		}
		if err != nil {
			return domain.LocalMutationTarget{}, fmt.Errorf("lstat local copy mutation path %q: %w", logical, err)
		}

		if info.Mode()&os.ModeSymlink != 0 {
			if i == len(parts)-1 {
				linkTarget, err := os.Readlink(candidate)
				if err != nil {
					return domain.LocalMutationTarget{}, fmt.Errorf("read final copy symlink %q: %w", logical, err)
				}
				resolved, resolvedPresent, err := resolveFollowedLocalPath(candidate, false)
				if err != nil || !resolvedPresent {
					if err == nil {
						err = os.ErrNotExist
					}
					return domain.LocalMutationTarget{}, fmt.Errorf("resolve final copy symlink %q: %w", logical, err)
				}
				if err := rejectPeerRootPath(resolved, peerLocalRoots); err != nil {
					return domain.LocalMutationTarget{}, fmt.Errorf("copy symlink %q: %w", logical, err)
				}
				if err := e.rejectForeignRootTarget(resolved); err != nil {
					return domain.LocalMutationTarget{}, fmt.Errorf("copy symlink %q: %w", logical, err)
				}
				resolvedInfo, err := os.Stat(resolved)
				if err != nil {
					return domain.LocalMutationTarget{}, fmt.Errorf("stat final copy symlink %q: %w", logical, err)
				}
				switch {
				case resolvedInfo.Mode().IsRegular():
				case resolvedInfo.IsDir():
					physicalDirInfo, err := os.Stat(physicalDir)
					if err != nil {
						return domain.LocalMutationTarget{}, fmt.Errorf("stat copy mutation ancestor for %q: %w", logical, err)
					}
					resolvedIsAncestor, err := strictPhysicalPathAncestor(resolved, physicalDir)
					if err != nil {
						return domain.LocalMutationTarget{}, fmt.Errorf("compare final copy-symlink ancestry for %q: %w", logical, err)
					}
					if os.SameFile(resolvedInfo, physicalDirInfo) || resolvedIsAncestor {
						return domain.LocalMutationTarget{}, fmt.Errorf("final copy symlink %q forms a physical directory cycle", logical)
					}
				default:
					return domain.LocalMutationTarget{}, fmt.Errorf("unsupported final copy symlink %q (%s)", logical, resolvedInfo.Mode().Type())
				}
				evidence, err := copyProjectionEvidenceFor(resolved, resolvedInfo)
				if err != nil {
					return domain.LocalMutationTarget{}, fmt.Errorf("identify final copy symlink %q: %w", logical, err)
				}
				copyEvidence[logical] = evidence
				authority := domain.LocalMutationLexical
				if throughCopyDir {
					authority = domain.LocalMutationCopyPhysical
				}
				return domain.LocalMutationTarget{Path: filepath.Clean(candidate), Authority: authority, SymlinkTarget: linkTarget, CopyProjectionEvidence: copyEvidence}, nil
			}
			resolved, resolvedPresent, err := resolveFollowedLocalPath(candidate, false)
			if err != nil || !resolvedPresent {
				if err == nil {
					err = os.ErrNotExist
				}
				return domain.LocalMutationTarget{}, fmt.Errorf("resolve copy directory boundary %q: %w", logical, err)
			}
			if err := rejectPeerRootPath(resolved, peerLocalRoots); err != nil {
				return domain.LocalMutationTarget{}, fmt.Errorf("copy symlink %q: %w", logical, err)
			}
			if err := e.rejectForeignRootTarget(resolved); err != nil {
				return domain.LocalMutationTarget{}, fmt.Errorf("copy symlink %q: %w", logical, err)
			}
			resolvedInfo, err := os.Stat(resolved)
			if err != nil {
				return domain.LocalMutationTarget{}, fmt.Errorf("stat copy directory boundary %q: %w", logical, err)
			}
			if !resolvedInfo.IsDir() {
				return domain.LocalMutationTarget{}, fmt.Errorf("copy file %q cannot contain descendants", logical)
			}
			evidence, err := copyProjectionEvidenceFor(resolved, resolvedInfo)
			if err != nil {
				return domain.LocalMutationTarget{}, fmt.Errorf("identify copy directory boundary %q: %w", logical, err)
			}
			copyEvidence[logical] = evidence
			physicalDir = filepath.Clean(resolved)
			throughCopyDir = true
			continue
		}

		if i == len(parts)-1 {
			authority := domain.LocalMutationLexical
			if throughCopyDir {
				authority = domain.LocalMutationCopyPhysical
			}
			return domain.LocalMutationTarget{Path: filepath.Clean(candidate), Authority: authority, CopyProjectionEvidence: copyEvidence}, nil
		}
		if !info.IsDir() {
			return domain.LocalMutationTarget{}, fmt.Errorf("local copy mutation path component %q is not a directory", logical)
		}
		physicalDir = candidate
	}
	return domain.LocalMutationTarget{}, fmt.Errorf("failed to resolve copy local mutation target %q", relPath)
}

func canonicalLocalMutationPathname(name string) (string, error) {
	parent, err := canonicalExistingLocalPath(filepath.Dir(name))
	if err != nil {
		return "", err
	}
	return filepath.Join(parent, filepath.Base(name)), nil
}

func samePhysicalDestination(a, b string, _ bool) bool {
	canonicalA, aErr := canonicalLocalMutationPathname(a)
	canonicalB, bErr := canonicalLocalMutationPathname(b)
	if aErr != nil || bErr != nil {
		return false
	}
	if runtime.GOOS == "windows" {
		return strings.EqualFold(canonicalA, canonicalB)
	}
	return canonicalA == canonicalB
}

func localMutationTargetMatchesPin(op domain.Operation, current domain.LocalMutationTarget) bool {
	// Mutation authority is a pathname, not merely an inode/file ID. Canonicalize
	// only the parent spelling so /var vs /private/var and Windows parent aliases
	// compare equal without allowing a retarget to a distinct hard-link pathname.
	if !samePhysicalDestination(op.LocalTargetPath, current.Path, false) {
		return false
	}
	if op.LocalTargetAuthority != "" && current.Authority != op.LocalTargetAuthority {
		return false
	}
	if op.LocalSymlinkTarget != current.SymlinkTarget {
		return false
	}
	return true
}

func verifyPinnedLocalMutationAnchor(op domain.Operation) error {
	if op.LocalTargetPath == "" || op.LocalTargetIdentity == "" {
		return fmt.Errorf("local operation %d has no complete physical target pin", op.ID)
	}
	anchorPath := op.LocalTargetPath
	if !op.ExpectedLocal.Present || op.LocalSymlinkTarget != "" {
		anchorPath = filepath.Dir(op.LocalTargetPath)
	}
	info, err := os.Stat(anchorPath)
	if err != nil {
		return fmt.Errorf("stat pinned local mutation anchor %q: %w", anchorPath, err)
	}
	if (!op.ExpectedLocal.Present || op.LocalSymlinkTarget != "") && !info.IsDir() {
		return fmt.Errorf("pinned local mutation anchor %q is not a directory", anchorPath)
	}
	identity, err := physicalObjectIdentity(anchorPath, info)
	if err != nil {
		return fmt.Errorf("identify pinned local mutation anchor %q: %w", anchorPath, err)
	}
	if identity != op.LocalTargetIdentity {
		return fmt.Errorf("local physical target identity changed before mutation for %q", op.SrcPath)
	}
	return nil
}

func validatePinnedLocalOperation(op domain.Operation) error {
	if op.Kind != domain.OperationEnsureLocal && op.Kind != domain.OperationDeleteLocal {
		return fmt.Errorf("operation %d is not a local mutation", op.ID)
	}
	if op.LocalTargetPath == "" || op.LocalTargetIdentity == "" || op.LocalTargetAuthority == "" {
		return fmt.Errorf("local operation %d has no complete physical target pin", op.ID)
	}
	if !filepath.IsAbs(op.LocalTargetPath) || filepath.Clean(op.LocalTargetPath) != op.LocalTargetPath {
		return fmt.Errorf("local operation %d has invalid pinned target path %q", op.ID, op.LocalTargetPath)
	}
	return nil
}

func operationMayHaveStarted(op domain.Operation) bool {
	return op.Phase == domain.OperationRunning || op.Phase == domain.OperationRecovering
}

// PinnedLocalPreconditionHolds proves safe replay after an earlier local
// mutation attempt without consulting the current lexical symlink chain. A
// copy-mode final symlink is checked by its durable readlink value plus the
// projected referent fingerprint; ordinary targets are checked directly at the
// journaled physical path. The original physical anchor must still match.
func (e *RootExecutor) PinnedLocalPreconditionHolds(ctx context.Context, op domain.Operation) (bool, error) {
	if err := validatePinnedLocalOperation(op); err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if op.LocalSymlinkTarget != "" {
		info, err := os.Lstat(op.LocalTargetPath)
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("lstat pinned copy symlink %q: %w", op.LocalTargetPath, err)
		}
		if info.Mode()&os.ModeSymlink == 0 {
			return false, nil
		}
		linkTarget, err := os.Readlink(op.LocalTargetPath)
		if err != nil {
			return false, fmt.Errorf("read pinned copy symlink %q: %w", op.LocalTargetPath, err)
		}
		if linkTarget != op.LocalSymlinkTarget {
			return false, nil
		}
		projected, err := observeProjectedPinnedSymlink(ctx, op.LocalTargetPath)
		if err != nil {
			return false, err
		}
		if !domain.LocalEquivalent(projected, op.ExpectedLocal) {
			return false, nil
		}
	} else {
		current, ordinary, err := e.ObservePinnedLocalEntry(ctx, op)
		if err != nil {
			return false, err
		}
		if !ordinary || !domain.LocalEquivalent(current, op.ExpectedLocal) {
			return false, nil
		}
	}
	if err := verifyPinnedLocalMutationAnchor(op); err != nil {
		return false, nil
	}
	return true, nil
}

func observeProjectedPinnedSymlink(ctx context.Context, fullPath string) (domain.LocalFingerprint, error) {
	if err := ctx.Err(); err != nil {
		return domain.LocalFingerprint{}, err
	}
	info, err := os.Stat(fullPath)
	if errors.Is(err, os.ErrNotExist) {
		return domain.LocalFingerprint{}, nil
	}
	if err != nil {
		return domain.LocalFingerprint{}, fmt.Errorf("stat pinned copy projection %q: %w", fullPath, err)
	}
	if info.IsDir() {
		return domain.LocalFingerprint{Present: true, Kind: domain.KindDir}, nil
	}
	if info.Mode().IsRegular() {
		return localFileFingerprint(info), nil
	}
	return domain.LocalFingerprint{}, fmt.Errorf("unsupported pinned copy projection %q (%s)", fullPath, info.Mode().Type())
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

// CleanupLocalDownloadArtifact removes the one disposable pre-side-effect
// staging slot that a fully pinned, unattempted file EnsureLocal may own. The
// path comes only from the durable pin and operation ID; current symlink state
// is never consulted. Restart recovery calls this before deciding whether the
// old planned intent is still current, so discarding that intent cannot orphan
// its staging file and wedge later complete scans.
func (e *RootExecutor) CleanupLocalDownloadArtifact(ctx context.Context, op domain.Operation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !operationOwnsPlannedDownloadArtifact(op) {
		return fmt.Errorf("operation %d does not own a disposable planned download artifact", op.ID)
	}
	if !filepath.IsAbs(op.LocalTargetPath) || filepath.Clean(op.LocalTargetPath) != op.LocalTargetPath {
		return fmt.Errorf("operation %d has invalid pinned local target %q", op.ID, op.LocalTargetPath)
	}
	downloadPath := operationPhysicalTempPath(op.LocalTargetPath, op.ID, "download")
	if err := os.Remove(downloadPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale planned download artifact %q: %w", downloadPath, err)
	}
	return nil
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
// the durable pinned target path identifies the operation-owned slot, while the
// paired physical identity prevents an unrelated replacement at that slot from
// being silently deleted.
func (e *RootExecutor) CleanupLocalRecoveryArtifact(ctx context.Context, op domain.Operation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	recoveryPath, ok := RecoveryArtifactPath(op)
	if !ok {
		return nil
	}
	if op.LocalSymlinkTarget != "" {
		info, err := os.Lstat(recoveryPath)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("lstat stale copy recovery artifact %q: %w", recoveryPath, err)
		}
		if info.Mode()&os.ModeSymlink == 0 {
			return fmt.Errorf("refusing to remove stale copy recovery artifact %q because it is not a symlink", recoveryPath)
		}
		target, err := os.Readlink(recoveryPath)
		if err != nil {
			return fmt.Errorf("read stale copy recovery artifact %q: %w", recoveryPath, err)
		}
		if target != op.LocalSymlinkTarget {
			return fmt.Errorf("refusing to remove stale copy recovery artifact %q because its link target changed", recoveryPath)
		}
		if err := os.Remove(recoveryPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove stale copy recovery artifact %q: %w", recoveryPath, err)
		}
		return nil
	}
	info, err := os.Stat(recoveryPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat stale local recovery artifact %q: %w", recoveryPath, err)
	}
	if op.LocalTargetIdentity == "" {
		return fmt.Errorf("refusing to remove stale local recovery artifact %q without a pinned physical identity", recoveryPath)
	}
	identity, err := physicalObjectIdentity(recoveryPath, info)
	if err != nil {
		return fmt.Errorf("identify stale local recovery artifact %q: %w", recoveryPath, err)
	}
	if identity != op.LocalTargetIdentity {
		return fmt.Errorf("refusing to remove stale local recovery artifact %q because its physical identity changed", recoveryPath)
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
// operation's expected pre-mutation fingerprint and, for v6+ pins, exact
// physical identity.
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
	if op.LocalSymlinkTarget != "" {
		return restoreCopySymlinkRecoveryArtifact(op, recoveryPath)
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
	if op.LocalTargetIdentity != "" {
		info, err := os.Stat(recoveryPath)
		if err != nil {
			return recoveryPath, fmt.Errorf("stat local recovery artifact %q: %w", recoveryPath, err)
		}
		identity, err := physicalObjectIdentity(recoveryPath, info)
		if err != nil {
			return recoveryPath, fmt.Errorf("identify local recovery artifact %q: %w", recoveryPath, err)
		}
		if identity != op.LocalTargetIdentity {
			return recoveryPath, fmt.Errorf("local recovery artifact %q does not match operation %d pinned physical identity", recoveryPath, op.ID)
		}
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
	if err == nil && domain.LocalEquivalent(restored, op.ExpectedLocal) && op.LocalTargetIdentity != "" {
		info, statErr := os.Stat(op.LocalTargetPath)
		if statErr == nil {
			identity, identityErr := physicalObjectIdentity(op.LocalTargetPath, info)
			if identityErr == nil && identity == op.LocalTargetIdentity {
				return recoveryPath, nil
			}
			if identityErr != nil {
				err = identityErr
			} else {
				err = fmt.Errorf("restored local target %q does not match operation %d pinned physical identity", op.LocalTargetPath, op.ID)
			}
		} else {
			err = statErr
		}
	} else if err == nil && domain.LocalEquivalent(restored, op.ExpectedLocal) && op.LocalTargetIdentity == "" {
		return recoveryPath, nil
	} else if err == nil {
		err = fmt.Errorf("restored local target %q does not match operation %d expected local state", op.LocalTargetPath, op.ID)
	}
	primary := err
	if restoreErr := movePathNoReplace(op.LocalTargetPath, recoveryPath); restoreErr != nil {
		return recoveryPath, errors.Join(primary, fmt.Errorf("unexpected restored object remains at %q because moving it back to recovery slot %q failed: %w", op.LocalTargetPath, recoveryPath, restoreErr))
	}
	return recoveryPath, primary
}
func restoreCopySymlinkRecoveryArtifact(op domain.Operation, recoveryPath string) (string, error) {
	info, err := os.Lstat(recoveryPath)
	if err != nil {
		return recoveryPath, fmt.Errorf("lstat copy recovery artifact %q: %w", recoveryPath, err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return recoveryPath, fmt.Errorf("copy recovery artifact %q is not a symlink", recoveryPath)
	}
	linkTarget, err := os.Readlink(recoveryPath)
	if err != nil {
		return recoveryPath, fmt.Errorf("read copy recovery artifact %q: %w", recoveryPath, err)
	}
	if linkTarget != op.LocalSymlinkTarget {
		return recoveryPath, fmt.Errorf("copy recovery artifact %q no longer matches operation %d symlink target", recoveryPath, op.ID)
	}
	if _, err := os.Lstat(op.LocalTargetPath); err == nil {
		return recoveryPath, fmt.Errorf("refusing to restore %q because pinned local target %q already exists", recoveryPath, op.LocalTargetPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return recoveryPath, fmt.Errorf("lstat pinned local target %q: %w", op.LocalTargetPath, err)
	}
	if err := movePathNoReplace(recoveryPath, op.LocalTargetPath); err != nil {
		return recoveryPath, fmt.Errorf("restore copy recovery artifact %q: %w", recoveryPath, err)
	}
	restoredTarget, err := os.Readlink(op.LocalTargetPath)
	if err == nil && restoredTarget == op.LocalSymlinkTarget {
		return recoveryPath, nil
	}
	primary := err
	if primary == nil {
		primary = fmt.Errorf("restored copy projection %q has unexpected link target", op.LocalTargetPath)
	}
	if restoreErr := movePathNoReplace(op.LocalTargetPath, recoveryPath); restoreErr != nil {
		return recoveryPath, errors.Join(primary, fmt.Errorf("unexpected restored copy object remains at %q because moving it back to recovery slot %q failed: %w", op.LocalTargetPath, recoveryPath, restoreErr))
	}
	return recoveryPath, primary
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

// CompareFileContent opens the exact planned remote revision as a guarded
// stream and compares it byte-for-byte with an unchanged local file. Plan-time
// comparison creates no local staging artifact, so a crash cannot leak internal
// bytes into any lexical or projected sync namespace.
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

	remoteObject, err := e.remote.NewObject(ctx, relPath)
	if err != nil {
		return false, fmt.Errorf("open remote comparison source %q: %w", relPath, err)
	}
	remoteReader, err := remoteObject.Open(ctx,
		&fs.HTTPOption{Key: syncExpectedIDDownloadHeader, Value: expectedRemote.ID},
		&fs.HTTPOption{Key: syncExpectedRevDownloadHeader, Value: expectedRemote.Rev},
	)
	if err != nil {
		return false, fmt.Errorf("open exact remote revision %q: %w", relPath, err)
	}
	localPath := filepath.Join(e.root.LocalRoot, filepath.FromSlash(relPath))
	equal, compareErr := fileReaderEqual(localPath, remoteReader)
	_ = remoteReader.Close()
	if compareErr != nil {
		return false, compareErr
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

// ComparePinnedFileContent proves recovery content against the exact physical
// local destination recorded in the operation journal. It never resolves the
// current lexical alias, so a copy-directory retarget after an attempted side
// effect cannot redirect the proof.
func (e *RootExecutor) ComparePinnedFileContent(ctx context.Context, op domain.Operation, expectedRemote domain.RemoteExpectation) (bool, error) {
	if err := validatePinnedLocalOperation(op); err != nil {
		return false, err
	}
	if op.EntryKind != domain.KindFile {
		return false, fmt.Errorf("pinned content comparison requires a file operation")
	}
	if err := validateFileRemoteExpectation(expectedRemote); err != nil || expectedRemote.Absent {
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("pinned content comparison requires a present remote expectation")
	}
	before, ordinary, err := e.ObservePinnedLocalEntry(ctx, op)
	if err != nil {
		return false, err
	}
	if !ordinary || !before.Present || before.Kind != domain.KindFile {
		return false, nil
	}

	tempPath := operationPhysicalTempPath(op.LocalTargetPath, op.ID, "download")
	_ = os.Remove(tempPath)
	defer func() { _ = os.Remove(tempPath) }()
	if err := e.downloadToPhysicalTemp(ctx, op.SrcPath, tempPath, expectedRemote); err != nil {
		return false, err
	}
	equal, err := filesEqual(op.LocalTargetPath, tempPath)
	if err != nil {
		return false, err
	}
	after, ordinary, err := e.ObservePinnedLocalEntry(ctx, op)
	if err != nil {
		return false, err
	}
	if !ordinary || !domain.LocalEquivalent(after, before) {
		return false, fmt.Errorf("pinned local file changed during recovery content comparison for %q", op.SrcPath)
	}
	remote, err := e.ObserveRemoteEntry(ctx, op.SrcPath)
	if err != nil {
		return false, err
	}
	if !remoteMatchesExpectation(remote, expectedRemote, domain.KindFile) {
		return false, fmt.Errorf("remote file changed during pinned content comparison for %q", op.SrcPath)
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

func fileReaderEqual(localPath string, remote io.Reader) (bool, error) {
	local, err := os.Open(localPath)
	if err != nil {
		return false, fmt.Errorf("open local comparison file: %w", err)
	}
	defer func() { _ = local.Close() }()
	return readersEqual(local, remote)
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
	return readersEqual(left, right)
}

func readersEqual(left, right io.Reader) (bool, error) {
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
