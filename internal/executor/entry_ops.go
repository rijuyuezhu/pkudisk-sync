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

// ObserveLocalEntry returns the exact ordinary-file/directory state currently
// visible at relPath. Unsupported local types are errors, never absence.
func (e *RootExecutor) ObserveLocalEntry(ctx context.Context, relPath string) (domain.LocalFingerprint, error) {
	if err := domain.ValidateRelPath(relPath); err != nil {
		return domain.LocalFingerprint{}, err
	}
	if err := ctx.Err(); err != nil {
		return domain.LocalFingerprint{}, err
	}
	fullPath := filepath.Join(e.root.LocalRoot, filepath.FromSlash(relPath))
	info, err := os.Lstat(fullPath)
	if errors.Is(err, os.ErrNotExist) {
		return domain.LocalFingerprint{}, nil
	}
	if err != nil {
		return domain.LocalFingerprint{}, fmt.Errorf("stat local entry %q: %w", relPath, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return domain.LocalFingerprint{}, fmt.Errorf("unsupported local symlink %q", relPath)
	}
	if info.IsDir() {
		return domain.LocalFingerprint{Present: true, Kind: domain.KindDir}, nil
	}
	if !info.Mode().IsRegular() {
		return domain.LocalFingerprint{}, fmt.Errorf("unsupported local file type %q (%s)", relPath, info.Mode().Type())
	}
	return domain.LocalFingerprint{
		Present: true,
		Kind:    domain.KindFile,
		Size:    info.Size(),
		MtimeNS: info.ModTime().UnixNano(),
	}, nil
}

// ObserveRemoteEntry resolves one path through its parent listing, so it can
// observe both files and directories and preserve PKU Disk object identity.
func (e *RootExecutor) ObserveRemoteEntry(ctx context.Context, relPath string) (domain.RemoteFingerprint, error) {
	if err := domain.ValidateRelPath(relPath); err != nil {
		return domain.RemoteFingerprint{}, err
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

// EnsureLocalDir creates exactly one planned directory. Parents must already
// exist, which lets the coordinator enforce shallow-to-deep ordering.
func (e *RootExecutor) EnsureLocalDir(ctx context.Context, relPath string, expected domain.LocalFingerprint) error {
	if err := expected.Validate(); err != nil {
		return fmt.Errorf("expected local state: %w", err)
	}
	if expected.Present {
		return fmt.Errorf("local directory create requires an absent expectation")
	}
	current, err := e.ObserveLocalEntry(ctx, relPath)
	if err != nil {
		return err
	}
	if current.Present {
		return fmt.Errorf("local directory create precondition failed for %q", relPath)
	}
	fullPath := filepath.Join(e.root.LocalRoot, filepath.FromSlash(relPath))
	if err := os.Mkdir(fullPath, 0o755); err != nil {
		return fmt.Errorf("create local directory %q: %w", relPath, err)
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

// EnsureLocalFile downloads the exact planned remote revision to a unique
// same-directory temp and only then commits it under local/remote preconditions.
// It returns the exact stat fingerprint of the temp that was atomically moved
// into place so the coordinator can detect a local write racing postcondition
// observation.
func (e *RootExecutor) EnsureLocalFile(ctx context.Context, relPath string, expectedLocal domain.LocalFingerprint, expectedRemote domain.RemoteExpectation) (domain.LocalFingerprint, error) {
	tempRel, err := newTempRelPath(relPath)
	if err != nil {
		return domain.LocalFingerprint{}, err
	}
	defer e.removeLocalTemp(tempRel)
	if err := e.DownloadToTemp(ctx, relPath, tempRel, expectedRemote); err != nil {
		return domain.LocalFingerprint{}, err
	}
	return e.CommitDownloadedTemp(ctx, relPath, tempRel, expectedLocal, expectedRemote)
}

// DeleteLocal removes exactly the expected file or an empty expected directory.
// It never recursively deletes a directory, so an unseen concurrent child makes
// the operation fail closed.
func (e *RootExecutor) DeleteLocal(ctx context.Context, relPath string, expected domain.LocalFingerprint) error {
	if err := expected.Validate(); err != nil {
		return fmt.Errorf("expected local state: %w", err)
	}
	if !expected.Present {
		return fmt.Errorf("local delete requires a present expectation")
	}
	current, err := e.ObserveLocalEntry(ctx, relPath)
	if err != nil {
		return err
	}
	if !domain.LocalEquivalent(current, expected) {
		return fmt.Errorf("local delete precondition failed for %q", relPath)
	}
	recoveryRel, err := e.preserveExpectedLocalEntry(ctx, relPath, expected)
	if err != nil {
		return err
	}
	recoveryPath := filepath.Join(e.root.LocalRoot, filepath.FromSlash(recoveryRel))
	if err := ctx.Err(); err != nil {
		return e.restorePreservedLocalEntry(relPath, recoveryRel, err)
	}
	if err := os.Remove(recoveryPath); err != nil {
		return e.restorePreservedLocalEntry(relPath, recoveryRel, fmt.Errorf("delete local %s %q: %w", expected.Kind, relPath, err))
	}
	return nil
}

// preserveExpectedLocalEntry atomically moves the current path into a unique
// same-directory recovery slot before validating the object that was actually
// moved. This closes the path-level Lstat->rename/unlink race: a concurrent
// save-by-rename is preserved rather than overwritten or deleted.
func (e *RootExecutor) preserveExpectedLocalEntry(ctx context.Context, relPath string, expected domain.LocalFingerprint) (string, error) {
	recoveryRel, err := newTempRelPath(relPath)
	if err != nil {
		return "", err
	}
	srcPath := filepath.Join(e.root.LocalRoot, filepath.FromSlash(relPath))
	recoveryPath := filepath.Join(e.root.LocalRoot, filepath.FromSlash(recoveryRel))
	if err := movePathNoReplace(srcPath, recoveryPath); err != nil {
		return "", fmt.Errorf("preserve local entry %q before mutation: %w", relPath, err)
	}

	observed, observeErr := e.ObserveLocalEntry(ctx, recoveryRel)
	if observeErr == nil && domain.LocalEquivalent(observed, expected) {
		return recoveryRel, nil
	}
	primary := observeErr
	if primary == nil {
		primary = fmt.Errorf("local mutation precondition failed for %q after preserving the actual path target", relPath)
	}
	if restoreErr := movePathNoReplace(recoveryPath, srcPath); restoreErr != nil {
		return "", errors.Join(primary, fmt.Errorf("local data is preserved at %q because restoring %q failed: %w", recoveryRel, relPath, restoreErr))
	}
	return "", primary
}

func (e *RootExecutor) restorePreservedLocalEntry(relPath, recoveryRel string, primary error) error {
	recoveryPath := filepath.Join(e.root.LocalRoot, filepath.FromSlash(recoveryRel))
	dstPath := filepath.Join(e.root.LocalRoot, filepath.FromSlash(relPath))
	if restoreErr := movePathNoReplace(recoveryPath, dstPath); restoreErr != nil {
		return errors.Join(primary, fmt.Errorf("local data is preserved at %q because restoring %q failed: %w", recoveryRel, relPath, restoreErr))
	}
	return primary
}

func (e *RootExecutor) installDownloadedTemp(ctx context.Context, relPath, tempRelPath string, expectedLocal domain.LocalFingerprint) error {
	tempPath := filepath.Join(e.root.LocalRoot, filepath.FromSlash(tempRelPath))
	dstPath := filepath.Join(e.root.LocalRoot, filepath.FromSlash(relPath))
	if !expectedLocal.Present {
		if err := movePathNoReplace(tempPath, dstPath); err != nil {
			return fmt.Errorf("install downloaded file %q without replacing a concurrent local create: %w", relPath, err)
		}
		return nil
	}

	recoveryRel, err := e.preserveExpectedLocalEntry(ctx, relPath, expectedLocal)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return e.restorePreservedLocalEntry(relPath, recoveryRel, err)
	}
	if err := movePathNoReplace(tempPath, dstPath); err != nil {
		return e.restorePreservedLocalEntry(relPath, recoveryRel, fmt.Errorf("install downloaded file %q without replacing a concurrent local write: %w", relPath, err))
	}
	recoveryPath := filepath.Join(e.root.LocalRoot, filepath.FromSlash(recoveryRel))
	if err := os.Remove(recoveryPath); err != nil {
		return fmt.Errorf("download installed at %q but could not remove preserved prior file %q: %w", relPath, recoveryRel, err)
	}
	return nil
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

func newTempRelPath(relPath string) (string, error) {
	if err := domain.ValidateRelPath(relPath); err != nil {
		return "", err
	}
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate temporary file name: %w", err)
	}
	name := tempNamePrefix + hex.EncodeToString(random[:])
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
