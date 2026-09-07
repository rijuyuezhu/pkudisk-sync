package executor

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
	pkudisk "github.com/rijuyuezhu/rclone-pkudisk/backend/pkudisk"
)

// RootExecutor is the in-process data plane for one selected sync-root pair.
// It owns no reconciliation state and starts no subprocesses.
type RootExecutor struct {
	root      domain.SyncRoot
	local     fs.Fs
	remote    fs.Fs
	remotePKU *pkudisk.Fs
}

// MoveResult is the exact object identity returned by PKU Disk after a
// server-side rename/move. Cross-directory moves may change the docid.
type MoveResult struct {
	ID     string
	Remote string
}

// NewRoot constructs local and PKU Disk filesystems in-process. remoteConfig
// is the mapper used by rclone-pkudisk for OAuth/backend settings; it should be
// backed by the application's persistent rclone configuration in production.
func NewRoot(ctx context.Context, root domain.SyncRoot, remoteConfig configmap.Mapper) (*RootExecutor, error) {
	if err := root.Validate(); err != nil {
		return nil, err
	}
	if remoteConfig == nil {
		return nil, fmt.Errorf("remote config mapper must not be nil")
	}

	localFS, err := local.NewFs(ctx, "local", root.LocalRoot, configmap.Simple{})
	if err != nil {
		return nil, fmt.Errorf("open local sync root %q: %w", root.LocalRoot, err)
	}
	remoteFS, err := pkudisk.NewFs(ctx, root.RemoteName, root.RemoteRoot, remoteConfig)
	if err != nil {
		return nil, fmt.Errorf("open PKU Disk sync root %q:%q: %w", root.RemoteName, root.RemoteRoot, err)
	}
	remotePKU, ok := remoteFS.(*pkudisk.Fs)
	if !ok {
		return nil, fmt.Errorf("PKU Disk constructor returned unexpected filesystem type %T", remoteFS)
	}
	return &RootExecutor{root: root, local: localFS, remote: remoteFS, remotePKU: remotePKU}, nil
}

// Upload copies the current local file to the same relative remote path under
// an explicit remote baseline precondition. The caller must still observe both
// sides after success before committing a new baseline; a local save can race
// with the transfer even though remote overwrite safety is CAS-protected.
func (e *RootExecutor) Upload(ctx context.Context, relPath string, expectedLocal domain.LocalFingerprint, expectedRemote domain.RemoteExpectation) error {
	if err := validateFilePathAndLocalExpectation(relPath, expectedLocal); err != nil {
		return err
	}
	if err := validateFileRemoteExpectation(expectedRemote); err != nil {
		return err
	}
	current, err := e.ObserveLocalFile(ctx, relPath)
	if err != nil {
		return err
	}
	if !domain.LocalEquivalent(current, expectedLocal) {
		return fmt.Errorf("local upload precondition failed for %q", relPath)
	}

	copyCtx := uploadContext(ctx, expectedRemote.ID, expectedRemote.Rev, expectedRemote.Absent)
	if err := operations.CopyFile(copyCtx, e.remote, e.local, relPath, relPath); err != nil {
		return fmt.Errorf("upload %q: %w", relPath, err)
	}
	return nil
}

// DownloadToTemp downloads an exact expected remote revision to a caller-owned
// unique temporary relative path. It never overwrites the canonical local path;
// atomic replacement remains a separate local-CAS step.
func (e *RootExecutor) DownloadToTemp(ctx context.Context, relPath, tempRelPath string, expected domain.RemoteExpectation) error {
	if err := domain.ValidateRelPath(relPath); err != nil {
		return err
	}
	if err := domain.ValidateRelPath(tempRelPath); err != nil {
		return fmt.Errorf("temporary path: %w", err)
	}
	if relPath == tempRelPath {
		return fmt.Errorf("temporary path must differ from canonical path")
	}
	if err := validateFileRemoteExpectation(expected); err != nil {
		return err
	}
	if expected.Absent {
		return fmt.Errorf("download requires a present remote expectation")
	}

	copyCtx := downloadContext(ctx, expected.ID, expected.Rev)
	if err := operations.CopyFile(copyCtx, e.local, e.remote, tempRelPath, relPath); err != nil {
		return fmt.Errorf("download %q: %w", relPath, err)
	}
	return nil
}

// DeleteRemoteFile deletes the exact expected docid/revision rather than
// resolving the path again at mutation time.
func (e *RootExecutor) DeleteRemoteFile(ctx context.Context, expected domain.RemoteExpectation) error {
	if err := validateFileRemoteExpectation(expected); err != nil {
		return err
	}
	if expected.Absent {
		return fmt.Errorf("remote delete requires a present expectation")
	}
	_, err := e.remotePKU.Command(ctx, "sync-delete", []string{expected.ID}, map[string]string{"expected-rev": expected.Rev})
	if err != nil {
		return fmt.Errorf("delete remote file %q: %w", expected.ID, err)
	}
	return nil
}

// MoveRemote moves or renames the exact object identity. Destination parent
// directories must already exist; the backend fails rather than implicitly
// creating them.
func (e *RootExecutor) MoveRemote(ctx context.Context, id, dstRelPath string, kind domain.EntryKind) (MoveResult, error) {
	if strings.TrimSpace(id) == "" {
		return MoveResult{}, fmt.Errorf("remote move requires an object ID")
	}
	if err := domain.ValidateRelPath(dstRelPath); err != nil {
		return MoveResult{}, err
	}
	if kind != domain.KindFile && kind != domain.KindDir {
		return MoveResult{}, fmt.Errorf("invalid move entry kind %q", kind)
	}

	result, err := e.remotePKU.Command(ctx, "sync-move", []string{id, dstRelPath}, map[string]string{"kind": string(kind)})
	if err != nil {
		return MoveResult{}, fmt.Errorf("move remote object %q: %w", id, err)
	}
	values, ok := result.(map[string]any)
	if !ok {
		return MoveResult{}, fmt.Errorf("sync-move returned unexpected result type %T", result)
	}
	newID, _ := values["id"].(string)
	remote, _ := values["remote"].(string)
	if newID == "" || remote == "" {
		return MoveResult{}, fmt.Errorf("sync-move returned incomplete result %#v", values)
	}
	return MoveResult{ID: newID, Remote: remote}, nil
}

// ObserveLocalFile returns the current file fingerprint, or an explicit absent
// fingerprint when the path is not a file.
func (e *RootExecutor) ObserveLocalFile(ctx context.Context, relPath string) (domain.LocalFingerprint, error) {
	if err := domain.ValidateRelPath(relPath); err != nil {
		return domain.LocalFingerprint{}, err
	}
	obj, err := e.local.NewObject(ctx, relPath)
	if errors.Is(err, fs.ErrorObjectNotFound) {
		return domain.LocalFingerprint{}, nil
	}
	if err != nil {
		return domain.LocalFingerprint{}, fmt.Errorf("stat local file %q: %w", relPath, err)
	}
	return domain.LocalFingerprint{
		Present: true,
		Kind:    domain.KindFile,
		Size:    obj.Size(),
		MtimeNS: obj.ModTime(ctx).UnixNano(),
	}, nil
}

// ObserveRemoteFile returns ID/revision metadata used by the planner's remote
// baseline and CAS expectations.
func (e *RootExecutor) ObserveRemoteFile(ctx context.Context, relPath string) (domain.RemoteFingerprint, error) {
	if err := domain.ValidateRelPath(relPath); err != nil {
		return domain.RemoteFingerprint{}, err
	}
	obj, err := e.remote.NewObject(ctx, relPath)
	if errors.Is(err, fs.ErrorObjectNotFound) {
		return domain.RemoteFingerprint{}, nil
	}
	if err != nil {
		return domain.RemoteFingerprint{}, fmt.Errorf("stat remote file %q: %w", relPath, err)
	}
	ider, ok := obj.(fs.IDer)
	if !ok || strings.TrimSpace(ider.ID()) == "" {
		return domain.RemoteFingerprint{}, fmt.Errorf("remote file %q does not expose an object ID", relPath)
	}
	metadataer, ok := obj.(fs.Metadataer)
	if !ok {
		return domain.RemoteFingerprint{}, fmt.Errorf("remote file %q does not expose revision metadata", relPath)
	}
	metadata, err := metadataer.Metadata(ctx)
	if err != nil {
		return domain.RemoteFingerprint{}, fmt.Errorf("read remote metadata %q: %w", relPath, err)
	}
	rev := strings.TrimSpace(metadata["rev"])
	if rev == "" {
		return domain.RemoteFingerprint{}, fmt.Errorf("remote file %q has no revision metadata", relPath)
	}
	return domain.RemoteFingerprint{
		Present: true,
		Kind:    domain.KindFile,
		ID:      ider.ID(),
		Rev:     rev,
		Size:    obj.Size(),
		MtimeUS: obj.ModTime(ctx).UnixMicro(),
	}, nil
}

func validateFilePathAndLocalExpectation(relPath string, expected domain.LocalFingerprint) error {
	if err := domain.ValidateRelPath(relPath); err != nil {
		return err
	}
	if err := expected.Validate(); err != nil {
		return fmt.Errorf("expected local fingerprint: %w", err)
	}
	if !expected.Present || expected.Kind != domain.KindFile {
		return fmt.Errorf("file upload requires a present local file expectation")
	}
	return nil
}

func validateFileRemoteExpectation(expected domain.RemoteExpectation) error {
	if err := expected.Validate(); err != nil {
		return fmt.Errorf("expected remote state: %w", err)
	}
	if !expected.Absent && strings.TrimSpace(expected.Rev) == "" {
		return fmt.Errorf("present remote file expectation requires a revision")
	}
	return nil
}
