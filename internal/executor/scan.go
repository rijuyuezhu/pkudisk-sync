package executor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/rclone/rclone/fs"
	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
	"github.com/rijuyuezhu/pkudisk-sync/internal/rootmarker"
)

const tempNamePrefix = ".pkudisk-sync-tmp-"

// ScanLocal performs a complete ordinary-files/directories walk. Symlinks and
// special files fail the scan instead of being omitted, because omission from a
// complete snapshot has deletion semantics after initial pairing.
func (e *RootExecutor) ScanLocal(ctx context.Context) (map[string]domain.LocalFingerprint, error) {
	entries := make(map[string]domain.LocalFingerprint)
	err := filepath.WalkDir(e.root.LocalRoot, func(fullPath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if fullPath == e.root.LocalRoot {
			return nil
		}

		relOS, err := filepath.Rel(e.root.LocalRoot, fullPath)
		if err != nil {
			return fmt.Errorf("resolve local relative path %q: %w", fullPath, err)
		}
		rel := filepath.ToSlash(relOS)
		if err := domain.ValidateRelPath(rel); err != nil {
			return fmt.Errorf("local path is not representable by the canonical sync namespace: %w", err)
		}
		if rel == rootmarker.FileName {
			if entry.IsDir() {
				return fmt.Errorf("reserved root marker path %q is a directory", rel)
			}
			return nil
		}
		if isInternalTempName(entry.Name()) {
			return fmt.Errorf("reserved internal temp path %q remains in the sync root; a prior local mutation may need recovery", rel)
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsupported symlink in sync root: %q", rel)
		}

		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("stat local entry %q: %w", rel, err)
		}
		switch {
		case info.IsDir():
			entries[rel] = domain.LocalFingerprint{Present: true, Kind: domain.KindDir}
		case info.Mode().IsRegular():
			entries[rel] = domain.LocalFingerprint{
				Present: true,
				Kind:    domain.KindFile,
				Size:    info.Size(),
				MtimeNS: info.ModTime().UnixNano(),
			}
		default:
			return fmt.Errorf("unsupported local file type %q (%s)", rel, info.Mode().Type())
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan local root %q: %w", e.root.LocalRoot, err)
	}
	return entries, nil
}

// ScanRemote recursively lists the configured PKU Disk root using the backend's
// normal List implementation. No ListR/sub_objects dependency is required.
// rootPresent distinguishes a genuinely empty root from a selected root that
// does not exist yet; callers may tolerate the latter only during initial
// non-destructive pairing. Missing children inside an existing root remain
// scan errors because omission from a complete snapshot has deletion semantics.
func (e *RootExecutor) ScanRemote(ctx context.Context) (entries map[string]domain.RemoteFingerprint, rootPresent bool, err error) {
	entries = make(map[string]domain.RemoteFingerprint)
	rootPresent, err = e.scanRemoteDir(ctx, "", entries)
	if err != nil {
		return nil, rootPresent, err
	}
	return entries, rootPresent, nil
}

func (e *RootExecutor) scanRemoteDir(ctx context.Context, dir string, out map[string]domain.RemoteFingerprint) (bool, error) {
	listed, err := e.remote.List(ctx, dir)
	if errors.Is(err, fs.ErrorDirNotFound) {
		if dir == "" {
			return false, nil
		}
		return true, fmt.Errorf("remote directory %q disappeared during complete scan: %w", dir, err)
	}
	if err != nil {
		return false, fmt.Errorf("list remote directory %q: %w", dir, err)
	}
	for _, entry := range listed {
		if err := ctx.Err(); err != nil {
			return true, err
		}
		rel := entry.Remote()
		if err := domain.ValidateRelPath(rel); err != nil {
			return true, fmt.Errorf("remote listing returned invalid path: %w", err)
		}
		base := filepath.Base(filepath.FromSlash(rel))
		if rel == rootmarker.FileName || isInternalTempName(base) {
			return true, fmt.Errorf("remote sync root contains reserved internal path %q", rel)
		}

		switch typed := entry.(type) {
		case fs.Object:
			fingerprint, err := remoteFileFingerprint(ctx, typed)
			if err != nil {
				return true, fmt.Errorf("remote file %q: %w", rel, err)
			}
			out[rel] = fingerprint
		case fs.Directory:
			id := strings.TrimSpace(typed.ID())
			if id == "" {
				return true, fmt.Errorf("remote directory %q has no object ID", rel)
			}
			out[rel] = domain.RemoteFingerprint{
				Present: true,
				Kind:    domain.KindDir,
				ID:      id,
				MtimeUS: typed.ModTime(ctx).UnixMicro(),
			}
			if _, err := e.scanRemoteDir(ctx, rel, out); err != nil {
				return true, err
			}
		default:
			return true, fmt.Errorf("remote path %q has unsupported listing type %T", rel, entry)
		}
	}
	return true, nil
}

func isInternalTempName(name string) bool {
	if !strings.HasPrefix(name, tempNamePrefix) || len(name) != len(tempNamePrefix)+24 {
		return false
	}
	for _, c := range name[len(tempNamePrefix):] {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func remoteFileFingerprint(ctx context.Context, obj fs.Object) (domain.RemoteFingerprint, error) {
	ider, ok := obj.(fs.IDer)
	if !ok || strings.TrimSpace(ider.ID()) == "" {
		return domain.RemoteFingerprint{}, fmt.Errorf("object does not expose an ID")
	}
	metadataer, ok := obj.(fs.Metadataer)
	if !ok {
		return domain.RemoteFingerprint{}, fmt.Errorf("object does not expose revision metadata")
	}
	metadata, err := metadataer.Metadata(ctx)
	if err != nil {
		return domain.RemoteFingerprint{}, fmt.Errorf("read metadata: %w", err)
	}
	rev := strings.TrimSpace(metadata["rev"])
	if rev == "" {
		return domain.RemoteFingerprint{}, fmt.Errorf("object has no revision metadata")
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
