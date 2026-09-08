package executor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/rclone/rclone/fs"
	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
	"github.com/rijuyuezhu/pkudisk-sync/internal/rootmarker"
)

const tempNamePrefix = ".pkudisk-sync-tmp-"

// ScanLocal performs a complete ordinary-files/directories walk under the
// configured symlink policy. Excluded prefixes are intentionally outside local
// authority for this cycle and must not be interpreted as deletions.
func (e *RootExecutor) ScanLocal(ctx context.Context) (map[string]domain.LocalFingerprint, []string, error) {
	entries := make(map[string]domain.LocalFingerprint)
	var excluded []string
	rootInfo, err := os.Stat(e.root.LocalRoot)
	if err != nil {
		return nil, nil, fmt.Errorf("scan local root %q: stat root: %w", e.root.LocalRoot, err)
	}
	if !rootInfo.IsDir() {
		return nil, nil, fmt.Errorf("scan local root %q: root is not a directory", e.root.LocalRoot)
	}
	if err := e.scanLocalDir(ctx, e.root.LocalRoot, "", []os.FileInfo{rootInfo}, entries, &excluded); err != nil {
		return nil, nil, fmt.Errorf("scan local root %q: %w", e.root.LocalRoot, err)
	}
	return entries, excluded, nil
}

func (e *RootExecutor) scanLocalDir(ctx context.Context, physicalDir, relDir string, ancestors []os.FileInfo, out map[string]domain.LocalFingerprint, excluded *[]string) error {
	listed, err := os.ReadDir(physicalDir)
	if err != nil {
		return fmt.Errorf("read local directory %q: %w", relDir, err)
	}
	for _, entry := range listed {
		if err := ctx.Err(); err != nil {
			return err
		}
		rel := entry.Name()
		if relDir != "" {
			rel = path.Join(relDir, entry.Name())
		}
		if err := domain.ValidateRelPath(rel); err != nil {
			return fmt.Errorf("local path is not representable by the canonical sync namespace: %w", err)
		}
		if rel == rootmarker.FileName {
			if entry.IsDir() {
				return fmt.Errorf("reserved root marker path %q is a directory", rel)
			}
			continue
		}
		if isOperationTempName(entry.Name()) {
			continue
		}
		if isInternalTempName(entry.Name()) {
			return fmt.Errorf("reserved internal temp path %q remains in the sync root; a prior local mutation may need recovery", rel)
		}

		physicalPath := filepath.Join(physicalDir, entry.Name())
		info, err := os.Lstat(physicalPath)
		if err != nil {
			return fmt.Errorf("lstat local entry %q: %w", rel, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			switch e.root.EffectiveSymlinkMode() {
			case domain.SymlinkReject:
				return fmt.Errorf("symlink rejected by policy: %q", rel)
			case domain.SymlinkIgnore:
				*excluded = append(*excluded, rel)
				continue
			case domain.SymlinkFollow:
				resolved, present, resolveErr := resolveFollowedLocalPath(physicalPath, true)
				if resolveErr != nil {
					*excluded = append(*excluded, rel)
					continue
				}
				if !present {
					// A resolvable dangling final symlink is authoritative absence,
					// not an excluded namespace. This preserves delete/recreate
					// semantics while keeping the symlink object itself intact.
					continue
				}
				resolvedInfo, statErr := os.Stat(resolved)
				if statErr != nil {
					*excluded = append(*excluded, rel)
					continue
				}
				if resolvedInfo.IsDir() {
					if sameAsAny(resolvedInfo, ancestors) || strictPathAncestor(resolved, physicalDir) {
						// A link to this directory or any ancestor is a lexical cycle.
						// Excluding the alias, rather than descending, also handles
						// links to the sync root and links to a parent directory.
						*excluded = append(*excluded, rel)
						continue
					}
					out[rel] = domain.LocalFingerprint{Present: true, Kind: domain.KindDir}
					if err := e.scanLocalDir(ctx, resolved, rel, append(ancestors, resolvedInfo), out, excluded); err != nil {
						return err
					}
					continue
				}
				if resolvedInfo.Mode().IsRegular() {
					out[rel] = localFileFingerprint(resolvedInfo)
					continue
				}
				// Like the legacy copy-links path, a link to a socket/device/etc.
				// is outside the synchronized namespace rather than deletion evidence.
				*excluded = append(*excluded, rel)
				continue
			default:
				return fmt.Errorf("invalid symlink mode %q", e.root.EffectiveSymlinkMode())
			}
		}

		switch {
		case info.IsDir():
			if sameAsAny(info, ancestors) {
				*excluded = append(*excluded, rel)
				continue
			}
			out[rel] = domain.LocalFingerprint{Present: true, Kind: domain.KindDir}
			if err := e.scanLocalDir(ctx, physicalPath, rel, append(ancestors, info), out, excluded); err != nil {
				return err
			}
		case info.Mode().IsRegular():
			out[rel] = localFileFingerprint(info)
		default:
			return fmt.Errorf("unsupported local file type %q (%s)", rel, info.Mode().Type())
		}
	}
	return nil
}

func localFileFingerprint(info os.FileInfo) domain.LocalFingerprint {
	return domain.LocalFingerprint{Present: true, Kind: domain.KindFile, Size: info.Size(), MtimeNS: info.ModTime().UnixNano()}
}

func sameAsAny(info os.FileInfo, ancestors []os.FileInfo) bool {
	for _, ancestor := range ancestors {
		if os.SameFile(info, ancestor) {
			return true
		}
	}
	return false
}

func strictPathAncestor(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil || rel == "." || filepath.IsAbs(rel) {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
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
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func isOperationTempName(name string) bool {
	rest := strings.TrimPrefix(name, tempNamePrefix+"op-")
	if rest == name {
		return false
	}
	parts := strings.Split(rest, "-")
	if len(parts) != 2 || parts[0] == "" || (parts[1] != "download" && parts[1] != "recovery") {
		return false
	}
	for _, c := range parts[0] {
		if c < '0' || c > '9' {
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
