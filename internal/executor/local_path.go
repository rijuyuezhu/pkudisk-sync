package executor

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// resolveFollowedLocalPath resolves symlinks while optionally allowing the
// final physical leaf to be absent. The scanner treats that state as excluded,
// not deletion evidence; callers that already have independent mutation
// authority can still use the resolved missing target to recreate it without
// replacing the symlink object itself.
func resolveFollowedLocalPath(name string, allowMissingLeaf bool) (resolved string, present bool, err error) {
	resolved, err = filepath.EvalSymlinks(name)
	if err == nil {
		return filepath.Clean(resolved), true, nil
	}
	if !allowMissingLeaf || !os.IsNotExist(err) {
		return "", false, err
	}
	return resolveMissingLocalLeaf(filepath.Clean(name), make(map[string]struct{}))
}

func resolveMissingLocalLeaf(name string, seen map[string]struct{}) (string, bool, error) {
	info, err := os.Lstat(name)
	if err == nil {
		if info.Mode()&os.ModeSymlink == 0 {
			resolved, evalErr := filepath.EvalSymlinks(name)
			if evalErr != nil {
				return "", false, evalErr
			}
			return filepath.Clean(resolved), true, nil
		}
		key, absErr := filepath.Abs(name)
		if absErr != nil {
			return "", false, absErr
		}
		key = filepath.Clean(key)
		if _, exists := seen[key]; exists {
			return "", false, fmt.Errorf("symlink cycle while resolving %q", name)
		}
		seen[key] = struct{}{}
		target, readErr := os.Readlink(name)
		if readErr != nil {
			return "", false, readErr
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(filepath.Dir(name), target)
		}
		return resolveMissingLocalLeaf(filepath.Clean(target), seen)
	}
	if !os.IsNotExist(err) {
		return "", false, err
	}
	parent := filepath.Dir(name)
	resolvedParent, evalErr := filepath.EvalSymlinks(parent)
	if evalErr != nil {
		return "", false, evalErr
	}
	return filepath.Join(filepath.Clean(resolvedParent), filepath.Base(name)), false, nil
}

func canonicalExistingLocalPath(name string) (string, error) {
	resolved, err := filepath.EvalSymlinks(filepath.Clean(name))
	if err != nil {
		return "", err
	}
	abs, err := filepath.Abs(resolved)
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}

func physicalPathContains(parent, child string) (bool, error) {
	canonicalParent, err := canonicalExistingLocalPath(parent)
	if err != nil {
		return false, err
	}
	canonicalChild, err := canonicalExistingLocalPath(child)
	if err != nil {
		return false, err
	}
	rel, err := filepath.Rel(canonicalParent, canonicalChild)
	if err != nil || filepath.IsAbs(rel) {
		// Different Windows volumes are a normal non-containment case. Both
		// inputs were already canonicalized successfully, so Rel failure here
		// cannot grant ownership and need not make an unrelated root unhealthy.
		return false, nil
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))), nil
}
