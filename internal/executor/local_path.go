package executor

import (
	"fmt"
	"os"
	"path/filepath"
)

// resolveFollowedLocalPath resolves symlinks while optionally allowing the
// final physical leaf to be absent. That absent-leaf case is what lets a
// dangling final symlink remain an authoritative virtual deletion and later be
// recreated without replacing the symlink object itself.
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
