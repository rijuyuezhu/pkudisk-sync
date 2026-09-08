//go:build !linux && !darwin && !windows

package executor

import (
	"os"
	"path/filepath"
)

func physicalObjectIdentity(path string, _ os.FileInfo) (string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	return filepath.Clean(resolved), nil
}
