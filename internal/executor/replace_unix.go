//go:build !windows

package executor

import "os"

func replaceFile(src, dst string) error {
	return os.Rename(src, dst)
}
