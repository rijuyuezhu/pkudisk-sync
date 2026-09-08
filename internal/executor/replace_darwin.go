//go:build darwin

package executor

import "golang.org/x/sys/unix"

func movePathNoReplace(src, dst string) error {
	return unix.RenameatxNp(unix.AT_FDCWD, src, unix.AT_FDCWD, dst, unix.RENAME_EXCL)
}
