//go:build linux

package executor

import "golang.org/x/sys/unix"

func movePathNoReplace(src, dst string) error {
	return unix.Renameat2(unix.AT_FDCWD, src, unix.AT_FDCWD, dst, unix.RENAME_NOREPLACE)
}
