//go:build linux || darwin

package executor

import (
	"fmt"
	"os"
	"syscall"
)

func physicalObjectIdentity(_ string, info os.FileInfo) (string, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return "", fmt.Errorf("filesystem does not expose a stable device/inode identity")
	}
	return fmt.Sprintf("%d:%d", stat.Dev, stat.Ino), nil
}
