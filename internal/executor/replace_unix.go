//go:build !windows && !linux && !darwin

package executor

import "fmt"

func movePathNoReplace(src, dst string) error {
	return fmt.Errorf("atomic no-replace rename is unsupported on this platform")
}
