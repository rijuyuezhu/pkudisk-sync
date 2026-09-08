//go:build !linux && !darwin && !windows

package executor

import (
	"fmt"
	"os"
	"runtime"
)

func physicalObjectIdentity(_ string, _ os.FileInfo) (string, error) {
	return "", fmt.Errorf("stable physical filesystem identity is unsupported on %s", runtime.GOOS)
}
