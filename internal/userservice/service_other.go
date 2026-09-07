//go:build !linux && !darwin && !windows

package userservice

import (
	"fmt"
	"runtime"
)

func newPlatformManager(string) (Manager, error) {
	return nil, fmt.Errorf("per-user service management is not implemented on %s yet", runtime.GOOS)
}
