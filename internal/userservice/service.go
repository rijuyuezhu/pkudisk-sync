package userservice

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Status is the native per-user service manager's view of pkudisk-sync.
type Status string

const (
	StatusNotInstalled Status = "not-installed"
	StatusActive       Status = "active"
	StatusInactive     Status = "inactive"
)

// Manager controls the current user's persistent pkudisk-sync daemon.
type Manager interface {
	Install(context.Context) error
	Uninstall(context.Context) error
	Start(context.Context) error
	Stop(context.Context) error
	Status(context.Context) (Status, error)
}

// New builds the native per-user service manager for the current platform.
func New(executable string) (Manager, error) {
	if executable == "" {
		return nil, fmt.Errorf("service executable must not be empty")
	}
	if !filepath.IsAbs(executable) {
		return nil, fmt.Errorf("service executable must be absolute: %q", executable)
	}
	if strings.ContainsAny(executable, "\x00\r\n") {
		return nil, fmt.Errorf("service executable contains unsupported control characters")
	}
	info, err := os.Stat(executable)
	if err != nil {
		return nil, fmt.Errorf("stat service executable %q: %w", executable, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("service executable %q is not a regular file", executable)
	}
	manager, err := newPlatformManager(filepath.Clean(executable))
	if err != nil {
		return nil, fmt.Errorf("initialize %s per-user service manager: %w", runtime.GOOS, err)
	}
	return manager, nil
}
