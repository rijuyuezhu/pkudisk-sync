//go:build linux

package userservice

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/adrg/xdg"
)

const systemdUnitName = "pkudisk-sync.service"

type commandRunner func(context.Context, string, ...string) ([]byte, error)

type systemdUserManager struct {
	executable string
	unitPath   string
	run        commandRunner
}

func newPlatformManager(executable string) (Manager, error) {
	if xdg.ConfigHome == "" {
		return nil, fmt.Errorf("XDG config home is unavailable")
	}
	return &systemdUserManager{
		executable: executable,
		unitPath:   filepath.Join(xdg.ConfigHome, "systemd", "user", systemdUnitName),
		run:        runCommand,
	}, nil
}

func (m *systemdUserManager) Install(ctx context.Context) error {
	unit, err := renderSystemdUnit(m.executable)
	if err != nil {
		return err
	}
	if err := writeServiceFile(m.unitPath, []byte(unit)); err != nil {
		return err
	}
	if _, err := m.run(ctx, "systemctl", "--user", "daemon-reload"); err != nil {
		return fmt.Errorf("reload systemd user manager: %w", err)
	}
	if _, err := m.run(ctx, "systemctl", "--user", "enable", systemdUnitName); err != nil {
		return fmt.Errorf("enable systemd user service: %w", err)
	}
	return nil
}

func (m *systemdUserManager) Uninstall(ctx context.Context) error {
	installed, err := regularFileExists(m.unitPath)
	if err != nil {
		return err
	}
	if !installed {
		return nil
	}
	if _, err := m.run(ctx, "systemctl", "--user", "disable", "--now", systemdUnitName); err != nil {
		return fmt.Errorf("disable systemd user service: %w", err)
	}
	if err := os.Remove(m.unitPath); err != nil {
		return fmt.Errorf("remove systemd user unit %q: %w", m.unitPath, err)
	}
	if _, err := m.run(ctx, "systemctl", "--user", "daemon-reload"); err != nil {
		return fmt.Errorf("reload systemd user manager after uninstall: %w", err)
	}
	return nil
}

func (m *systemdUserManager) Start(ctx context.Context) error {
	if err := m.requireInstalled(); err != nil {
		return err
	}
	if _, err := m.run(ctx, "systemctl", "--user", "start", systemdUnitName); err != nil {
		return fmt.Errorf("start systemd user service: %w", err)
	}
	return nil
}

func (m *systemdUserManager) Stop(ctx context.Context) error {
	if err := m.requireInstalled(); err != nil {
		return err
	}
	if _, err := m.run(ctx, "systemctl", "--user", "stop", systemdUnitName); err != nil {
		return fmt.Errorf("stop systemd user service: %w", err)
	}
	return nil
}

func (m *systemdUserManager) Status(ctx context.Context) (Status, error) {
	installed, err := regularFileExists(m.unitPath)
	if err != nil {
		return "", err
	}
	if !installed {
		return StatusNotInstalled, nil
	}
	output, runErr := m.run(ctx, "systemctl", "--user", "is-active", systemdUnitName)
	state := strings.TrimSpace(string(output))
	if state != "" {
		if state == string(StatusActive) {
			return StatusActive, nil
		}
		return Status(state), nil
	}
	if runErr != nil {
		return "", fmt.Errorf("query systemd user service: %w", runErr)
	}
	return StatusInactive, nil
}

func (m *systemdUserManager) requireInstalled() error {
	installed, err := regularFileExists(m.unitPath)
	if err != nil {
		return err
	}
	if !installed {
		return fmt.Errorf("pkudisk-sync user service is not installed")
	}
	return nil
}

func renderSystemdUnit(executable string) (string, error) {
	quoted, err := quoteSystemdArgument(executable)
	if err != nil {
		return "", err
	}
	return "[Unit]\n" +
		"Description=PKU Disk bidirectional sync\n\n" +
		"[Service]\n" +
		"Type=simple\n" +
		"ExecStart=" + quoted + " daemon\n" +
		"Restart=on-failure\n" +
		"RestartSec=5s\n" +
		"UMask=0077\n\n" +
		"[Install]\n" +
		"WantedBy=default.target\n", nil
}

func quoteSystemdArgument(value string) (string, error) {
	if value == "" || strings.ContainsAny(value, "\x00\r\n") {
		return "", fmt.Errorf("invalid systemd command argument")
	}
	value = strings.ReplaceAll(value, "%", "%%")
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\"", "\\\"")
	return "\"" + value + "\"", nil
}

func writeServiceFile(path string, contents []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create service directory %q: %w", dir, err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("stat service directory %q: %w", dir, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("service directory %q must be a real directory", dir)
	}
	tmp, err := os.CreateTemp(dir, ".pkudisk-sync-service-*")
	if err != nil {
		return fmt.Errorf("create temporary service file: %w", err)
	}
	tmpPath := tmp.Name()
	keep := false
	defer func() {
		_ = tmp.Close()
		if !keep {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return fmt.Errorf("secure temporary service file: %w", err)
	}
	if _, err := tmp.Write(contents); err != nil {
		return fmt.Errorf("write temporary service file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temporary service file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary service file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("install service file %q: %w", path, err)
	}
	keep = true
	return nil
}

func regularFileExists(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat service file %q: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return false, fmt.Errorf("service file %q is not a regular file", path)
	}
	return true, nil
}

func runCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if message != "" {
			return output, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, message)
		}
		return output, fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return output, nil
}
