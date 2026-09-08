//go:build darwin

package userservice

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
)

type launchctlManager struct {
	executable string
	plistPath  string
	domain     string
	target     string
	run        commandRunnerDarwin
}

type commandRunnerDarwin func(context.Context, string, ...string) ([]byte, error)

func newPlatformManager(executable string) (Manager, error) {
	current, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("resolve service user: %w", err)
	}
	home := current.HomeDir
	if home == "" {
		return nil, fmt.Errorf("resolve service user home: empty home directory")
	}
	domain := "gui/" + strconv.Itoa(os.Getuid())
	return &launchctlManager{
		executable: executable,
		plistPath:  filepath.Join(home, "Library", "LaunchAgents", launchAgentLabel+".plist"),
		domain:     domain,
		target:     domain + "/" + launchAgentLabel,
		run:        runLaunchctl,
	}, nil
}

func (m *launchctlManager) Install(context.Context) error {
	plist, err := renderLaunchAgentPlist(m.executable)
	if err != nil {
		return err
	}
	return writeDarwinServiceFile(m.plistPath, []byte(plist))
}

func (m *launchctlManager) Uninstall(ctx context.Context) error {
	installed, err := darwinRegularFileExists(m.plistPath)
	if err != nil {
		return err
	}
	if !installed {
		return nil
	}
	if _, err := m.run(ctx, "launchctl", "print", m.target); err == nil {
		if _, err := m.run(ctx, "launchctl", "bootout", m.target); err != nil {
			return fmt.Errorf("boot out launch agent: %w", err)
		}
	}
	if err := os.Remove(m.plistPath); err != nil {
		return fmt.Errorf("remove launch agent %q: %w", m.plistPath, err)
	}
	return nil
}

func (m *launchctlManager) Start(ctx context.Context) error {
	if err := m.requireInstalled(); err != nil {
		return err
	}
	// launchd caches ProgramArguments when a plist is bootstrapped. Reinstalling
	// the plist from a new executable path is therefore not enough by itself:
	// unload any existing inactive job so bootstrap below always reads the
	// current on-disk definition. The CLI daemon-lease preflight prevents this
	// path from racing a live sync process.
	if _, err := m.run(ctx, "launchctl", "print", m.target); err == nil {
		if _, err := m.run(ctx, "launchctl", "bootout", m.target); err != nil {
			return fmt.Errorf("boot out launch agent before start: %w", err)
		}
	}
	if _, err := m.run(ctx, "launchctl", "bootstrap", m.domain, m.plistPath); err != nil {
		return fmt.Errorf("bootstrap launch agent: %w", err)
	}
	return nil
}

func (m *launchctlManager) Stop(ctx context.Context) error {
	if err := m.requireInstalled(); err != nil {
		return err
	}
	if _, err := m.run(ctx, "launchctl", "print", m.target); err != nil {
		return nil
	}
	if _, err := m.run(ctx, "launchctl", "bootout", m.target); err != nil {
		return fmt.Errorf("boot out launch agent: %w", err)
	}
	return nil
}

func (m *launchctlManager) Status(ctx context.Context) (Status, error) {
	installed, err := darwinRegularFileExists(m.plistPath)
	if err != nil {
		return "", err
	}
	if !installed {
		return StatusNotInstalled, nil
	}
	output, runErr := m.run(ctx, "launchctl", "print", m.target)
	if runErr == nil {
		return parseLaunchctlStatus(output), nil
	}
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		return StatusInactive, nil
	}
	return "", fmt.Errorf("query launch agent: %w", runErr)
}

func (m *launchctlManager) requireInstalled() error {
	installed, err := darwinRegularFileExists(m.plistPath)
	if err != nil {
		return err
	}
	if !installed {
		return fmt.Errorf("pkudisk-sync user service is not installed")
	}
	return nil
}

func writeDarwinServiceFile(path string, contents []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create LaunchAgents directory %q: %w", dir, err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("stat LaunchAgents directory %q: %w", dir, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("LaunchAgents directory %q must be a real directory", dir)
	}
	tmp, err := os.CreateTemp(dir, ".pkudisk-sync-launchagent-*")
	if err != nil {
		return fmt.Errorf("create temporary launch agent: %w", err)
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
		return fmt.Errorf("secure temporary launch agent: %w", err)
	}
	if _, err := tmp.Write(contents); err != nil {
		return fmt.Errorf("write temporary launch agent: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temporary launch agent: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary launch agent: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("install launch agent %q: %w", path, err)
	}
	keep = true
	return nil
}

func darwinRegularFileExists(path string) (bool, error) {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat launch agent %q: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return false, fmt.Errorf("launch agent %q is not a regular file", path)
	}
	return true, nil
}

func runLaunchctl(ctx context.Context, name string, args ...string) ([]byte, error) {
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
