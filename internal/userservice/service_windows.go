//go:build windows

package userservice

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

type windowsTaskManager struct {
	executable string
	run        commandRunnerWindows
}

type commandRunnerWindows func(context.Context, string, ...string) ([]byte, error)

func newPlatformManager(executable string) (Manager, error) {
	if strings.Contains(executable, `"`) {
		return nil, fmt.Errorf("service executable contains an unsupported quote")
	}
	return &windowsTaskManager{executable: executable, run: runSchtasks}, nil
}

func (m *windowsTaskManager) Install(ctx context.Context) error {
	_, err := m.run(ctx, "schtasks.exe", scheduledTaskCreateArgs(m.executable)...)
	if err != nil {
		return fmt.Errorf("create per-user scheduled task: %w", err)
	}
	return nil
}

func (m *windowsTaskManager) Uninstall(ctx context.Context) error {
	installed, err := m.isInstalled(ctx)
	if err != nil {
		return err
	}
	if !installed {
		return nil
	}
	// /delete does not stop a currently running program, so end first. Ignore
	// an end failure because an installed task may simply be idle.
	_, _ = m.run(ctx, "schtasks.exe", "/end", "/tn", scheduledTaskName)
	if _, err := m.run(ctx, "schtasks.exe", "/delete", "/tn", scheduledTaskName, "/f"); err != nil {
		return fmt.Errorf("delete per-user scheduled task: %w", err)
	}
	return nil
}

func (m *windowsTaskManager) Start(ctx context.Context) error {
	if err := m.requireInstalled(ctx); err != nil {
		return err
	}
	if _, err := m.run(ctx, "schtasks.exe", "/run", "/tn", scheduledTaskName); err != nil {
		return fmt.Errorf("run per-user scheduled task: %w", err)
	}
	return nil
}

func (m *windowsTaskManager) Stop(ctx context.Context) error {
	if err := m.requireInstalled(ctx); err != nil {
		return err
	}
	if _, err := m.run(ctx, "schtasks.exe", "/end", "/tn", scheduledTaskName); err != nil {
		return fmt.Errorf("end per-user scheduled task: %w", err)
	}
	return nil
}

func (m *windowsTaskManager) Status(ctx context.Context) (Status, error) {
	output, err := m.run(ctx, "schtasks.exe", scheduledTaskQueryArgs()...)
	if err != nil {
		if scheduledTaskNotFound(err) {
			return StatusNotInstalled, nil
		}
		return "", fmt.Errorf("query per-user scheduled task: %w", err)
	}
	return parseScheduledTaskStatus(output), nil
}

func (m *windowsTaskManager) isInstalled(ctx context.Context) (bool, error) {
	_, err := m.run(ctx, "schtasks.exe", scheduledTaskQueryArgs()...)
	if err == nil {
		return true, nil
	}
	if scheduledTaskNotFound(err) {
		return false, nil
	}
	return false, fmt.Errorf("query per-user scheduled task: %w", err)
}

func scheduledTaskNotFound(err error) bool {
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return false
	}
	return isScheduledTaskNotFoundExitCode(uint32(exitErr.ExitCode()))
}

func (m *windowsTaskManager) requireInstalled(ctx context.Context) error {
	installed, err := m.isInstalled(ctx)
	if err != nil {
		return err
	}
	if !installed {
		return fmt.Errorf("pkudisk-sync user service is not installed")
	}
	return nil
}

func runSchtasks(ctx context.Context, name string, args ...string) ([]byte, error) {
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
