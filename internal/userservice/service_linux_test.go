//go:build linux

package userservice

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestRenderSystemdUnitEscapesExecutablePath(t *testing.T) {
	unit, err := renderSystemdUnit(`/opt/PKU Disk/$sync%/pkudisk-sync`)
	if err != nil {
		t.Fatal(err)
	}
	want := `ExecStart="/opt/PKU Disk/$sync%%/pkudisk-sync" daemon`
	if !strings.Contains(unit, want) {
		t.Fatalf("unit missing %q:\n%s", want, unit)
	}
	for _, required := range []string{
		"Restart=on-failure",
		"RestartSec=5s",
		"UMask=0077",
		"WantedBy=default.target",
	} {
		if !strings.Contains(unit, required) {
			t.Fatalf("unit missing %q:\n%s", required, unit)
		}
	}
}

func TestSystemdUserManagerInstallStartStopStatusAndUninstall(t *testing.T) {
	ctx := context.Background()
	unitPath := filepath.Join(t.TempDir(), "systemd", "user", systemdUnitName)
	var calls []string
	run := func(_ context.Context, name string, args ...string) ([]byte, error) {
		calls = append(calls, strings.Join(append([]string{name}, args...), " "))
		if len(args) >= 2 && args[1] == "is-active" {
			return []byte("active\n"), nil
		}
		return nil, nil
	}
	manager := &systemdUserManager{
		executable: "/opt/pkudisk-sync",
		unitPath:   unitPath,
		run:        run,
	}

	status, err := manager.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status != StatusNotInstalled {
		t.Fatalf("Status() before install = %q", status)
	}
	if err := manager.Install(ctx); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(unitPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), `ExecStart="/opt/pkudisk-sync" daemon`) {
		t.Fatalf("installed unit:\n%s", contents)
	}
	if err := manager.Start(ctx); err != nil {
		t.Fatal(err)
	}
	status, err = manager.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status != StatusActive {
		t.Fatalf("Status() = %q", status)
	}
	if err := manager.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := manager.Uninstall(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(unitPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unit remains after uninstall: %v", err)
	}

	wantCalls := []string{
		"systemctl --user daemon-reload",
		"systemctl --user enable " + systemdUnitName,
		"systemctl --user start " + systemdUnitName,
		"systemctl --user is-active " + systemdUnitName,
		"systemctl --user stop " + systemdUnitName,
		"systemctl --user disable --now " + systemdUnitName,
		"systemctl --user daemon-reload",
	}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("systemctl calls = %#v, want %#v", calls, wantCalls)
	}
}

func TestSystemdStatusKeepsNativeInactiveState(t *testing.T) {
	unitPath := filepath.Join(t.TempDir(), systemdUnitName)
	if err := os.WriteFile(unitPath, []byte("[Unit]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	manager := &systemdUserManager{
		unitPath: unitPath,
		run: func(context.Context, string, ...string) ([]byte, error) {
			return []byte("inactive\n"), errors.New("exit status 3")
		},
	}
	status, err := manager.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status != StatusInactive {
		t.Fatalf("Status() = %q", status)
	}
}
