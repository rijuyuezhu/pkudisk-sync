package daemonlock

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestAcquireIsExclusiveAndReusable(t *testing.T) {
	runtimeDir := t.TempDir()
	first, err := Acquire(runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	if _, err := Acquire(runtimeDir); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second Acquire() error = %v, want ErrAlreadyRunning", err)
	}

	contents, err := os.ReadFile(filepath.Join(runtimeDir, fileName))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(contents)) != strconv.Itoa(os.Getpid()) {
		t.Fatalf("lock PID = %q, want %d", strings.TrimSpace(string(contents)), os.Getpid())
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}

	third, err := Acquire(runtimeDir)
	if err != nil {
		t.Fatalf("Acquire() after release = %v", err)
	}
	if err := third.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireRejectsMissingOrSymlinkRuntimeDir(t *testing.T) {
	base := t.TempDir()
	missing := filepath.Join(base, "missing")
	if _, err := Acquire(missing); err == nil {
		t.Fatal("missing runtime directory was accepted")
	}

	realDir := filepath.Join(base, "real")
	if err := os.Mkdir(realDir, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(realDir, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := Acquire(link); err == nil {
		t.Fatal("symlink runtime directory was accepted")
	}
}
