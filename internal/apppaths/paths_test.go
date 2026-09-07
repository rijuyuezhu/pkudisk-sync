package apppaths

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultHonorsExplicitOverrides(t *testing.T) {
	base := t.TempDir()
	want := Paths{
		StateDB:      filepath.Join(base, "state", "custom.db"),
		RcloneConfig: filepath.Join(base, "config", "rclone.conf"),
		CacheDir:     filepath.Join(base, "cache"),
		RuntimeDir:   filepath.Join(base, "runtime"),
	}
	t.Setenv(envStateDB, want.StateDB)
	t.Setenv(envRcloneConfig, want.RcloneConfig)
	t.Setenv(envCacheDir, want.CacheDir)
	t.Setenv(envRuntimeDir, want.RuntimeDir)

	got, err := Default()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("Default() = %+v, want %+v", got, want)
	}
}

func TestPrepareCreatesRealPrivateDirectories(t *testing.T) {
	base := t.TempDir()
	paths := Paths{
		StateDB:      filepath.Join(base, "state", "state.db"),
		RcloneConfig: filepath.Join(base, "config", "rclone.conf"),
		CacheDir:     filepath.Join(base, "cache"),
		RuntimeDir:   filepath.Join(base, "runtime"),
	}
	if err := paths.PrepareState(); err != nil {
		t.Fatal(err)
	}
	if err := paths.PrepareConfig(); err != nil {
		t.Fatal(err)
	}
	if err := paths.PrepareRuntime(); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{filepath.Dir(paths.StateDB), filepath.Dir(paths.RcloneConfig), paths.RuntimeDir} {
		info, err := os.Lstat(dir)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			t.Fatalf("prepared path %q is not a real directory", dir)
		}
	}
}

func TestPrepareRejectsSymlinkApplicationDirectory(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "state")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	paths := Paths{StateDB: filepath.Join(link, "state.db")}
	if err := paths.PrepareState(); err == nil {
		t.Fatal("symlink application directory was accepted")
	}
}
