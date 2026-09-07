package apppaths

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/adrg/xdg"
)

const appName = "pkudisk-sync"

const (
	envStateDB      = "PKUDISK_SYNC_STATE_DB"
	envRcloneConfig = "PKUDISK_SYNC_RCLONE_CONFIG"
	envCacheDir     = "PKUDISK_SYNC_CACHE_DIR"
	envRuntimeDir   = "PKUDISK_SYNC_RUNTIME_DIR"
)

// OverridesActive reports whether this process is using explicit path
// overrides that a login-managed background service would not inherit.
func OverridesActive() bool {
	for _, name := range []string{envStateDB, envRcloneConfig, envCacheDir, envRuntimeDir} {
		if os.Getenv(name) != "" {
			return true
		}
	}
	return false
}

// Paths are all per-user paths owned by pkudisk-sync. The embedded rclone
// backend deliberately does not use the user's global rclone configuration.
type Paths struct {
	StateDB      string
	RcloneConfig string
	CacheDir     string
	RuntimeDir   string
}

// Default returns platform-appropriate per-user paths, with explicit
// environment overrides for packaging, tests, and advanced deployments.
func Default() (Paths, error) {
	runtimeDir := filepath.Join(xdg.RuntimeDir, appName)
	if xdg.RuntimeDir == "" {
		runtimeDir = filepath.Join(xdg.StateHome, appName, "runtime")
	}
	p := Paths{
		StateDB:      filepath.Join(xdg.StateHome, appName, "state.db"),
		RcloneConfig: filepath.Join(xdg.ConfigHome, appName, "rclone.conf"),
		CacheDir:     filepath.Join(xdg.CacheHome, appName),
		RuntimeDir:   runtimeDir,
	}
	if value := os.Getenv(envStateDB); value != "" {
		p.StateDB = value
	}
	if value := os.Getenv(envRcloneConfig); value != "" {
		p.RcloneConfig = value
	}
	if value := os.Getenv(envCacheDir); value != "" {
		p.CacheDir = value
	}
	if value := os.Getenv(envRuntimeDir); value != "" {
		p.RuntimeDir = value
	}
	return p.absolute()
}

func (p Paths) absolute() (Paths, error) {
	values := []*string{&p.StateDB, &p.RcloneConfig, &p.CacheDir, &p.RuntimeDir}
	for _, value := range values {
		if *value == "" {
			return Paths{}, fmt.Errorf("pkudisk-sync path must not be empty")
		}
		abs, err := filepath.Abs(*value)
		if err != nil {
			return Paths{}, fmt.Errorf("resolve pkudisk-sync path %q: %w", *value, err)
		}
		*value = filepath.Clean(abs)
	}
	return p, nil
}

// PrepareState creates the private application directory containing state.db.
func (p Paths) PrepareState() error {
	return ensurePrivateDir(filepath.Dir(p.StateDB))
}

// PrepareConfig creates the application directory containing the embedded
// rclone config. It does not create or populate rclone.conf itself.
func (p Paths) PrepareConfig() error {
	return ensurePrivateDir(filepath.Dir(p.RcloneConfig))
}

// PrepareRuntime creates the per-user runtime directory used for process-level
// coordination such as the daemon single-instance lock.
func (p Paths) PrepareRuntime() error {
	return ensurePrivateDir(p.RuntimeDir)
}

func ensurePrivateDir(dir string) error {
	// MkdirAll applies 0700 only to components it creates. Never chmod an
	// already-existing parent: explicit path overrides may intentionally live
	// below a shared directory such as /tmp or a packaging-managed location.
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create application directory %q: %w", dir, err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("stat application directory %q: %w", dir, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("application path %q must be a real directory", dir)
	}
	return nil
}
