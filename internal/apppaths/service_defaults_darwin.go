//go:build darwin

package apppaths

import (
	"fmt"
	"os/user"
	"path/filepath"
)

func servicePlatformDefaults() (Paths, error) {
	current, err := user.Current()
	if err != nil {
		return Paths{}, fmt.Errorf("resolve service user: %w", err)
	}
	home := current.HomeDir
	if home == "" {
		return Paths{}, fmt.Errorf("resolve service user home: empty home directory")
	}
	appSupport := filepath.Join(home, "Library", "Application Support")
	return Paths{
		StateDB:      filepath.Join(appSupport, appName, "state.db"),
		RcloneConfig: filepath.Join(appSupport, appName, "rclone.conf"),
		CacheDir:     filepath.Join(home, "Library", "Caches", appName),
		RuntimeDir:   filepath.Join(appSupport, appName),
	}, nil
}
