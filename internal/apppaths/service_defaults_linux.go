//go:build linux

package apppaths

import (
	"fmt"
	"os"
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
	return Paths{
		StateDB:      filepath.Join(home, ".local", "state", appName, "state.db"),
		RcloneConfig: filepath.Join(home, ".config", appName, "rclone.conf"),
		CacheDir:     filepath.Join(home, ".cache", appName),
		RuntimeDir:   filepath.Join("/run", "user", fmt.Sprintf("%d", os.Getuid()), appName),
	}, nil
}
