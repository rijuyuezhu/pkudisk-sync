//go:build windows

package apppaths

import (
	"fmt"
	"path/filepath"

	"golang.org/x/sys/windows"
)

func servicePlatformDefaults() (Paths, error) {
	localAppData, err := windows.KnownFolderPath(
		windows.FOLDERID_LocalAppData,
		windows.KF_FLAG_DEFAULT|windows.KF_FLAG_DONT_VERIFY,
	)
	if err != nil {
		return Paths{}, fmt.Errorf("resolve service LocalAppData known folder: %w", err)
	}
	if localAppData == "" {
		return Paths{}, fmt.Errorf("resolve service LocalAppData known folder: empty path")
	}
	return Paths{
		StateDB:      filepath.Join(localAppData, appName, "state.db"),
		RcloneConfig: filepath.Join(localAppData, appName, "rclone.conf"),
		CacheDir:     filepath.Join(localAppData, "cache", appName),
		RuntimeDir:   filepath.Join(localAppData, appName),
	}, nil
}
