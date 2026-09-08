package executor

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/rclone/rclone/fs/config/configmap"
)

var rcloneConfigState struct {
	sync.Mutex
	installed bool
	path      string
}

// InstallRcloneConfig installs the process-global rclone config storage used by
// the embedded backend. pkudisk-sync is a single daemon and deliberately owns
// exactly one rclone config file, shared by all selected sync roots.
func InstallRcloneConfig(configPath string) error {
	if configPath == "" {
		return fmt.Errorf("rclone config path must not be empty")
	}
	abs, err := filepath.Abs(configPath)
	if err != nil {
		return fmt.Errorf("resolve rclone config path: %w", err)
	}

	rcloneConfigState.Lock()
	defer rcloneConfigState.Unlock()
	if rcloneConfigState.installed {
		if rcloneConfigState.path != abs {
			return fmt.Errorf("rclone config already installed at %q, cannot switch to %q in a running daemon", rcloneConfigState.path, abs)
		}
		return nil
	}
	if err := config.SetConfigPath(abs); err != nil {
		return fmt.Errorf("set rclone config path: %w", err)
	}
	configfile.Install()
	// Force the initial load now so configuration errors are reported at daemon
	// startup rather than during the first file transfer.
	_ = config.LoadedData()
	rcloneConfigState.installed = true
	rcloneConfigState.path = abs
	return nil
}

// RemoteConfig returns the live config mapper for one configured PKU Disk
// remote. Mapper Set calls persist OAuth token refreshes through rclone's config
// storage; selected roots sharing remoteName therefore share one auth authority.
func RemoteConfig(remoteName string) (configmap.Mapper, error) {
	if remoteName == "" {
		return nil, fmt.Errorf("remote name must not be empty")
	}
	if !rcloneConfigInstalled() {
		return nil, fmt.Errorf("rclone config storage is not installed")
	}
	backendType := config.GetValue(remoteName, "type")
	if backendType == "" {
		return nil, fmt.Errorf("rclone remote %q is not configured", remoteName)
	}
	if backendType != "pkudisk" {
		return nil, fmt.Errorf("rclone remote %q has type %q, want pkudisk", remoteName, backendType)
	}
	info, err := fs.Find("pkudisk")
	if err != nil {
		return nil, fmt.Errorf("find embedded pkudisk backend: %w", err)
	}
	return fs.ConfigMap(info.Prefix, info.Options, remoteName, nil), nil
}

// ConfigureRemote interactively creates or re-authenticates one PKU Disk
// remote in pkudisk-sync's app-owned rclone config. The OAuth/browser flow is
// driven by rclone and the embedded pkudisk backend directly; no rclone binary
// or subprocess is involved.
func ConfigureRemote(ctx context.Context, configPath, remoteName string) error {
	if remoteName == "" {
		return fmt.Errorf("remote name must not be empty")
	}
	if err := InstallRcloneConfig(configPath); err != nil {
		return err
	}

	backendType := config.GetValue(remoteName, "type")
	switch backendType {
	case "":
		if _, err := config.CreateRemote(ctx, remoteName, "pkudisk", nil, config.UpdateRemoteOpt{}); err != nil {
			return fmt.Errorf("configure PKU Disk remote %q: %w", remoteName, err)
		}
	case "pkudisk":
		if _, err := config.UpdateRemote(ctx, remoteName, nil, config.UpdateRemoteOpt{}); err != nil {
			return fmt.Errorf("reconfigure PKU Disk remote %q: %w", remoteName, err)
		}
	default:
		return fmt.Errorf("rclone remote %q has type %q, want pkudisk", remoteName, backendType)
	}
	return nil
}

func rcloneConfigInstalled() bool {
	rcloneConfigState.Lock()
	defer rcloneConfigState.Unlock()
	return rcloneConfigState.installed
}
