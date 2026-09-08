//go:build !linux && !darwin && !windows

package apppaths

func servicePlatformDefaults() (Paths, error) {
	return platformDefaults(), nil
}
