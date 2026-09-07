package rootmarker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const FileName = ".pkudisk-sync-root"

// Ensure creates the root marker once, or validates the existing marker. It
// never replaces an unexpected path: a missing/rebound root must be an explicit
// user action rather than something a sync cycle silently repairs.
func Ensure(localRoot, uuid string) error {
	if err := validateInputs(localRoot, uuid); err != nil {
		return err
	}
	if err := Check(localRoot, uuid); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	marker := filepath.Join(localRoot, FileName)
	f, err := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return Check(localRoot, uuid)
		}
		return fmt.Errorf("create sync root marker: %w", err)
	}
	ok := false
	defer func() {
		_ = f.Close()
		if !ok {
			_ = os.Remove(marker)
		}
	}()
	if _, err := f.WriteString(uuid + "\n"); err != nil {
		return fmt.Errorf("write sync root marker: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync root marker: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close sync root marker: %w", err)
	}
	ok = true
	return nil
}

// Check proves that localRoot is the directory originally paired with uuid.
func Check(localRoot, uuid string) error {
	if err := validateInputs(localRoot, uuid); err != nil {
		return err
	}
	rootInfo, err := os.Stat(localRoot)
	if err != nil {
		return fmt.Errorf("stat sync root: %w", err)
	}
	if !rootInfo.IsDir() {
		return fmt.Errorf("sync root %q is not a directory", localRoot)
	}

	marker := filepath.Join(localRoot, FileName)
	info, err := os.Lstat(marker)
	if err != nil {
		return fmt.Errorf("stat sync root marker: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("sync root marker %q is not a regular file", marker)
	}
	contents, err := os.ReadFile(marker)
	if err != nil {
		return fmt.Errorf("read sync root marker: %w", err)
	}
	if strings.TrimSpace(string(contents)) != uuid {
		return fmt.Errorf("sync root marker UUID mismatch")
	}
	return nil
}

func validateInputs(localRoot, uuid string) error {
	if strings.TrimSpace(localRoot) == "" {
		return fmt.Errorf("local root must not be empty")
	}
	if strings.TrimSpace(uuid) == "" {
		return fmt.Errorf("sync root UUID must not be empty")
	}
	return nil
}
