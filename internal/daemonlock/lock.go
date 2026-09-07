package daemonlock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

const fileName = "daemon.lock"

// ErrAlreadyRunning means another pkudisk-sync daemon owns the per-user lock.
var ErrAlreadyRunning = errors.New("pkudisk-sync daemon is already running")

// Lock owns the process-level daemon lease until Close is called or the process
// exits. The lock file itself is persistent and only carries diagnostic PID
// text; ownership is provided by the OS file lock, not file existence.
type Lock struct {
	file   *os.File
	closed bool
}

// Acquire obtains the non-blocking daemon lease in runtimeDir.
func Acquire(runtimeDir string) (*Lock, error) {
	if runtimeDir == "" {
		return nil, fmt.Errorf("runtime directory must not be empty")
	}
	info, err := os.Lstat(runtimeDir)
	if err != nil {
		return nil, fmt.Errorf("stat runtime directory %q: %w", runtimeDir, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, fmt.Errorf("runtime directory %q must be a real directory", runtimeDir)
	}

	path := filepath.Join(runtimeDir, fileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open daemon lock %q: %w", path, err)
	}
	if err := lockFile(file); err != nil {
		_ = file.Close()
		if isLockConflict(err) {
			return nil, ErrAlreadyRunning
		}
		return nil, fmt.Errorf("lock daemon file %q: %w", path, err)
	}

	lock := &Lock{file: file}
	if err := writePID(file); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("write daemon lock PID: %w", err)
	}
	return lock, nil
}

func writePID(file *os.File) error {
	if err := file.Truncate(0); err != nil {
		return err
	}
	if _, err := file.Seek(0, 0); err != nil {
		return err
	}
	if _, err := file.WriteString(strconv.Itoa(os.Getpid()) + "\n"); err != nil {
		return err
	}
	return file.Sync()
}

// Close releases the daemon lease. It is safe to call more than once.
func (l *Lock) Close() error {
	if l == nil || l.closed {
		return nil
	}
	l.closed = true
	unlockErr := unlockFile(l.file)
	closeErr := l.file.Close()
	return errors.Join(unlockErr, closeErr)
}
