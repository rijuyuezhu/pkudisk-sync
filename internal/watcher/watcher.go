package watcher

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
)

const eventBuffer = 1024

// Watcher turns platform filesystem notifications into coalesced root-level
// hints. Hints are never authoritative sync state: callers must run a complete
// reconciliation scan after receiving one.
type Watcher struct {
	root string
	fsw  *fsnotify.Watcher

	hints chan struct{}
	errs  chan error
	done  chan struct{}

	mu      sync.Mutex
	watched map[string]struct{}

	lifecycleMu sync.Mutex
	closing     bool
	addWG       sync.WaitGroup

	closeOnce sync.Once
	wg        sync.WaitGroup
}

// New recursively watches all directories currently below root. The event
// loop starts before the recursive walk, and every successful setup must still
// be followed by a complete scan; this closes the setup race without treating
// watcher delivery as durable truth.
func New(root string) (*Watcher, error) {
	if strings.TrimSpace(root) == "" {
		return nil, fmt.Errorf("watch root must not be empty")
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, fmt.Errorf("watch root %q must be an absolute canonical path", root)
	}
	info, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("stat watch root: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, fmt.Errorf("watch root %q must be a real directory", root)
	}

	fsw, err := fsnotify.NewBufferedWatcher(eventBuffer)
	if err != nil {
		return nil, fmt.Errorf("create filesystem watcher: %w", err)
	}
	w := &Watcher{
		root:    root,
		fsw:     fsw,
		hints:   make(chan struct{}, 1),
		errs:    make(chan error, 4),
		done:    make(chan struct{}),
		watched: make(map[string]struct{}),
	}
	w.wg.Add(1)
	go w.run()
	if err := w.addTree(root); err != nil {
		_ = w.Close()
		return nil, err
	}
	return w, nil
}

func (w *Watcher) Hints() <-chan struct{} { return w.hints }
func (w *Watcher) Errors() <-chan error   { return w.errs }
func (w *Watcher) Done() <-chan struct{}  { return w.done }

func (w *Watcher) Close() error {
	var err error
	w.closeOnce.Do(func() {
		w.lifecycleMu.Lock()
		w.closing = true
		w.lifecycleMu.Unlock()

		// Do not race fsnotify.Close with an in-flight Add. In particular, the
		// Windows backend has historically been sensitive to Add/Close overlap.
		// beginAdd is serialized with the closing transition, so after this wait
		// no new Add can reach the underlying watcher.
		w.addWG.Wait()
		err = w.fsw.Close()
		w.wg.Wait()
	})
	return err
}

func (w *Watcher) run() {
	defer w.wg.Done()
	defer close(w.hints)
	defer close(w.errs)
	defer close(w.done)

	for {
		select {
		case event, ok := <-w.fsw.Events:
			if !ok {
				return
			}
			w.handleEvent(event)
		case err, ok := <-w.fsw.Errors:
			if !ok {
				return
			}
			w.reportError(err)
			w.signal()
		}
	}
}

func (w *Watcher) handleEvent(event fsnotify.Event) {
	if event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) {
		w.dropTree(event.Name)
	}
	if event.Has(fsnotify.Create) {
		info, err := os.Lstat(event.Name)
		switch {
		case err == nil && info.Mode()&os.ModeSymlink == 0 && info.IsDir():
			if err := w.addTree(event.Name); err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, fsnotify.ErrClosed) {
				w.reportError(err)
			}
		case err != nil && !errors.Is(err, os.ErrNotExist):
			w.reportError(fmt.Errorf("stat created watch path %q: %w", event.Name, err))
		}
	}
	w.signal()
}

func (w *Watcher) addTree(root string) error {
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if !entry.IsDir() {
			return nil
		}
		return w.addDir(path)
	})
	if err != nil {
		return fmt.Errorf("install recursive watch under %q: %w", root, err)
	}
	return nil
}

func (w *Watcher) addDir(path string) error {
	if !w.beginAdd() {
		return fsnotify.ErrClosed
	}
	defer w.addWG.Done()

	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.watched[path]; ok {
		return nil
	}
	if err := w.fsw.Add(path); err != nil {
		return err
	}
	w.watched[path] = struct{}{}
	return nil
}

func (w *Watcher) beginAdd() bool {
	w.lifecycleMu.Lock()
	defer w.lifecycleMu.Unlock()
	if w.closing {
		return false
	}
	w.addWG.Add(1)
	return true
}

func (w *Watcher) dropTree(root string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	prefix := root + string(filepath.Separator)
	for path := range w.watched {
		if path == root || strings.HasPrefix(path, prefix) {
			delete(w.watched, path)
		}
	}
}

func (w *Watcher) signal() {
	select {
	case w.hints <- struct{}{}:
	default:
	}
}

func (w *Watcher) reportError(err error) {
	if err == nil {
		return
	}
	select {
	case w.errs <- err:
	default:
	}
}
