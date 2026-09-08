package watcher

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

func TestWatcherHintsExistingNestedWrite(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "a", "b")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	if err := os.WriteFile(filepath.Join(dir, "x.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitHint(t, w)
}

func TestWatcherAddsNewDirectoryBeforeCreateHint(t *testing.T) {
	root := t.TempDir()
	w, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	dir := filepath.Join(root, "new")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// handleEvent installs the directory watch before publishing this hint.
	waitHint(t, w)

	if err := os.WriteFile(filepath.Join(dir, "later.txt"), []byte("later"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitHint(t, w)
}

func TestWatcherRejectsSymlinkRoot(t *testing.T) {
	parent := t.TempDir()
	realRoot := filepath.Join(parent, "real")
	if err := os.Mkdir(realRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(realRoot, link); err != nil {
		t.Fatal(err)
	}
	if _, err := New(link); err == nil {
		t.Fatal("symlink watch root was accepted")
	}
}

func TestWatcherDoesNotFollowSymlinkTargetOutsideRoot(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(root, "linked")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	w, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.watched[root]; !ok {
		t.Fatal("real sync root is not watched")
	}
	if _, ok := w.watched[link]; ok {
		t.Fatal("symlink lexical path was installed as a recursive directory watch")
	}
	if _, ok := w.watched[outside]; ok {
		t.Fatal("watcher followed a symlink target outside the sync root")
	}
}

func TestCloseSignalsDone(t *testing.T) {
	w, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-w.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("watcher did not stop after Close")
	}
}

func TestCloseSerializesConcurrentDirectoryAdds(t *testing.T) {
	for iteration := 0; iteration < 50; iteration++ {
		root := t.TempDir()
		w, err := New(root)
		if err != nil {
			t.Fatal(err)
		}

		var creators sync.WaitGroup
		creatorErr := make(chan error, 1)
		creators.Add(1)
		go func() {
			defer creators.Done()
			for i := 0; i < 100; i++ {
				dir := filepath.Join(root, fmt.Sprintf("dir-%03d", i))
				if err := os.Mkdir(dir, 0o755); err != nil {
					select {
					case creatorErr <- err:
					default:
					}
					return
				}
				if err := os.WriteFile(filepath.Join(dir, "file.txt"), []byte("x"), 0o644); err != nil {
					select {
					case creatorErr <- err:
					default:
					}
					return
				}
			}
		}()

		closed := make(chan error, 1)
		go func() { closed <- w.Close() }()
		select {
		case err := <-closed:
			if err != nil {
				t.Fatalf("iteration %d: close watcher: %v", iteration, err)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("iteration %d: Close raced with directory Add and did not return", iteration)
		}
		creators.Wait()
		select {
		case err := <-creatorErr:
			t.Fatalf("iteration %d: create tree: %v", iteration, err)
		default:
		}
		if err := w.addDir(root); !errors.Is(err, fsnotify.ErrClosed) {
			t.Fatalf("iteration %d: add after close = %v, want fsnotify.ErrClosed", iteration, err)
		}
	}
}

func waitHint(t *testing.T, w *Watcher) {
	t.Helper()
	select {
	case _, ok := <-w.Hints():
		if !ok {
			t.Fatal("watcher closed before hint")
		}
	case err, ok := <-w.Errors():
		if ok {
			t.Fatalf("watcher error before hint: %v", err)
		}
		t.Fatal("watcher error channel closed before hint")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for watcher hint")
	}
}
