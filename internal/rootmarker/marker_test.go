package rootmarker

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureAndCheck(t *testing.T) {
	root := t.TempDir()
	if err := Ensure(root, "root-uuid"); err != nil {
		t.Fatal(err)
	}
	if err := Check(root, "root-uuid"); err != nil {
		t.Fatal(err)
	}
	if err := Ensure(root, "root-uuid"); err != nil {
		t.Fatalf("idempotent Ensure() failed: %v", err)
	}
	if err := Check(root, "other-uuid"); err == nil {
		t.Fatal("marker UUID mismatch accepted")
	}
}

func TestRemoveOnlyDeletesMatchingMarker(t *testing.T) {
	root := t.TempDir()
	if err := Ensure(root, "root-uuid"); err != nil {
		t.Fatal(err)
	}
	if err := Remove(root, "other-uuid"); err == nil {
		t.Fatal("Remove accepted mismatched marker UUID")
	}
	if err := Check(root, "root-uuid"); err != nil {
		t.Fatalf("mismatched Remove changed marker: %v", err)
	}
	if err := Remove(root, "root-uuid"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(root, FileName)); !os.IsNotExist(err) {
		t.Fatalf("matching marker remains after Remove: %v", err)
	}
}

func TestEnsureRefusesUnexpectedReservedPath(t *testing.T) {
	root := t.TempDir()
	marker := filepath.Join(root, FileName)
	if err := os.WriteFile(marker, []byte("someone-else\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Ensure(root, "root-uuid"); err == nil {
		t.Fatal("unexpected pre-existing marker was overwritten or accepted")
	}
	contents, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "someone-else\n" {
		t.Fatalf("unexpected marker contents changed: %q", contents)
	}
}

func TestEnsureRejectsSymlinkRoot(t *testing.T) {
	parent := t.TempDir()
	realRoot := filepath.Join(parent, "real")
	if err := os.Mkdir(realRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(realRoot, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if err := Ensure(link, "root-uuid"); err == nil {
		t.Fatal("symlink sync root was accepted")
	}
	if _, err := os.Lstat(filepath.Join(realRoot, FileName)); !os.IsNotExist(err) {
		t.Fatalf("rejected symlink root created marker in target: %v", err)
	}
}

func TestCheckRejectsMarkerSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.WriteFile(target, []byte("root-uuid\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, FileName)); err != nil {
		t.Fatal(err)
	}
	if err := Check(root, "root-uuid"); err == nil {
		t.Fatal("symlink marker accepted")
	}
}
