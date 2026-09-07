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
