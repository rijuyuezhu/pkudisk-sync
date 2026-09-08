package syncer

import (
	"strings"
	"testing"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

func TestValidateSnapshotNamespaceAllowsExactCrossSidePath(t *testing.T) {
	local := map[string]domain.LocalFingerprint{"same.txt": {Present: true, Kind: domain.KindFile}}
	remote := map[string]domain.RemoteFingerprint{"same.txt": {Present: true, Kind: domain.KindFile, ID: "id", Rev: "rev"}}
	for _, target := range []string{"linux", "windows", "darwin"} {
		if err := validateSnapshotNamespace(local, remote, target); err != nil {
			t.Fatalf("target %s: %v", target, err)
		}
	}
}

func TestValidateSnapshotNamespaceWindowsCaseCollisionAcrossSides(t *testing.T) {
	local := map[string]domain.LocalFingerprint{"dir/Foo.txt": {Present: true, Kind: domain.KindFile}}
	remote := map[string]domain.RemoteFingerprint{"DIR/foo.txt": {Present: true, Kind: domain.KindFile, ID: "id", Rev: "rev"}}
	if err := validateSnapshotNamespace(local, remote, "windows"); err == nil || !strings.Contains(err.Error(), "namespace collision") {
		t.Fatalf("error = %v, want namespace collision", err)
	}
	if err := validateSnapshotNamespace(local, remote, "linux"); err != nil {
		t.Fatalf("linux should preserve distinct spellings: %v", err)
	}
}

func TestValidateSnapshotNamespaceWindowsRemoteCollision(t *testing.T) {
	remote := map[string]domain.RemoteFingerprint{
		"Foo.txt": {Present: true, Kind: domain.KindFile, ID: "a", Rev: "1"},
		"foo.txt": {Present: true, Kind: domain.KindFile, ID: "b", Rev: "1"},
	}
	if err := validateSnapshotNamespace(nil, remote, "windows"); err == nil {
		t.Fatal("case-colliding remote paths were accepted on Windows")
	}
}

func TestValidateSnapshotNamespaceDarwinNormalizationCollision(t *testing.T) {
	remote := map[string]domain.RemoteFingerprint{
		"caf\u00e9.txt":  {Present: true, Kind: domain.KindFile, ID: "a", Rev: "1"},
		"cafe\u0301.txt": {Present: true, Kind: domain.KindFile, ID: "b", Rev: "1"},
	}
	if err := validateSnapshotNamespace(nil, remote, "darwin"); err == nil || !strings.Contains(err.Error(), "namespace collision") {
		t.Fatalf("error = %v, want Unicode normalization collision", err)
	}
	if err := validateSnapshotNamespace(nil, remote, "linux"); err != nil {
		t.Fatalf("linux should preserve canonically distinct spellings: %v", err)
	}
}

func TestValidateSnapshotNamespaceDarwinCaseCollision(t *testing.T) {
	remote := map[string]domain.RemoteFingerprint{
		"Alpha": {Present: true, Kind: domain.KindDir, ID: "a"},
		"alpha": {Present: true, Kind: domain.KindDir, ID: "b"},
	}
	if err := validateSnapshotNamespace(nil, remote, "darwin"); err == nil {
		t.Fatal("case-colliding remote paths were accepted on macOS")
	}
}

func TestLocalNamespaceKeyRejectsWindowsUnrepresentableNames(t *testing.T) {
	invalid := []string{
		"CON",
		"con.txt",
		"NUL.data",
		"CLOCK$",
		"COM1",
		"COM1 .txt",
		"LPT9.log",
		"COM¹.txt",
		"dir/trailing.",
		"dir/trailing ",
		"bad:name",
		"bad\x01name",
	}
	for _, rel := range invalid {
		t.Run(rel, func(t *testing.T) {
			if _, err := localNamespaceKey(rel, "windows"); err == nil {
				t.Fatalf("%q was accepted", rel)
			}
		})
	}

	for _, rel := range []string{"COM10", "nulled.txt", "normal.name", "中文.txt"} {
		if _, err := localNamespaceKey(rel, "windows"); err != nil {
			t.Fatalf("valid Windows path %q rejected: %v", rel, err)
		}
	}
}

func TestLocalNamespaceKeyRejectsNULAndNonCanonicalPaths(t *testing.T) {
	for _, target := range []string{"linux", "windows", "darwin"} {
		if _, err := localNamespaceKey("bad\x00name", target); err == nil {
			t.Fatalf("target %s accepted NUL", target)
		}
		if _, err := localNamespaceKey("bad\xffname", target); err == nil {
			t.Fatalf("target %s accepted invalid UTF-8", target)
		}
	}
	for _, rel := range []string{"../escape", "a//b", `a\\b`} {
		if _, err := localNamespaceKey(rel, "linux"); err == nil {
			t.Fatalf("non-canonical path %q accepted", rel)
		}
	}
	if _, err := localNamespaceKey("valid", "plan9"); err == nil {
		t.Fatal("unsupported target platform was accepted")
	}
}
