package executor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

func TestScanLocalFollowRejectsParentContainingConfiguredPeerRoot(t *testing.T) {
	root := t.TempDir()
	shared := t.TempDir()
	peer := filepath.Join(shared, "peer-root")
	if err := os.Mkdir(peer, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(shared, "outside.txt"), []byte("outside"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(shared, filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	_, _, _, err := exec.ScanLocal(context.Background(), nil, []string{peer})
	if err == nil || !strings.Contains(err.Error(), "contains configured sync root") {
		t.Fatalf("followed parent containing configured peer root error = %v", err)
	}
}
