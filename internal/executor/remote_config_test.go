package executor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigureRemoteRejectsWrongExistingBackendWithoutOAuth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rclone.conf")
	if err := os.WriteFile(path, []byte("[other]\ntype = local\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := ConfigureRemote(context.Background(), path, "other")
	if err == nil || !strings.Contains(err.Error(), "want pkudisk") {
		t.Fatalf("ConfigureRemote() error = %v", err)
	}
}
