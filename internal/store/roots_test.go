package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

func TestCreateSyncRootRejectsOverlappingOwnership(t *testing.T) {
	ctx := context.Background()
	localBase := filepath.Join(t.TempDir(), "roots")

	tests := []struct {
		name      string
		existing  domain.SyncRoot
		candidate domain.SyncRoot
		wantErr   bool
	}{
		{
			name:      "sibling roots are independent",
			existing:  testSyncRoot("one", filepath.Join(localBase, "Data"), "pkudisk", "Personal/Data"),
			candidate: testSyncRoot("two", filepath.Join(localBase, "Work"), "pkudisk", "Personal/Work"),
		},
		{
			name:      "local child overlaps",
			existing:  testSyncRoot("one", filepath.Join(localBase, "Data"), "pkudisk", "Personal/Data"),
			candidate: testSyncRoot("two", filepath.Join(localBase, "Data", "sub"), "pkudisk", "Personal/Elsewhere"),
			wantErr:   true,
		},
		{
			name:      "local parent overlaps",
			existing:  testSyncRoot("one", filepath.Join(localBase, "Data", "sub"), "pkudisk", "Personal/Data"),
			candidate: testSyncRoot("two", filepath.Join(localBase, "Data"), "pkudisk", "Personal/Elsewhere"),
			wantErr:   true,
		},
		{
			name:      "remote child overlaps on same remote",
			existing:  testSyncRoot("one", filepath.Join(localBase, "Data"), "pkudisk", "Personal/Sync"),
			candidate: testSyncRoot("two", filepath.Join(localBase, "Work"), "pkudisk", "Personal/Sync/sub"),
			wantErr:   true,
		},
		{
			name:      "remote parent overlaps on same remote",
			existing:  testSyncRoot("one", filepath.Join(localBase, "Data"), "pkudisk", "Personal/Sync/sub"),
			candidate: testSyncRoot("two", filepath.Join(localBase, "Work"), "pkudisk", "Personal/Sync"),
			wantErr:   true,
		},
		{
			name:      "same remote path under different remote config is independent",
			existing:  testSyncRoot("one", filepath.Join(localBase, "Data"), "pkudisk-a", "Personal/Sync"),
			candidate: testSyncRoot("two", filepath.Join(localBase, "Work"), "pkudisk-b", "Personal/Sync"),
		},
		{
			name:      "disabled root still owns its namespace",
			existing:  testSyncRoot("one", filepath.Join(localBase, "Data"), "pkudisk", "Personal/Sync"),
			candidate: testSyncRoot("two", filepath.Join(localBase, "Work"), "pkudisk", "Personal/Sync/sub"),
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestStore(t)
			tt.existing.Enabled = false
			if _, err := s.CreateSyncRoot(ctx, tt.existing); err != nil {
				t.Fatalf("create existing root: %v", err)
			}
			_, err := s.CreateSyncRoot(ctx, tt.candidate)
			if tt.wantErr && err == nil {
				t.Fatal("expected overlapping sync root to be rejected")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("independent sync root rejected: %v", err)
			}
		})
	}
}

func TestSetSyncRootEnabledPreservesSelection(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	root := createTestRoot(t, s)

	if err := s.SetSyncRootEnabled(ctx, root.ID, false); err != nil {
		t.Fatal(err)
	}
	paused, ok, err := s.GetSyncRoot(ctx, root.ID)
	if err != nil || !ok {
		t.Fatalf("get paused root = %+v, %v, %v", paused, ok, err)
	}
	if paused.Enabled {
		t.Fatal("root remained enabled after pause")
	}

	if err := s.SetSyncRootEnabled(ctx, root.ID, true); err != nil {
		t.Fatal(err)
	}
	resumed, ok, err := s.GetSyncRoot(ctx, root.ID)
	if err != nil || !ok {
		t.Fatalf("get resumed root = %+v, %v, %v", resumed, ok, err)
	}
	if !resumed.Enabled {
		t.Fatal("root remained disabled after resume")
	}
	if resumed.LocalRoot != root.LocalRoot || resumed.RemoteRoot != root.RemoteRoot {
		t.Fatalf("pause/resume changed root selection: got %+v want %+v", resumed, root)
	}
}

func testSyncRoot(uuid, localRoot, remoteName, remoteRoot string) domain.SyncRoot {
	return domain.SyncRoot{
		UUID:                uuid,
		LocalRoot:           localRoot,
		RemoteName:          remoteName,
		RemoteRoot:          remoteRoot,
		Enabled:             true,
		PollIntervalSeconds: 60,
	}
}
