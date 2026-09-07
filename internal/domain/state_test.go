package domain

import (
	"path/filepath"
	"testing"
)

func TestSyncRootValidateCanonicalPaths(t *testing.T) {
	base := SyncRoot{
		UUID:                "root-1",
		LocalRoot:           filepath.Clean(filepath.Join(string(filepath.Separator), "tmp", "sync")),
		RemoteName:          "pkudisk",
		RemoteRoot:          "Personal/Sync",
		Enabled:             true,
		PollIntervalSeconds: 60,
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("valid sync root rejected: %v", err)
	}

	tests := []struct {
		name string
		edit func(*SyncRoot)
	}{
		{name: "relative local root", edit: func(r *SyncRoot) { r.LocalRoot = "relative/path" }},
		{name: "unclean local root", edit: func(r *SyncRoot) {
			r.LocalRoot = base.LocalRoot + string(filepath.Separator) + ".." + string(filepath.Separator) + "sync"
		}},
		{name: "absolute remote root", edit: func(r *SyncRoot) { r.RemoteRoot = "/Personal/Sync" }},
		{name: "unclean remote root", edit: func(r *SyncRoot) { r.RemoteRoot = "Personal/x/../Sync" }},
		{name: "escaping remote root", edit: func(r *SyncRoot) { r.RemoteRoot = "../Sync" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := base
			tt.edit(&root)
			if err := root.Validate(); err == nil {
				t.Fatalf("invalid sync root accepted: %+v", root)
			}
		})
	}
}
