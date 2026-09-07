package executor

import (
	"context"
	"testing"

	"github.com/rclone/rclone/fs"
)

func TestSafeCopyContextNeutralizesInheritedPolicy(t *testing.T) {
	parent, inherited := fs.AddConfig(context.Background())
	inherited.IgnoreExisting = true
	inherited.UpdateOlder = true
	inherited.DryRun = true
	inherited.Interactive = true
	inherited.NoCheckDest = true
	inherited.CompareDest = []string{"compare:"}
	inherited.CopyDest = []string{"copy:"}
	inherited.BackupDir = "backup:"
	inherited.Suffix = ".bak"
	inherited.Inplace = false
	inherited.PartialSuffix = ".partial"
	inherited.MetadataSet = fs.Metadata{"bad": "metadata"}
	inherited.DownloadHeaders = []*fs.HTTPOption{{Key: "X-Bad", Value: "header"}}

	ctx, got := safeCopyContext(parent)
	if fs.GetConfig(parent) != inherited {
		t.Fatal("test setup lost inherited config")
	}
	if fs.GetConfig(ctx) != got {
		t.Fatal("safe context did not install its own config")
	}
	if !got.IgnoreTimes || got.IgnoreExisting || got.UpdateOlder || got.DryRun || got.Interactive || got.NoCheckDest {
		t.Fatalf("unsafe scalar copy policy survived: %+v", got)
	}
	if len(got.CompareDest) != 0 || len(got.CopyDest) != 0 || got.BackupDir != "" || got.Suffix != "" {
		t.Fatalf("unsafe destination policy survived: %+v", got)
	}
	if !got.Inplace || got.PartialSuffix != "" {
		t.Fatalf("partial staging was not disabled: inplace=%v suffix=%q", got.Inplace, got.PartialSuffix)
	}
	if got.MetadataSet != nil || got.DownloadHeaders != nil {
		t.Fatalf("operation-specific controls leaked from parent: metadata=%v headers=%v", got.MetadataSet, got.DownloadHeaders)
	}

	// The parent must remain untouched. Per-operation safety must not mutate a
	// shared daemon context that other roots may use concurrently.
	if !inherited.IgnoreExisting || inherited.Suffix != ".bak" || inherited.BackupDir != "backup:" || !inherited.DryRun {
		t.Fatalf("safe copy context mutated parent: %+v", inherited)
	}
}

func TestUploadContextCarriesExactlyOneRemoteExpectation(t *testing.T) {
	existingCtx := uploadContext(context.Background(), "doc-1", "rev-1", false)
	existing := fs.GetConfig(existingCtx).MetadataSet
	if len(existing) != 2 || existing[syncExpectedIDMetadataKey] != "doc-1" || existing[syncExpectedRevMetadataKey] != "rev-1" {
		t.Fatalf("existing upload metadata = %#v", existing)
	}
	if _, ok := existing[syncExpectedAbsentMetadataKey]; ok {
		t.Fatalf("existing upload unexpectedly carried expected-absent: %#v", existing)
	}

	absentCtx := uploadContext(context.Background(), "ignored", "ignored", true)
	absent := fs.GetConfig(absentCtx).MetadataSet
	if len(absent) != 1 || absent[syncExpectedAbsentMetadataKey] != "true" {
		t.Fatalf("absent upload metadata = %#v", absent)
	}
}

func TestDownloadContextPinsRevisionAndDisablesMultithread(t *testing.T) {
	ctx := downloadContext(context.Background(), "doc-1", "rev-1")
	ci := fs.GetConfig(ctx)
	if ci.MultiThreadStreams != 1 || !ci.MultiThreadSet {
		t.Fatalf("conditional download multi-thread config = streams=%d set=%v", ci.MultiThreadStreams, ci.MultiThreadSet)
	}
	if len(ci.DownloadHeaders) != 2 {
		t.Fatalf("download headers = %#v", ci.DownloadHeaders)
	}
	got := map[string]string{}
	for _, header := range ci.DownloadHeaders {
		got[header.Key] = header.Value
	}
	if got[syncExpectedIDDownloadHeader] != "doc-1" || got[syncExpectedRevDownloadHeader] != "rev-1" {
		t.Fatalf("download precondition headers = %#v", got)
	}
}
func TestEmbeddedPKUDiskBackendHasSyncCommands(t *testing.T) {
	info, err := fs.Find("pkudisk")
	if err != nil {
		t.Fatal(err)
	}
	commands := make(map[string]bool, len(info.CommandHelp))
	for _, command := range info.CommandHelp {
		commands[command.Name] = true
	}
	for _, required := range []string{"sync-delete", "sync-delete-dir", "sync-move"} {
		if !commands[required] {
			t.Fatalf("embedded pkudisk backend is missing required command %q", required)
		}
	}
}
