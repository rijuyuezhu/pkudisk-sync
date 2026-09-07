package executor

import (
	"context"

	"github.com/rclone/rclone/fs"
)

const (
	syncExpectedIDMetadataKey     = "pkudisk-sync-expected-id"
	syncExpectedRevMetadataKey    = "pkudisk-sync-expected-rev"
	syncExpectedAbsentMetadataKey = "pkudisk-sync-expected-absent"
	syncExpectedIDDownloadHeader  = "X-PKUDisk-Sync-Expected-ID"
	syncExpectedRevDownloadHeader = "X-PKUDisk-Sync-Expected-Rev"
)

// safeCopyContext creates the only copy-policy context used by the sync
// executor. The daemon does not expose rclone CLI flags, but these fields are
// still set explicitly so sync correctness cannot accidentally depend on a
// future global/default configuration change.
func safeCopyContext(parent context.Context) (context.Context, *fs.ConfigInfo) {
	ctx, ci := fs.AddConfig(parent)
	ci.IgnoreTimes = true
	ci.IgnoreExisting = false
	ci.UpdateOlder = false
	ci.DryRun = false
	ci.Interactive = false
	ci.NoCheckDest = false
	ci.CompareDest = nil
	ci.CopyDest = nil
	ci.BackupDir = ""
	ci.Suffix = ""
	ci.Inplace = true
	ci.PartialSuffix = ""
	ci.MetadataSet = nil
	ci.DownloadHeaders = nil
	return ctx, ci
}

func uploadContext(parent context.Context, expectedID, expectedRev string, expectedAbsent bool) context.Context {
	ctx, ci := safeCopyContext(parent)
	ci.MetadataSet = fs.Metadata{}
	if expectedAbsent {
		ci.MetadataSet[syncExpectedAbsentMetadataKey] = "true"
	} else {
		ci.MetadataSet[syncExpectedIDMetadataKey] = expectedID
		ci.MetadataSet[syncExpectedRevMetadataKey] = expectedRev
	}
	return ctx
}

func downloadContext(parent context.Context, expectedID, expectedRev string) context.Context {
	ctx, ci := safeCopyContext(parent)
	// rclone's multi-thread source path does not propagate DownloadHeaders into
	// each Object.Open call. One stream is therefore a correctness invariant for
	// conditional PKU Disk downloads, not a tuning preference.
	ci.MultiThreadStreams = 1
	ci.MultiThreadSet = true
	ci.DownloadHeaders = []*fs.HTTPOption{
		{Key: syncExpectedIDDownloadHeader, Value: expectedID},
		{Key: syncExpectedRevDownloadHeader, Value: expectedRev},
	}
	return ctx
}
