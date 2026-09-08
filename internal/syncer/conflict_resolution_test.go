package syncer

import (
	"testing"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

func TestOperationForConflictResolutionUsesExactConflictFingerprints(t *testing.T) {
	local := domain.LocalFingerprint{Present: true, Kind: domain.KindFile, Size: 12, MtimeNS: 123}
	remote := domain.RemoteFingerprint{Present: true, Kind: domain.KindFile, ID: "doc", Rev: "rev-b", Size: 13}
	conflict := domain.Conflict{
		ID:         7,
		SyncRootID: 3,
		RelPath:    "docs/conflict.txt",
		Kind:       domain.ConflictBothModified,
		Local:      local,
		Remote:     remote,
	}

	keepLocal, err := OperationForConflictResolution(conflict, ConflictKeepLocal)
	if err != nil {
		t.Fatal(err)
	}
	if keepLocal.Kind != domain.OperationEnsureRemote || keepLocal.EntryKind != domain.KindFile || keepLocal.SrcPath != conflict.RelPath {
		t.Fatalf("keep-local operation = %+v", keepLocal)
	}
	if !domain.LocalEquivalent(keepLocal.ExpectedLocal, local) || keepLocal.ExpectedRemote.Absent || keepLocal.ExpectedRemote.ID != "doc" || keepLocal.ExpectedRemote.Rev != "rev-b" {
		t.Fatalf("keep-local preconditions = %+v", keepLocal)
	}

	keepRemote, err := OperationForConflictResolution(conflict, ConflictKeepRemote)
	if err != nil {
		t.Fatal(err)
	}
	if keepRemote.Kind != domain.OperationEnsureLocal || keepRemote.EntryKind != domain.KindFile {
		t.Fatalf("keep-remote operation = %+v", keepRemote)
	}
	if !domain.LocalEquivalent(keepRemote.ExpectedLocal, local) || keepRemote.ExpectedRemote.ID != "doc" || keepRemote.ExpectedRemote.Rev != "rev-b" {
		t.Fatalf("keep-remote preconditions = %+v", keepRemote)
	}
}

func TestOperationForConflictResolutionMapsDeleteEditConflicts(t *testing.T) {
	remoteFile := domain.RemoteFingerprint{Present: true, Kind: domain.KindFile, ID: "doc-1", Rev: "rev-2", Size: 8}
	localDeleted := domain.Conflict{
		ID:         1,
		SyncRootID: 1,
		RelPath:    "gone.txt",
		Kind:       domain.ConflictLocalDeleteRemoteEdit,
		Remote:     remoteFile,
	}
	keepLocal, err := OperationForConflictResolution(localDeleted, ConflictKeepLocal)
	if err != nil {
		t.Fatal(err)
	}
	if keepLocal.Kind != domain.OperationDeleteRemote || keepLocal.EntryKind != domain.KindFile || keepLocal.ExpectedRemote.ID != "doc-1" || keepLocal.ExpectedRemote.Rev != "rev-2" {
		t.Fatalf("local-delete keep-local = %+v", keepLocal)
	}
	keepRemote, err := OperationForConflictResolution(localDeleted, ConflictKeepRemote)
	if err != nil {
		t.Fatal(err)
	}
	if keepRemote.Kind != domain.OperationEnsureLocal || keepRemote.EntryKind != domain.KindFile || keepRemote.ExpectedLocal.Present {
		t.Fatalf("local-delete keep-remote = %+v", keepRemote)
	}

	localFile := domain.LocalFingerprint{Present: true, Kind: domain.KindFile, Size: 4, MtimeNS: 40}
	remoteDeleted := domain.Conflict{
		ID:         2,
		SyncRootID: 1,
		RelPath:    "local.txt",
		Kind:       domain.ConflictRemoteDeleteLocalEdit,
		Local:      localFile,
	}
	keepRemote, err = OperationForConflictResolution(remoteDeleted, ConflictKeepRemote)
	if err != nil {
		t.Fatal(err)
	}
	if keepRemote.Kind != domain.OperationDeleteLocal || !keepRemote.ExpectedRemote.Absent {
		t.Fatalf("remote-delete keep-remote = %+v", keepRemote)
	}
	keepLocal, err = OperationForConflictResolution(remoteDeleted, ConflictKeepLocal)
	if err != nil {
		t.Fatal(err)
	}
	if keepLocal.Kind != domain.OperationEnsureRemote || !keepLocal.ExpectedRemote.Absent {
		t.Fatalf("remote-delete keep-local = %+v", keepLocal)
	}
}

func TestOperationForConflictResolutionRejectsDirectorySubtreeChoices(t *testing.T) {
	conflict := domain.Conflict{
		ID:         3,
		SyncRootID: 1,
		RelPath:    "docs",
		Kind:       domain.ConflictLocalDeleteRemoteEdit,
		Remote:     domain.RemoteFingerprint{Present: true, Kind: domain.KindDir, ID: "dir-1"},
	}
	for _, resolution := range []ConflictResolution{ConflictKeepLocal, ConflictKeepRemote} {
		if _, err := OperationForConflictResolution(conflict, resolution); err == nil {
			t.Fatalf("directory conflict accepted for %s", resolution)
		}
	}
}

func TestOperationForConflictResolutionRejectsKindMismatch(t *testing.T) {
	conflict := domain.Conflict{
		ID:         9,
		SyncRootID: 1,
		RelPath:    "mixed",
		Kind:       domain.ConflictKindMismatch,
		Local:      domain.LocalFingerprint{Present: true, Kind: domain.KindFile, Size: 1, MtimeNS: 1},
		Remote:     domain.RemoteFingerprint{Present: true, Kind: domain.KindDir, ID: "dir"},
	}
	for _, resolution := range []ConflictResolution{ConflictKeepLocal, ConflictKeepRemote} {
		if _, err := OperationForConflictResolution(conflict, resolution); err == nil {
			t.Fatalf("kind mismatch accepted for %s", resolution)
		}
	}
}
