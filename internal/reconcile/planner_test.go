package reconcile

import (
	"testing"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

func TestPlanInitial(t *testing.T) {
	localFile := lf(10, 100)
	remoteFile := rf("id-a", "rev-a", 10)

	tests := []struct {
		name     string
		local    domain.LocalFingerprint
		remote   domain.RemoteFingerprint
		content  domain.ContentEvidence
		wantKind domain.DecisionKind
		wantConf domain.ConflictKind
	}{
		{name: "both absent", wantKind: domain.DecisionNoop},
		{name: "local only file", local: localFile, wantKind: domain.DecisionEnsureRemote},
		{name: "remote only file", remote: remoteFile, wantKind: domain.DecisionEnsureLocal},
		{name: "same size needs content proof", local: localFile, remote: remoteFile, wantKind: domain.DecisionCompareContent},
		{name: "same content establishes baseline", local: localFile, remote: remoteFile, content: domain.ContentEqual, wantKind: domain.DecisionCommitBaseline},
		{name: "different content conflicts", local: localFile, remote: remoteFile, content: domain.ContentDifferent, wantKind: domain.DecisionConflict, wantConf: domain.ConflictSimultaneousCreate},
		{name: "different size conflicts", local: localFile, remote: rf("id-a", "rev-a", 11), wantKind: domain.DecisionConflict, wantConf: domain.ConflictSimultaneousCreate},
		{name: "matching directories merge", local: ldir(), remote: rdir("dir-a"), wantKind: domain.DecisionCommitBaseline},
		{name: "local only directory", local: ldir(), wantKind: domain.DecisionEnsureRemote},
		{name: "remote only directory", remote: rdir("dir-a"), wantKind: domain.DecisionEnsureLocal},
		{name: "kind mismatch conflicts", local: localFile, remote: rdir("dir-a"), wantKind: domain.DecisionConflict, wantConf: domain.ConflictKindMismatch},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PlanInitial("docs/a", tt.local, tt.remote, tt.content)
			if err != nil {
				t.Fatalf("PlanInitial() error = %v", err)
			}
			if got.Kind != tt.wantKind {
				t.Fatalf("kind = %q, want %q; decision=%+v", got.Kind, tt.wantKind, got)
			}
			if got.Conflict != tt.wantConf {
				t.Fatalf("conflict = %q, want %q; decision=%+v", got.Conflict, tt.wantConf, got)
			}
		})
	}
}

func TestPlanInitialCarriesCreateAndDownloadPreconditions(t *testing.T) {
	local := lf(5, 10)
	create, err := PlanInitial("new.txt", local, domain.RemoteFingerprint{}, domain.ContentUnknown)
	if err != nil {
		t.Fatal(err)
	}
	if create.Kind != domain.DecisionEnsureRemote || !create.ExpectedRemote.Absent {
		t.Fatalf("create decision does not require remote absence: %+v", create)
	}
	if create.ExpectedLocal != local {
		t.Fatalf("create local expectation = %+v, want %+v", create.ExpectedLocal, local)
	}

	remote := rf("doc-1", "rev-7", 5)
	download, err := PlanInitial("remote.txt", domain.LocalFingerprint{}, remote, domain.ContentUnknown)
	if err != nil {
		t.Fatal(err)
	}
	if download.Kind != domain.DecisionEnsureLocal {
		t.Fatalf("download kind = %q", download.Kind)
	}
	if download.ExpectedRemote.Absent || download.ExpectedRemote.ID != "doc-1" || download.ExpectedRemote.Rev != "rev-7" {
		t.Fatalf("download remote expectation = %+v", download.ExpectedRemote)
	}
	if download.ExpectedLocal.Present {
		t.Fatalf("download should expect local absence: %+v", download.ExpectedLocal)
	}
}

func TestPlanThreeWayNormalMatrix(t *testing.T) {
	baseLocal := lf(10, 100)
	baseRemote := rf("doc-1", "rev-1", 10)
	base := baseline(baseLocal, baseRemote)

	tests := []struct {
		name     string
		local    domain.LocalFingerprint
		remote   domain.RemoteFingerprint
		content  domain.ContentEvidence
		wantKind domain.DecisionKind
		wantConf domain.ConflictKind
	}{
		{name: "unchanged", local: baseLocal, remote: baseRemote, wantKind: domain.DecisionNoop},
		{name: "local edit", local: lf(12, 200), remote: baseRemote, wantKind: domain.DecisionEnsureRemote},
		{name: "remote edit", local: baseLocal, remote: rf("doc-1", "rev-2", 12), wantKind: domain.DecisionEnsureLocal},
		{name: "local delete", remote: baseRemote, wantKind: domain.DecisionDeleteRemote},
		{name: "remote delete", local: baseLocal, wantKind: domain.DecisionDeleteLocal},
		{name: "both delete", wantKind: domain.DecisionDropBaseline},
		{name: "both edit different sizes", local: lf(12, 200), remote: rf("doc-1", "rev-2", 13), wantKind: domain.DecisionConflict, wantConf: domain.ConflictBothModified},
		{name: "both edit same size needs comparison", local: lf(12, 200), remote: rf("doc-1", "rev-2", 12), wantKind: domain.DecisionCompareContent},
		{name: "both edit converge", local: lf(12, 200), remote: rf("doc-1", "rev-2", 12), content: domain.ContentEqual, wantKind: domain.DecisionCommitBaseline},
		{name: "both edit differ", local: lf(12, 200), remote: rf("doc-1", "rev-2", 12), content: domain.ContentDifferent, wantKind: domain.DecisionConflict, wantConf: domain.ConflictBothModified},
		{name: "local delete remote edit", remote: rf("doc-1", "rev-2", 12), wantKind: domain.DecisionConflict, wantConf: domain.ConflictLocalDeleteRemoteEdit},
		{name: "remote delete local edit", local: lf(12, 200), wantKind: domain.DecisionConflict, wantConf: domain.ConflictRemoteDeleteLocalEdit},
		{name: "current kind mismatch", local: ldir(), remote: baseRemote, wantKind: domain.DecisionConflict, wantConf: domain.ConflictKindMismatch},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PlanThreeWay(base, tt.local, tt.remote, tt.content)
			if err != nil {
				t.Fatalf("PlanThreeWay() error = %v", err)
			}
			if got.Kind != tt.wantKind {
				t.Fatalf("kind = %q, want %q; decision=%+v", got.Kind, tt.wantKind, got)
			}
			if got.Conflict != tt.wantConf {
				t.Fatalf("conflict = %q, want %q; decision=%+v", got.Conflict, tt.wantConf, got)
			}
		})
	}
}

func TestPlanThreeWayCreateFromAbsentBaseline(t *testing.T) {
	base := baseline(domain.LocalFingerprint{}, domain.RemoteFingerprint{})

	localCreate, err := PlanThreeWay(base, lf(7, 11), domain.RemoteFingerprint{}, domain.ContentUnknown)
	if err != nil {
		t.Fatal(err)
	}
	if localCreate.Kind != domain.DecisionEnsureRemote || !localCreate.ExpectedRemote.Absent {
		t.Fatalf("local create decision = %+v", localCreate)
	}

	remoteCreate, err := PlanThreeWay(base, domain.LocalFingerprint{}, rf("doc-new", "rev-new", 7), domain.ContentUnknown)
	if err != nil {
		t.Fatal(err)
	}
	if remoteCreate.Kind != domain.DecisionEnsureLocal || remoteCreate.ExpectedRemote.ID != "doc-new" {
		t.Fatalf("remote create decision = %+v", remoteCreate)
	}

	bothCreate, err := PlanThreeWay(base, lf(7, 11), rf("doc-new", "rev-new", 7), domain.ContentUnknown)
	if err != nil {
		t.Fatal(err)
	}
	if bothCreate.Kind != domain.DecisionCompareContent {
		t.Fatalf("simultaneous equal-size create = %+v", bothCreate)
	}

	bothDifferent, err := PlanThreeWay(base, lf(7, 11), rf("doc-new", "rev-new", 8), domain.ContentUnknown)
	if err != nil {
		t.Fatal(err)
	}
	if bothDifferent.Kind != domain.DecisionConflict || bothDifferent.Conflict != domain.ConflictSimultaneousCreate {
		t.Fatalf("simultaneous different create = %+v", bothDifferent)
	}
}

func TestPlanThreeWayCarriesMutationCAS(t *testing.T) {
	baseLocal := lf(10, 100)
	baseRemote := rf("doc-1", "rev-1", 10)
	base := baseline(baseLocal, baseRemote)

	upload, err := PlanThreeWay(base, lf(11, 200), baseRemote, domain.ContentUnknown)
	if err != nil {
		t.Fatal(err)
	}
	if upload.ExpectedRemote.Absent || upload.ExpectedRemote.ID != "doc-1" || upload.ExpectedRemote.Rev != "rev-1" {
		t.Fatalf("upload remote expectation = %+v", upload.ExpectedRemote)
	}
	if upload.ExpectedLocal != lf(11, 200) {
		t.Fatalf("upload local expectation = %+v", upload.ExpectedLocal)
	}

	downloadRemote := rf("doc-1", "rev-2", 11)
	download, err := PlanThreeWay(base, baseLocal, downloadRemote, domain.ContentUnknown)
	if err != nil {
		t.Fatal(err)
	}
	if download.ExpectedRemote.ID != "doc-1" || download.ExpectedRemote.Rev != "rev-2" {
		t.Fatalf("download remote expectation = %+v", download.ExpectedRemote)
	}
	if download.ExpectedLocal != baseLocal {
		t.Fatalf("download local expectation = %+v, want %+v", download.ExpectedLocal, baseLocal)
	}

	deleteRemote, err := PlanThreeWay(base, domain.LocalFingerprint{}, baseRemote, domain.ContentUnknown)
	if err != nil {
		t.Fatal(err)
	}
	if deleteRemote.ExpectedRemote.ID != "doc-1" || deleteRemote.ExpectedRemote.Rev != "rev-1" {
		t.Fatalf("delete remote expectation = %+v", deleteRemote.ExpectedRemote)
	}
}

func TestPlanThreeWayDirectories(t *testing.T) {
	base := baseline(ldir(), rdir("dir-1"))

	unchanged, err := PlanThreeWay(base, ldir(), rdir("dir-1"), domain.ContentUnknown)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Kind != domain.DecisionNoop {
		t.Fatalf("unchanged directory = %+v", unchanged)
	}

	replacedRemote, err := PlanThreeWay(base, ldir(), rdir("dir-2"), domain.ContentUnknown)
	if err != nil {
		t.Fatal(err)
	}
	if replacedRemote.Kind != domain.DecisionCommitBaseline {
		t.Fatalf("directory identity replacement = %+v", replacedRemote)
	}

	localDelete, err := PlanThreeWay(base, domain.LocalFingerprint{}, rdir("dir-1"), domain.ContentUnknown)
	if err != nil {
		t.Fatal(err)
	}
	if localDelete.Kind != domain.DecisionDeleteRemote || localDelete.EntryKind != domain.KindDir {
		t.Fatalf("local directory delete = %+v", localDelete)
	}
}

func TestPlannerRejectsInvalidInput(t *testing.T) {
	if _, err := PlanInitial("../escape", lf(1, 1), domain.RemoteFingerprint{}, domain.ContentUnknown); err == nil {
		t.Fatal("expected invalid relative path error")
	}
	if _, err := PlanInitial("a", domain.LocalFingerprint{Present: true, Kind: domain.KindFile, Size: -1}, domain.RemoteFingerprint{}, domain.ContentUnknown); err == nil {
		t.Fatal("expected invalid local size error")
	}
	if _, err := PlanInitial("a", lf(1, 1), domain.RemoteFingerprint{Present: true, Kind: domain.KindFile, ID: "id", Size: 1}, domain.ContentUnknown); err == nil {
		t.Fatal("expected missing remote revision error")
	}
}

func lf(size, mtime int64) domain.LocalFingerprint {
	return domain.LocalFingerprint{Present: true, Kind: domain.KindFile, Size: size, MtimeNS: mtime}
}

func rf(id, rev string, size int64) domain.RemoteFingerprint {
	return domain.RemoteFingerprint{Present: true, Kind: domain.KindFile, ID: id, Rev: rev, Size: size}
}

func ldir() domain.LocalFingerprint {
	return domain.LocalFingerprint{Present: true, Kind: domain.KindDir}
}

func rdir(id string) domain.RemoteFingerprint {
	return domain.RemoteFingerprint{Present: true, Kind: domain.KindDir, ID: id}
}

func baseline(local domain.LocalFingerprint, remote domain.RemoteFingerprint) domain.Baseline {
	return domain.Baseline{SyncRootID: 1, RelPath: "docs/a", Local: local, Remote: remote}
}
