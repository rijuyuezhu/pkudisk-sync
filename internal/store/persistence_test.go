package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

func TestStorePersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.sqlite3")

	s, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	root, err := s.CreateSyncRoot(ctx, domain.SyncRoot{
		UUID:                "reopen-root",
		LocalRoot:           "/tmp/reopen-root",
		RemoteName:          "pkudisk",
		RemoteRoot:          "Personal/Reopen",
		Enabled:             true,
		PollIntervalSeconds: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	base := domain.Baseline{
		SyncRootID: root.ID,
		RelPath:    "persist.txt",
		Local:      localFile(4, 40),
		Remote:     remoteFile("doc-persist", "rev-persist", 4),
	}
	if err := s.PutBaseline(ctx, base); err != nil {
		t.Fatal(err)
	}
	op, err := s.CreateOperation(ctx, domain.Operation{
		SyncRootID:     root.ID,
		Kind:           domain.OperationEnsureRemote,
		EntryKind:      domain.KindFile,
		SrcPath:        "persist.txt",
		ExpectedLocal:  localFile(5, 50),
		ExpectedRemote: domain.RemoteExpectation{ID: "doc-persist", Rev: "rev-persist"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	gotBase, ok, err := s.GetBaseline(ctx, root.ID, "persist.txt")
	if err != nil || !ok {
		t.Fatalf("baseline after reopen: %+v ok=%v err=%v", gotBase, ok, err)
	}
	assertBaselineEqual(t, gotBase, base)
	gotOp, ok, err := s.GetOperation(ctx, op.ID)
	if err != nil || !ok || gotOp.Phase != domain.OperationPlanned {
		t.Fatalf("operation after reopen: %+v ok=%v err=%v", gotOp, ok, err)
	}
}
