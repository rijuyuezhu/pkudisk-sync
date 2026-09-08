package executor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

type lookupFS struct {
	fs.Fs
	object fs.Object
	err    error
}

func (f *lookupFS) NewObject(context.Context, string) (fs.Object, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.object == nil {
		return nil, fs.ErrorObjectNotFound
	}
	return f.object, nil
}

type fingerprintObject struct {
	fs.Object
	remote string
	id     string
	rev    string
	size   int64
	mtime  time.Time
}

func (o *fingerprintObject) Remote() string                    { return o.remote }
func (o *fingerprintObject) ID() string                        { return o.id }
func (o *fingerprintObject) Size() int64                       { return o.size }
func (o *fingerprintObject) ModTime(context.Context) time.Time { return o.mtime }
func (o *fingerprintObject) Metadata(context.Context) (fs.Metadata, error) {
	return fs.Metadata{"rev": o.rev}, nil
}

func testLocalFS(t *testing.T, root string) fs.Fs {
	t.Helper()
	f, err := local.NewFs(context.Background(), "test-local", root, configmap.Simple{})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func assertDownloadConfig(t *testing.T, ctx context.Context, id, rev string) {
	t.Helper()
	ci := fs.GetConfig(ctx)
	if ci.MultiThreadStreams != 1 || !ci.MultiThreadSet {
		t.Fatalf("download config multi-thread = %d set=%v", ci.MultiThreadStreams, ci.MultiThreadSet)
	}
	got := map[string]string{}
	for _, header := range ci.DownloadHeaders {
		got[header.Key] = header.Value
	}
	if got[syncExpectedIDDownloadHeader] != id || got[syncExpectedRevDownloadHeader] != rev {
		t.Fatalf("download headers = %#v", got)
	}
}

func TestUploadExpectedAbsentUsesConditionalMetadataAndReturnsWrittenIdentity(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	remote := &lookupFS{}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}, local: testLocalFS(t, root), remote: remote}
	expectedLocal, err := exec.ObserveLocalFile(ctx, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	exec.copyObjectFn = func(copyCtx context.Context, _ fs.Fs, dst fs.Object, remoteName string, src fs.Object) (fs.Object, error) {
		if dst != nil || remoteName != "a.txt" || src.Remote() != "a.txt" {
			t.Fatalf("copy args dst=%v remote=%q src=%q", dst, remoteName, src.Remote())
		}
		ci := fs.GetConfig(copyCtx)
		if ci.MetadataSet[syncExpectedAbsentMetadataKey] != "true" {
			t.Fatalf("upload metadata = %#v", ci.MetadataSet)
		}
		return &fingerprintObject{remote: "a.txt", id: "doc-new", rev: "rev-new", size: 7, mtime: time.Unix(2, 0)}, nil
	}
	got, err := exec.Upload(ctx, "a.txt", expectedLocal, domain.RemoteExpectation{Absent: true})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "doc-new" || got.Rev != "rev-new" || got.Size != 7 {
		t.Fatalf("Upload() = %+v", got)
	}
}

func TestDownloadToTempUsesExactRevisionSingleStream(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}, local: testLocalFS(t, root)}
	expected := domain.RemoteExpectation{ID: "doc", Rev: "rev"}
	exec.copyFileFn = func(copyCtx context.Context, dst, _ fs.Fs, dstRemote, srcRemote string) error {
		assertDownloadConfig(t, copyCtx, "doc", "rev")
		if srcRemote != "a.txt" || dstRemote != ".pkudisk-sync-tmp-test" {
			t.Fatalf("copy file src=%q dst=%q", srcRemote, dstRemote)
		}
		return os.WriteFile(filepath.Join(dst.Root(), filepath.FromSlash(dstRemote)), []byte("remote"), 0o600)
	}
	if err := exec.DownloadToTemp(ctx, "a.txt", ".pkudisk-sync-tmp-test", expected); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, ".pkudisk-sync-tmp-test"))
	if err != nil || string(got) != "remote" {
		t.Fatalf("downloaded temp = %q err=%v", got, err)
	}
}

func TestEnsureLocalFileStagesAndCommitsGuardedDownload(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	target := filepath.Join(root, "a.txt")
	expectedRemote := domain.RemoteExpectation{ID: "doc", Rev: "rev"}
	remoteState := domain.RemoteFingerprint{Present: true, Kind: domain.KindFile, ID: "doc", Rev: "rev", Size: 6}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	exec.observeRemoteFn = func(context.Context, string) (domain.RemoteFingerprint, error) { return remoteState, nil }
	exec.copyFileFn = func(copyCtx context.Context, dst, _ fs.Fs, dstRemote, _ string) error {
		assertDownloadConfig(t, copyCtx, "doc", "rev")
		return os.WriteFile(filepath.Join(dst.Root(), filepath.FromSlash(dstRemote)), []byte("remote"), 0o600)
	}
	op := domain.Operation{
		ID:                  7,
		Kind:                domain.OperationEnsureLocal,
		EntryKind:           domain.KindFile,
		SrcPath:             "a.txt",
		LocalTargetPath:     target,
		LocalTargetIdentity: physicalIdentityForTest(t, root),
		ExpectedRemote:      expectedRemote,
	}
	got, err := exec.EnsureLocalFile(ctx, op)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Present || got.Kind != domain.KindFile || got.Size != 6 {
		t.Fatalf("EnsureLocalFile() = %+v", got)
	}
	contents, err := os.ReadFile(target)
	if err != nil || string(contents) != "remote" {
		t.Fatalf("committed target = %q err=%v", contents, err)
	}
}

func TestCommitDownloadedTempRevalidatesRemoteAndMovesNoReplace(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".pkudisk-sync-tmp-test"), []byte("remote"), 0o600); err != nil {
		t.Fatal(err)
	}
	expectedRemote := domain.RemoteExpectation{ID: "doc", Rev: "rev"}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	exec.observeRemoteFn = func(context.Context, string) (domain.RemoteFingerprint, error) {
		return domain.RemoteFingerprint{Present: true, Kind: domain.KindFile, ID: "doc", Rev: "rev", Size: 6}, nil
	}
	got, err := exec.CommitDownloadedTemp(ctx, "a.txt", ".pkudisk-sync-tmp-test", domain.LocalFingerprint{}, expectedRemote)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Present || got.Size != 6 {
		t.Fatalf("CommitDownloadedTemp() = %+v", got)
	}
	contents, err := os.ReadFile(filepath.Join(root, "a.txt"))
	if err != nil || string(contents) != "remote" {
		t.Fatalf("committed file = %q err=%v", contents, err)
	}
}

func TestCompareFileContentUsesGuardedDownloadAndRevalidatesBothSides(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("same"), 0o600); err != nil {
		t.Fatal(err)
	}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}, local: testLocalFS(t, root)}
	expectedLocal, err := exec.ObserveLocalFile(ctx, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	expectedRemote := domain.RemoteExpectation{ID: "doc", Rev: "rev"}
	exec.observeRemoteFn = func(context.Context, string) (domain.RemoteFingerprint, error) {
		return domain.RemoteFingerprint{Present: true, Kind: domain.KindFile, ID: "doc", Rev: "rev", Size: 4}, nil
	}
	exec.copyFileFn = func(copyCtx context.Context, dst, _ fs.Fs, dstRemote, _ string) error {
		assertDownloadConfig(t, copyCtx, "doc", "rev")
		return os.WriteFile(filepath.Join(dst.Root(), filepath.FromSlash(dstRemote)), []byte("same"), 0o600)
	}
	equal, err := exec.CompareFileContent(ctx, "a.txt", expectedLocal, expectedRemote)
	if err != nil {
		t.Fatal(err)
	}
	if !equal {
		t.Fatal("equal local/remote contents reported different")
	}
}
