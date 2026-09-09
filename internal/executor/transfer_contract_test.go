package executor

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
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

type guardedContentObject struct {
	fingerprintObject
	t    *testing.T
	data string
}

func (o *guardedContentObject) Open(_ context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	o.t.Helper()
	assertGuardedOpenOptions(o.t, options, o.id, o.rev)
	return io.NopCloser(strings.NewReader(o.data)), nil
}

func assertGuardedOpenOptions(t *testing.T, options []fs.OpenOption, id, rev string) {
	t.Helper()
	got := map[string]string{}
	for _, option := range options {
		key, value := option.Header()
		if key != "" {
			got[key] = value
		}
	}
	if got[syncExpectedIDDownloadHeader] != id || got[syncExpectedRevDownloadHeader] != rev {
		t.Fatalf("guarded stream options = %#v, want id=%q rev=%q", got, id, rev)
	}
}

type reopeningContentObject struct {
	fingerprintObject
	t     *testing.T
	opens int
}

func (o *reopeningContentObject) Open(_ context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	o.t.Helper()
	assertGuardedOpenOptions(o.t, options, o.id, o.rev)
	var start int64
	for _, option := range options {
		switch value := option.(type) {
		case *fs.RangeOption:
			start = value.Start
		case *fs.SeekOption:
			start = value.Offset
		}
	}
	o.opens++
	switch o.opens {
	case 1:
		if start != 0 {
			o.t.Fatalf("initial stream starts at %d, want 0", start)
		}
		return io.NopCloser(io.MultiReader(strings.NewReader("sa"), iotest.ErrReader(errors.New("transient read failure")))), nil
	case 2:
		if start != 2 {
			o.t.Fatalf("reopened stream starts at %d, want 2", start)
		}
		return io.NopCloser(strings.NewReader("me")), nil
	default:
		o.t.Fatalf("unexpected remote reopen %d", o.opens)
		return nil, errors.New("unexpected reopen")
	}
}

func testLocalFS(t *testing.T, root string) fs.Fs {
	t.Helper()
	f, err := local.NewFs(context.Background(), "test-local", root, configmap.Simple{})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func testLocalCopyLinksFS(t *testing.T, root string) fs.Fs {
	t.Helper()
	f, err := local.NewFs(context.Background(), "test-local-copy-links", root, configmap.Simple{"copy_links": "true"})
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

func TestCopySymlinkUploadReadsProjectedFileBytes(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	external := t.TempDir()
	target := filepath.Join(external, "target.txt")
	if err := os.WriteFile(target, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "link.txt")); err != nil {
		t.Fatal(err)
	}
	remote := &lookupFS{}
	exec := &RootExecutor{
		root:   domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkCopy},
		local:  testLocalCopyLinksFS(t, root),
		remote: remote,
	}
	before, err := exec.ObserveLocalFile(ctx, "link.txt")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("projected-payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	expectedLocal, err := exec.ObserveLocalFile(ctx, "link.txt")
	if err != nil {
		t.Fatal(err)
	}
	if domain.LocalEquivalent(before, expectedLocal) {
		t.Fatalf("referent update was not observed: before=%+v after=%+v", before, expectedLocal)
	}
	if !expectedLocal.Present || expectedLocal.Size != int64(len("projected-payload")) {
		t.Fatalf("projected local fingerprint = %+v", expectedLocal)
	}
	exec.copyObjectFn = func(copyCtx context.Context, _ fs.Fs, dst fs.Object, remoteName string, src fs.Object) (fs.Object, error) {
		if dst != nil || remoteName != "link.txt" || src.Remote() != "link.txt" {
			t.Fatalf("copy args dst=%v remote=%q src=%q", dst, remoteName, src.Remote())
		}
		r, err := src.Open(copyCtx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := r.Close(); err != nil {
				t.Errorf("close upload source: %v", err)
			}
		}()
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "projected-payload" {
			t.Fatalf("upload source bytes = %q", got)
		}
		return &fingerprintObject{remote: "link.txt", id: "doc-new", rev: "rev-new", size: int64(len(got)), mtime: time.Unix(2, 0)}, nil
	}
	got, err := exec.Upload(ctx, "link.txt", expectedLocal, domain.RemoteExpectation{Absent: true})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "doc-new" || got.Rev != "rev-new" || got.Size != expectedLocal.Size {
		t.Fatalf("Upload() = %+v", got)
	}
}

func TestCopyDirectoryAliasesUploadIndependentLogicalPaths(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	external := t.TempDir()
	if err := os.WriteFile(filepath.Join(external, "x.txt"), []byte("shared"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{"a", "b"} {
		if err := os.Symlink(external, filepath.Join(root, alias)); err != nil {
			t.Fatal(err)
		}
	}
	exec := &RootExecutor{
		root:   domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkCopy},
		local:  testLocalCopyLinksFS(t, root),
		remote: &lookupFS{},
	}
	seen := make(map[string]string)
	exec.copyObjectFn = func(copyCtx context.Context, _ fs.Fs, _ fs.Object, remoteName string, src fs.Object) (fs.Object, error) {
		r, err := src.Open(copyCtx)
		if err != nil {
			return nil, err
		}
		defer func() {
			if err := r.Close(); err != nil {
				t.Errorf("close alias upload source: %v", err)
			}
		}()
		payload, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		seen[remoteName] = string(payload)
		return &fingerprintObject{remote: remoteName, id: "doc-" + remoteName, rev: "rev-" + remoteName, size: int64(len(payload)), mtime: time.Unix(2, 0)}, nil
	}
	for _, rel := range []string{"a/x.txt", "b/x.txt"} {
		expected, err := exec.ObserveLocalFile(ctx, rel)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := exec.Upload(ctx, rel, expected, domain.RemoteExpectation{Absent: true}); err != nil {
			t.Fatal(err)
		}
	}
	if seen["a/x.txt"] != "shared" || seen["b/x.txt"] != "shared" || len(seen) != 2 {
		t.Fatalf("independent alias uploads = %+v", seen)
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
	got, err := exec.EnsureLocalFile(ctx, op, nil, allowLocalSideEffectForTest)
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

func TestEnsureLocalFileDiscardsStalePlannedDownloadStagingBeforeRedownload(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	target := filepath.Join(root, "a.txt")
	expectedRemote := domain.RemoteExpectation{ID: "doc", Rev: "rev"}
	remoteState := domain.RemoteFingerprint{Present: true, Kind: domain.KindFile, ID: "doc", Rev: "rev", Size: 6}
	exec := &RootExecutor{root: domain.SyncRoot{LocalRoot: root}}
	exec.observeRemoteFn = func(context.Context, string) (domain.RemoteFingerprint, error) { return remoteState, nil }
	op := domain.Operation{
		ID:                   7,
		Kind:                 domain.OperationEnsureLocal,
		EntryKind:            domain.KindFile,
		SrcPath:              "a.txt",
		LocalTargetPath:      target,
		LocalTargetIdentity:  physicalIdentityForTest(t, root),
		LocalTargetAuthority: domain.LocalMutationLexical,
		ExpectedRemote:       expectedRemote,
		Phase:                domain.OperationPlanned,
	}
	stale := operationPhysicalTempPath(target, op.ID, "download")
	if err := os.WriteFile(stale, []byte("partial-old-download"), 0o600); err != nil {
		t.Fatal(err)
	}
	exec.copyFileFn = func(copyCtx context.Context, dst, _ fs.Fs, dstRemote, _ string) error {
		assertDownloadConfig(t, copyCtx, "doc", "rev")
		staged := filepath.Join(dst.Root(), filepath.FromSlash(dstRemote))
		if _, err := os.Lstat(staged); !os.IsNotExist(err) {
			t.Fatalf("stale download staging was not removed before redownload: %v", err)
		}
		return os.WriteFile(staged, []byte("remote"), 0o600)
	}

	got, err := exec.EnsureLocalFile(ctx, op, nil, allowLocalSideEffectForTest)
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
	if _, err := os.Lstat(stale); !os.IsNotExist(err) {
		t.Fatalf("download staging survived successful retry: %v", err)
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

func TestCompareFileContentStreamsGuardedRevisionWithoutStaging(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("same"), 0o600); err != nil {
		t.Fatal(err)
	}
	remoteObject := &guardedContentObject{
		fingerprintObject: fingerprintObject{remote: "a.txt", id: "doc", rev: "rev", size: 4},
		t:                 t,
		data:              "same",
	}
	exec := &RootExecutor{
		root:   domain.SyncRoot{LocalRoot: root},
		local:  testLocalFS(t, root),
		remote: &lookupFS{object: remoteObject},
	}
	expectedLocal, err := exec.ObserveLocalFile(ctx, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	expectedRemote := domain.RemoteExpectation{ID: "doc", Rev: "rev"}
	exec.observeRemoteFn = func(context.Context, string) (domain.RemoteFingerprint, error) {
		return domain.RemoteFingerprint{Present: true, Kind: domain.KindFile, ID: "doc", Rev: "rev", Size: 4}, nil
	}
	exec.copyFileFn = func(context.Context, fs.Fs, fs.Fs, string, string) error {
		t.Fatal("plan-time content comparison must not stage through CopyFile")
		return nil
	}
	equal, err := exec.CompareFileContent(ctx, "a.txt", expectedLocal, expectedRemote)
	if err != nil {
		t.Fatal(err)
	}
	if !equal {
		t.Fatal("equal local/remote contents reported different")
	}
}

func TestCompareFileContentRequiresCompleteRemoteStream(t *testing.T) {
	tests := []struct {
		name      string
		data      string
		wantError bool
		wantEqual bool
	}{
		{name: "short", data: "sa", wantError: true},
		{name: "overlong", data: "same-extra", wantError: true},
		{name: "exact different", data: "diff", wantEqual: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("same"), 0o600); err != nil {
				t.Fatal(err)
			}
			remoteObject := &guardedContentObject{
				fingerprintObject: fingerprintObject{remote: "a.txt", id: "doc", rev: "rev", size: 4},
				t:                 t,
				data:              test.data,
			}
			exec := &RootExecutor{
				root:   domain.SyncRoot{LocalRoot: root},
				local:  testLocalFS(t, root),
				remote: &lookupFS{object: remoteObject},
			}
			expectedLocal, err := exec.ObserveLocalFile(ctx, "a.txt")
			if err != nil {
				t.Fatal(err)
			}
			exec.observeRemoteFn = func(context.Context, string) (domain.RemoteFingerprint, error) {
				return domain.RemoteFingerprint{Present: true, Kind: domain.KindFile, ID: "doc", Rev: "rev", Size: 4}, nil
			}
			equal, err := exec.CompareFileContent(ctx, "a.txt", expectedLocal, domain.RemoteExpectation{ID: "doc", Rev: "rev"})
			if test.wantError {
				if err == nil {
					t.Fatalf("CompareFileContent() = equal %v, nil error; want incomplete-stream error", equal)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if equal != test.wantEqual {
				t.Fatalf("CompareFileContent() equal = %v, want %v", equal, test.wantEqual)
			}
		})
	}
}

func TestReadersEqualPropagatesRemoteReadError(t *testing.T) {
	transportErr := errors.New("transport failed")
	equal, err := readersEqual(
		strings.NewReader("same"),
		io.MultiReader(strings.NewReader("sa"), iotest.ErrReader(transportErr)),
	)
	if err == nil || !errors.Is(err, transportErr) {
		t.Fatalf("readersEqual() = equal %v, err %v; want remote read error", equal, err)
	}
}

func TestCompareFileContentReopensGuardedStreamOnReadFailure(t *testing.T) {
	ctx, config := fs.AddConfig(context.Background())
	config.LowLevelRetries = 2
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("same"), 0o600); err != nil {
		t.Fatal(err)
	}
	remoteObject := &reopeningContentObject{
		fingerprintObject: fingerprintObject{remote: "a.txt", id: "doc", rev: "rev", size: 4},
		t:                 t,
	}
	exec := &RootExecutor{
		root:   domain.SyncRoot{LocalRoot: root},
		local:  testLocalFS(t, root),
		remote: &lookupFS{object: remoteObject},
	}
	expectedLocal, err := exec.ObserveLocalFile(ctx, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	expectedRemote := domain.RemoteExpectation{ID: "doc", Rev: "rev"}
	exec.observeRemoteFn = func(context.Context, string) (domain.RemoteFingerprint, error) {
		return domain.RemoteFingerprint{Present: true, Kind: domain.KindFile, ID: "doc", Rev: "rev", Size: 4}, nil
	}
	equal, err := exec.CompareFileContent(ctx, "a.txt", expectedLocal, expectedRemote)
	if err != nil {
		t.Fatal(err)
	}
	if !equal {
		t.Fatal("reopened exact remote revision reported different content")
	}
	if remoteObject.opens != 2 {
		t.Fatalf("remote opens = %d, want initial open plus one range reopen", remoteObject.opens)
	}
}

func TestCopySymlinkContentComparisonUsesProjectedFileBytes(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	external := t.TempDir()
	target := filepath.Join(external, "target.txt")
	if err := os.WriteFile(target, []byte("same"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "link.txt")); err != nil {
		t.Fatal(err)
	}
	remoteObject := &guardedContentObject{
		fingerprintObject: fingerprintObject{remote: "link.txt", id: "doc", rev: "rev", size: 4},
		t:                 t,
		data:              "same",
	}
	exec := &RootExecutor{
		root:   domain.SyncRoot{LocalRoot: root, SymlinkMode: domain.SymlinkCopy},
		local:  testLocalCopyLinksFS(t, root),
		remote: &lookupFS{object: remoteObject},
	}
	expectedLocal, err := exec.ObserveLocalFile(ctx, "link.txt")
	if err != nil {
		t.Fatal(err)
	}
	expectedRemote := domain.RemoteExpectation{ID: "doc", Rev: "rev"}
	exec.observeRemoteFn = func(context.Context, string) (domain.RemoteFingerprint, error) {
		return domain.RemoteFingerprint{Present: true, Kind: domain.KindFile, ID: "doc", Rev: "rev", Size: 4}, nil
	}
	equal, err := exec.CompareFileContent(ctx, "link.txt", expectedLocal, expectedRemote)
	if err != nil {
		t.Fatal(err)
	}
	if !equal {
		t.Fatal("copy symlink projection content reported different")
	}
}
