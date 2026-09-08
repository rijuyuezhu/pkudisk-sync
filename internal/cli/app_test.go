package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rijuyuezhu/pkudisk-sync/internal/apppaths"
	"github.com/rijuyuezhu/pkudisk-sync/internal/daemon"
	"github.com/rijuyuezhu/pkudisk-sync/internal/daemonlock"
	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
	"github.com/rijuyuezhu/pkudisk-sync/internal/reconcile"
	"github.com/rijuyuezhu/pkudisk-sync/internal/rootmarker"
	"github.com/rijuyuezhu/pkudisk-sync/internal/store"
	"github.com/rijuyuezhu/pkudisk-sync/internal/syncer"
	"github.com/rijuyuezhu/pkudisk-sync/internal/userservice"
)

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestCLIReportsOutputWriteFailures(t *testing.T) {
	wantErr := errors.New("output unavailable")
	for _, args := range [][]string{{"help"}, {"paths"}, {"version"}} {
		t.Run(strings.Join(args, "-"), func(t *testing.T) {
			app := New(apppaths.Paths{}, failingWriter{err: wantErr}, &bytes.Buffer{})
			if err := app.Run(context.Background(), args); !errors.Is(err, wantErr) {
				t.Fatalf("Run(%v) error = %v, want %v", args, err, wantErr)
			}
		})
	}
}

func TestRootAddListPauseResume(t *testing.T) {
	ctx := context.Background()
	paths := cliTestPaths(t)
	var stdout, stderr bytes.Buffer
	app := New(paths, &stdout, &stderr)
	app.installRcloneConfig = func(path string) error {
		if path != paths.RcloneConfig {
			t.Fatalf("rclone config path = %q, want %q", path, paths.RcloneConfig)
		}
		return nil
	}
	app.validateRemote = func(name string) error {
		if name != "pkudisk" {
			t.Fatalf("remote name = %q", name)
		}
		return nil
	}
	app.newUUID = func() (string, error) { return "root-fixed-uuid", nil }
	localRoot := t.TempDir()

	if err := app.Run(ctx, []string{"root", "add", "--local", localRoot, "--remote", "pkudisk:Personal/Data", "--poll", "2m"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "added root 1") {
		t.Fatalf("add output = %q", stdout.String())
	}
	if err := rootmarker.Check(localRoot, "root-fixed-uuid"); err != nil {
		t.Fatalf("root marker: %v", err)
	}
	state, err := store.Open(ctx, paths.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	roots, err := state.ListSyncRoots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = state.Close()
	if len(roots) != 1 || roots[0].PollIntervalSeconds != 120 || !roots[0].Enabled {
		t.Fatalf("stored root = %+v", roots)
	}

	stdout.Reset()
	if err := app.Run(ctx, []string{"root", "list"}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"ID", "enabled", localRoot, "pkudisk:Personal/Data", "2m0s"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("list output %q missing %q", stdout.String(), want)
		}
	}

	stdout.Reset()
	if err := app.Run(ctx, []string{"root", "pause", "1"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "paused root 1") {
		t.Fatalf("pause output = %q", stdout.String())
	}
	state, err = store.Open(ctx, paths.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	paused, ok, err := state.GetSyncRoot(ctx, 1)
	_ = state.Close()
	if err != nil || !ok || paused.Enabled {
		t.Fatalf("paused root = %+v, %v, %v", paused, ok, err)
	}

	stdout.Reset()
	if err := app.Run(ctx, []string{"root", "resume", "1"}); err != nil {
		t.Fatal(err)
	}
	state, err = store.Open(ctx, paths.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	resumed, ok, err := state.GetSyncRoot(ctx, 1)
	_ = state.Close()
	if err != nil || !ok || !resumed.Enabled {
		t.Fatalf("resumed root = %+v, %v, %v", resumed, ok, err)
	}
}

func TestRootAddRecoversOrphanMarkerOnlyWithExplicitFlag(t *testing.T) {
	ctx := context.Background()
	paths := cliTestPaths(t)
	app := New(paths, &bytes.Buffer{}, &bytes.Buffer{})
	app.installRcloneConfig = func(string) error { return nil }
	app.validateRemote = func(string) error { return nil }
	app.newUUID = func() (string, error) { return "replacement-root-uuid", nil }
	localRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(localRoot, "keep.txt"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := rootmarker.Ensure(localRoot, "orphan-root-uuid"); err != nil {
		t.Fatal(err)
	}
	baseArgs := []string{"root", "add", "--local", localRoot, "--remote", "pkudisk:Personal/Recovered"}
	if err := app.Run(ctx, baseArgs); err == nil {
		t.Fatal("root add silently adopted an existing marker")
	}
	if err := rootmarker.Check(localRoot, "orphan-root-uuid"); err != nil {
		t.Fatalf("ordinary add changed orphan marker: %v", err)
	}

	recoverArgs := append(append([]string{}, baseArgs...), "--recover-orphan-marker")
	if err := app.Run(ctx, recoverArgs); err != nil {
		t.Fatal(err)
	}
	if err := rootmarker.Check(localRoot, "replacement-root-uuid"); err != nil {
		t.Fatalf("explicit recovery did not replace marker: %v", err)
	}
	contents, err := os.ReadFile(filepath.Join(localRoot, "keep.txt"))
	if err != nil || string(contents) != "keep" {
		t.Fatalf("explicit orphan recovery changed user data: contents=%q err=%v", contents, err)
	}
}

func TestStatusAndConflictListExposeDurableAttentionState(t *testing.T) {
	ctx := context.Background()
	paths := cliTestPaths(t)
	var stdout bytes.Buffer
	app := New(paths, &stdout, &bytes.Buffer{})
	app.installRcloneConfig = func(string) error { return nil }
	app.validateRemote = func(string) error { return nil }
	app.newUUID = func() (string, error) { return "status-root-uuid", nil }
	localRoot := t.TempDir()
	if err := app.Run(ctx, []string{"root", "add", "--local", localRoot, "--remote", "pkudisk:Personal/Status"}); err != nil {
		t.Fatal(err)
	}

	state, err := store.Open(ctx, paths.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	local := domain.LocalFingerprint{Present: true, Kind: domain.KindFile, Size: 12, MtimeNS: 123}
	for _, op := range []domain.Operation{
		{
			SyncRootID:     1,
			Kind:           domain.OperationEnsureRemote,
			EntryKind:      domain.KindFile,
			SrcPath:        "planned.txt",
			ExpectedLocal:  local,
			ExpectedRemote: domain.RemoteExpectation{Absent: true},
		},
		{
			SyncRootID:     1,
			Kind:           domain.OperationEnsureRemote,
			EntryKind:      domain.KindFile,
			SrcPath:        "running.txt",
			ExpectedLocal:  local,
			ExpectedRemote: domain.RemoteExpectation{Absent: true},
			Phase:          domain.OperationRunning,
			Attempts:       1,
		},
		{
			SyncRootID:     1,
			Kind:           domain.OperationEnsureRemote,
			EntryKind:      domain.KindFile,
			SrcPath:        "recovering.txt",
			ExpectedLocal:  local,
			ExpectedRemote: domain.RemoteExpectation{Absent: true},
			Phase:          domain.OperationRecovering,
			Attempts:       1,
			LastError:      "outcome unknown",
		},
		{
			SyncRootID:     1,
			Kind:           domain.OperationEnsureRemote,
			EntryKind:      domain.KindFile,
			SrcPath:        "blocked.txt",
			ExpectedLocal:  local,
			ExpectedRemote: domain.RemoteExpectation{Absent: true},
			Phase:          domain.OperationBlocked,
			Attempts:       1,
			LastError:      "needs attention",
		},
	} {
		if _, err := state.CreateOperation(ctx, op); err != nil {
			_ = state.Close()
			t.Fatal(err)
		}
	}
	if _, err := state.CreateConflict(ctx, domain.Conflict{
		SyncRootID: 1,
		RelPath:    "conflict.txt",
		Kind:       domain.ConflictBothModified,
		Local:      local,
		Remote: domain.RemoteFingerprint{
			Present: true,
			Kind:    domain.KindFile,
			ID:      "doc-conflict",
			Rev:     "rev-b",
			Size:    13,
		},
	}); err != nil {
		_ = state.Close()
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}

	stdout.Reset()
	if err := app.Run(ctx, []string{"status"}); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("status output = %q", stdout.String())
	}
	fields := strings.Fields(lines[1])
	if len(fields) < 10 {
		t.Fatalf("status row fields = %#v", fields)
	}
	if fields[0] != "1" || fields[1] != "enabled" || fields[3] != "1" || fields[4] != "1" || fields[5] != "1" || fields[6] != "1" || fields[7] != "1" {
		t.Fatalf("status row = %#v", fields)
	}

	stdout.Reset()
	if err := app.Run(ctx, []string{"conflict", "list", "--root", "1"}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"both-modified", "conflict.txt", "file:12B", "file:13B@rev-b"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("conflict output %q missing %q", stdout.String(), want)
		}
	}
	if err := app.Run(ctx, []string{"conflict", "list", "--root", "999"}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("unknown root conflict list error = %v", err)
	}
}

func TestRootRemoveRequiresStoppedDaemonAndPausedRootAndKeepsData(t *testing.T) {
	ctx := context.Background()
	paths := cliTestPaths(t)
	var stdout bytes.Buffer
	app := New(paths, &stdout, &bytes.Buffer{})
	app.installRcloneConfig = func(string) error { return nil }
	app.validateRemote = func(string) error { return nil }
	app.newUUID = func() (string, error) { return "remove-cli-uuid", nil }
	localRoot := t.TempDir()
	userFile := filepath.Join(localRoot, "keep.txt")
	if err := os.WriteFile(userFile, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := app.Run(ctx, []string{"root", "add", "--local", localRoot, "--remote", "pkudisk:Personal/RemoveCLI"}); err != nil {
		t.Fatal(err)
	}
	if err := app.Run(ctx, []string{"root", "remove", "1"}); err == nil || !strings.Contains(err.Error(), "must be paused") {
		t.Fatalf("enabled root remove error = %v", err)
	}
	if err := app.Run(ctx, []string{"root", "pause", "1"}); err != nil {
		t.Fatal(err)
	}
	if err := paths.PrepareRuntime(); err != nil {
		t.Fatal(err)
	}
	held, err := daemonlock.Acquire(paths.RuntimeDir)
	if err != nil {
		t.Fatal(err)
	}
	removeErr := app.Run(ctx, []string{"root", "remove", "1"})
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if removeErr == nil || !strings.Contains(removeErr.Error(), "stopped") {
		t.Fatalf("root remove while daemon lease held = %v", removeErr)
	}

	stdout.Reset()
	if err := app.Run(ctx, []string{"root", "remove", "1"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "data left unchanged") {
		t.Fatalf("root remove output = %q", stdout.String())
	}
	if _, err := os.ReadFile(userFile); err != nil {
		t.Fatalf("root remove deleted user data: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(localRoot, rootmarker.FileName)); !os.IsNotExist(err) {
		t.Fatalf("root remove left marker: %v", err)
	}
	state, err := store.Open(ctx, paths.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	if _, ok, err := state.GetSyncRoot(ctx, 1); err != nil || ok {
		t.Fatalf("removed root still in store: ok=%v err=%v", ok, err)
	}
}

func TestConflictResolveQueuesExactDurableOperationWithoutMarkingConflictResolved(t *testing.T) {
	ctx := context.Background()
	paths := cliTestPaths(t)
	var stdout bytes.Buffer
	app := New(paths, &stdout, &bytes.Buffer{})
	app.installRcloneConfig = func(string) error { return nil }
	app.validateRemote = func(string) error { return nil }
	app.newUUID = func() (string, error) { return "resolve-root-uuid", nil }
	if err := app.Run(ctx, []string{"root", "add", "--local", t.TempDir(), "--remote", "pkudisk:Personal/Resolve"}); err != nil {
		t.Fatal(err)
	}

	state, err := store.Open(ctx, paths.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	conflict, err := state.CreateConflict(ctx, domain.Conflict{
		SyncRootID: 1,
		RelPath:    "conflict.txt",
		Kind:       domain.ConflictBothModified,
		Local:      domain.LocalFingerprint{Present: true, Kind: domain.KindFile, Size: 12, MtimeNS: 123},
		Remote:     domain.RemoteFingerprint{Present: true, Kind: domain.KindFile, ID: "doc", Rev: "rev-b", Size: 13},
	})
	if err != nil {
		_ = state.Close()
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}

	stdout.Reset()
	if err := app.Run(ctx, []string{"conflict", "resolve", "1", "--keep-local"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "queued conflict 1 keep-local as operation") {
		t.Fatalf("resolve output = %q", stdout.String())
	}
	state, err = store.Open(ctx, paths.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	operations, err := state.ListOperations(ctx, 1)
	if err != nil {
		_ = state.Close()
		t.Fatal(err)
	}
	if len(operations) != 1 || operations[0].Kind != domain.OperationEnsureRemote || operations[0].ExpectedRemote.ID != "doc" || operations[0].ExpectedRemote.Rev != "rev-b" {
		_ = state.Close()
		t.Fatalf("queued resolution operation = %+v", operations)
	}
	storedConflict, ok, err := state.GetConflict(ctx, conflict.ID)
	if err != nil || !ok || storedConflict.Resolved {
		_ = state.Close()
		t.Fatalf("conflict was prematurely resolved: %+v ok=%v err=%v", storedConflict, ok, err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	if err := app.Run(ctx, []string{"conflict", "resolve", "--keep-remote", "1"}); err == nil || !strings.Contains(err.Error(), "pending operation") {
		t.Fatalf("duplicate resolution error = %v", err)
	}
}

func TestConflictResolveRejectsKindMismatchWithoutQueueingOperation(t *testing.T) {
	ctx := context.Background()
	paths := cliTestPaths(t)
	app := New(paths, &bytes.Buffer{}, &bytes.Buffer{})
	app.installRcloneConfig = func(string) error { return nil }
	app.validateRemote = func(string) error { return nil }
	app.newUUID = func() (string, error) { return "kind-root-uuid", nil }
	if err := app.Run(ctx, []string{"root", "add", "--local", t.TempDir(), "--remote", "pkudisk:Personal/Kind"}); err != nil {
		t.Fatal(err)
	}
	state, err := store.Open(ctx, paths.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.CreateConflict(ctx, domain.Conflict{
		SyncRootID: 1,
		RelPath:    "mixed",
		Kind:       domain.ConflictKindMismatch,
		Local:      domain.LocalFingerprint{Present: true, Kind: domain.KindFile, Size: 1, MtimeNS: 1},
		Remote:     domain.RemoteFingerprint{Present: true, Kind: domain.KindDir, ID: "dir"},
	}); err != nil {
		_ = state.Close()
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	err = app.Run(ctx, []string{"conflict", "resolve", "1", "--keep-local"})
	if err == nil || !strings.Contains(err.Error(), "kind mismatch") {
		t.Fatalf("kind mismatch resolution error = %v", err)
	}
	state, err = store.Open(ctx, paths.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	if operations, err := state.ListOperations(ctx, 1); err != nil || len(operations) != 0 {
		t.Fatalf("kind mismatch queued operations = %+v err=%v", operations, err)
	}
}

func TestRootAddValidatesRemoteBeforeCreatingRootState(t *testing.T) {
	paths := cliTestPaths(t)
	app := New(paths, &bytes.Buffer{}, &bytes.Buffer{})
	app.installRcloneConfig = func(string) error { return nil }
	app.validateRemote = func(string) error { return errors.New("missing remote") }
	localRoot := t.TempDir()

	err := app.Run(context.Background(), []string{"root", "add", "--local", localRoot, "--remote", "pkudisk:Personal/Data"})
	if err == nil || !strings.Contains(err.Error(), "missing remote") {
		t.Fatalf("root add error = %v", err)
	}
	if _, err := os.Lstat(filepath.Join(localRoot, rootmarker.FileName)); !os.IsNotExist(err) {
		t.Fatalf("failed remote validation created marker: %v", err)
	}
	if _, err := os.Stat(paths.StateDB); !os.IsNotExist(err) {
		t.Fatalf("failed remote validation created state database: %v", err)
	}
}

func TestRootAddRejectsSubsecondPoll(t *testing.T) {
	paths := cliTestPaths(t)
	app := New(paths, &bytes.Buffer{}, &bytes.Buffer{})
	localRoot := t.TempDir()
	err := app.Run(context.Background(), []string{"root", "add", "--local", localRoot, "--remote", "pkudisk:Personal/Data", "--poll", "1500ms"})
	if err == nil || !strings.Contains(err.Error(), "whole number of seconds") {
		t.Fatalf("root add error = %v", err)
	}
}

func TestRootSymlinkPolicyCanBeConfiguredWhilePaused(t *testing.T) {
	ctx := context.Background()
	paths := cliTestPaths(t)
	var stdout bytes.Buffer
	app := New(paths, &stdout, &bytes.Buffer{})
	app.installRcloneConfig = func(string) error { return nil }
	app.validateRemote = func(string) error { return nil }
	app.newUUID = func() (string, error) { return "symlink-policy-root", nil }
	localRoot := t.TempDir()

	if err := app.Run(ctx, []string{"root", "add", "--local", localRoot, "--remote", "pkudisk:Personal/Symlink", "--symlinks", "reject"}); err != nil {
		t.Fatal(err)
	}
	if err := app.Run(ctx, []string{"root", "pause", "1"}); err != nil {
		t.Fatal(err)
	}
	if err := app.Run(ctx, []string{"root", "config", "1", "--symlinks", "ignore"}); err != nil {
		t.Fatal(err)
	}

	state, err := store.Open(ctx, paths.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	root, ok, err := state.GetSyncRoot(ctx, 1)
	if err != nil || !ok {
		t.Fatalf("GetSyncRoot() = %+v ok=%v err=%v", root, ok, err)
	}
	if root.Enabled || root.EffectiveSymlinkMode() != domain.SymlinkIgnore {
		t.Fatalf("configured root = %+v", root)
	}
	if !strings.Contains(stdout.String(), "configured root 1 symlinks=ignore") {
		t.Fatalf("root config output = %q", stdout.String())
	}
}

func TestPathsCommand(t *testing.T) {
	paths := cliTestPaths(t)
	var stdout bytes.Buffer
	app := New(paths, &stdout, &bytes.Buffer{})
	if err := app.Run(context.Background(), []string{"paths"}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{paths.StateDB, paths.RcloneConfig, paths.CacheDir, paths.RuntimeDir} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("paths output %q missing %q", stdout.String(), want)
		}
	}
}

func TestVersionCommandUsesDevelopmentDefaults(t *testing.T) {
	var stdout bytes.Buffer
	app := New(cliTestPaths(t), &stdout, &bytes.Buffer{})
	if err := app.Run(context.Background(), []string{"version"}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"pkudisk-sync dev", "commit\tunknown", "built\tunknown"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("version output %q missing %q", stdout.String(), want)
		}
	}
	if err := app.Run(context.Background(), []string{"version", "extra"}); err == nil {
		t.Fatal("version accepted positional arguments")
	}
}

func TestRemoteConfigureUsesSingleAppOwnedRemote(t *testing.T) {
	paths := cliTestPaths(t)
	var stdout bytes.Buffer
	app := New(paths, &stdout, &bytes.Buffer{})
	var gotPath, gotName string
	app.configureRemote = func(_ context.Context, path, name string) error {
		gotPath, gotName = path, name
		return nil
	}
	if err := app.Run(context.Background(), []string{"remote", "configure"}); err != nil {
		t.Fatal(err)
	}
	if gotPath != paths.RcloneConfig || gotName != "pkudisk" {
		t.Fatalf("configureRemote path=%q name=%q", gotPath, gotName)
	}
	if info, err := os.Stat(filepath.Dir(paths.RcloneConfig)); err != nil || !info.IsDir() {
		t.Fatalf("config directory was not prepared: info=%v err=%v", info, err)
	}
	if !strings.Contains(stdout.String(), "configured PKU Disk remote pkudisk") {
		t.Fatalf("remote configure output = %q", stdout.String())
	}

	gotName = ""
	if err := app.Run(context.Background(), []string{"remote", "configure", "school"}); err == nil || !strings.Contains(err.Error(), "single app-owned remote") {
		t.Fatalf("custom remote configure error = %v", err)
	}
	if gotName != "" {
		t.Fatalf("custom remote name reached configureRemote: %q", gotName)
	}
}

func TestRootAddRejectsNonAppRemoteBeforeConfigMutation(t *testing.T) {
	paths := cliTestPaths(t)
	app := New(paths, &bytes.Buffer{}, &bytes.Buffer{})
	installed := false
	validated := false
	app.installRcloneConfig = func(string) error {
		installed = true
		return nil
	}
	app.validateRemote = func(string) error {
		validated = true
		return nil
	}
	err := app.Run(context.Background(), []string{"root", "add", "--local", t.TempDir(), "--remote", "school:Personal/Data"})
	if err == nil || !strings.Contains(err.Error(), "single app-owned remote") {
		t.Fatalf("root add error = %v", err)
	}
	if installed || validated {
		t.Fatalf("unsupported remote reached config surface: installed=%v validated=%v", installed, validated)
	}
}

func TestRemoteConfigureRequiresStoppedDaemon(t *testing.T) {
	paths := cliTestPaths(t)
	if err := paths.PrepareRuntime(); err != nil {
		t.Fatal(err)
	}
	lease, err := daemonlock.Acquire(paths.RuntimeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Close() }()

	called := false
	app := New(paths, &bytes.Buffer{}, &bytes.Buffer{})
	app.configureRemote = func(context.Context, string, string) error {
		called = true
		return nil
	}
	err = app.Run(context.Background(), []string{"remote", "configure"})
	if err == nil || !strings.Contains(err.Error(), "requires the foreground daemon and user service to be stopped") {
		t.Fatalf("remote configure error = %v", err)
	}
	if called {
		t.Fatal("remote configuration ran while daemon lease was held")
	}
}

func TestDaemonWiresDeletePolicyAndRunner(t *testing.T) {
	paths := cliTestPaths(t)
	var stdout, stderr bytes.Buffer
	app := New(paths, &stdout, &stderr)
	installed := false
	app.installRcloneConfig = func(path string) error {
		installed = true
		if path != paths.RcloneConfig {
			t.Fatalf("rclone config path = %q", path)
		}
		return nil
	}
	fake := &fakeDaemonRunner{}
	var gotPolicy reconcile.DeletePolicy
	app.newRunner = func(_ *store.Store, policy reconcile.DeletePolicy, report daemon.Reporter) (daemonRunner, error) {
		gotPolicy = policy
		if report == nil {
			t.Fatal("daemon reporter is nil")
		}
		return fake, nil
	}
	if err := app.Run(context.Background(), []string{"daemon", "--max-delete-count", "17", "--max-delete-fraction", "0.1"}); err != nil {
		t.Fatal(err)
	}
	if !installed || !fake.called {
		t.Fatalf("daemon wiring: installed=%v runner_called=%v", installed, fake.called)
	}
	if gotPolicy.MaxCount != 17 || gotPolicy.MaxFraction != 0.1 || gotPolicy.MassDeleteApproved {
		t.Fatalf("daemon policy = %+v", gotPolicy)
	}
}

func TestDaemonDefaultDeletePolicyUsesCountGuardOnly(t *testing.T) {
	paths := cliTestPaths(t)
	app := New(paths, &bytes.Buffer{}, &bytes.Buffer{})
	app.installRcloneConfig = func(string) error { return nil }
	var gotPolicy reconcile.DeletePolicy
	app.newRunner = func(_ *store.Store, policy reconcile.DeletePolicy, _ daemon.Reporter) (daemonRunner, error) {
		gotPolicy = policy
		return &fakeDaemonRunner{}, nil
	}
	if err := app.Run(context.Background(), []string{"daemon"}); err != nil {
		t.Fatal(err)
	}
	if gotPolicy.MaxCount != 100 || gotPolicy.MaxFraction != 0 || gotPolicy.MassDeleteApproved {
		t.Fatalf("default daemon policy = %+v", gotPolicy)
	}
}

func TestDaemonRefusesSecondInstanceBeforeWiringRunner(t *testing.T) {
	paths := cliTestPaths(t)
	if err := paths.PrepareRuntime(); err != nil {
		t.Fatal(err)
	}
	held, err := daemonlock.Acquire(paths.RuntimeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()

	app := New(paths, &bytes.Buffer{}, &bytes.Buffer{})
	app.installRcloneConfig = func(string) error {
		t.Fatal("rclone config installed before daemon lease was acquired")
		return nil
	}
	app.newRunner = func(*store.Store, reconcile.DeletePolicy, daemon.Reporter) (daemonRunner, error) {
		t.Fatal("runner created while another daemon held the lease")
		return nil, nil
	}
	err = app.Run(context.Background(), []string{"daemon"})
	if !errors.Is(err, daemonlock.ErrAlreadyRunning) {
		t.Fatalf("second daemon error = %v, want ErrAlreadyRunning", err)
	}
}

func TestServiceDaemonTreatsExistingOwnerAsCleanExit(t *testing.T) {
	applicationPaths := cliTestPaths(t)
	servicePaths := cliTestPaths(t)
	if err := servicePaths.PrepareRuntime(); err != nil {
		t.Fatal(err)
	}
	held, err := daemonlock.Acquire(servicePaths.RuntimeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()

	app := New(applicationPaths, &bytes.Buffer{}, &bytes.Buffer{})
	app.servicePaths = func() (apppaths.Paths, error) { return servicePaths, nil }
	app.installRcloneConfig = func(string) error {
		t.Fatal("service-mode lock conflict reached rclone config")
		return nil
	}
	app.newRunner = func(*store.Store, reconcile.DeletePolicy, daemon.Reporter) (daemonRunner, error) {
		t.Fatal("service-mode lock conflict created runner")
		return nil, nil
	}
	if err := app.Run(context.Background(), []string{"daemon", "--service"}); err != nil {
		t.Fatalf("service-mode duplicate daemon error = %v", err)
	}
}

func TestDaemonRejectsAllDeleteThresholdsDisabled(t *testing.T) {
	paths := cliTestPaths(t)
	app := New(paths, &bytes.Buffer{}, &bytes.Buffer{})
	err := app.Run(context.Background(), []string{"daemon", "--max-delete-count", "0", "--max-delete-fraction", "0"})
	if err == nil || !strings.Contains(err.Error(), "at least one") {
		t.Fatalf("daemon error = %v", err)
	}
}

func TestServiceCommandsWireNativeManager(t *testing.T) {
	for _, name := range []string{
		"PKUDISK_SYNC_STATE_DB",
		"PKUDISK_SYNC_RCLONE_CONFIG",
		"PKUDISK_SYNC_CACHE_DIR",
		"PKUDISK_SYNC_RUNTIME_DIR",
	} {
		t.Setenv(name, "")
	}
	paths := cliTestPaths(t)
	var stdout bytes.Buffer
	app := New(paths, &stdout, &bytes.Buffer{})
	app.servicePaths = func() (apppaths.Paths, error) { return paths, nil }
	executable := filepath.Join(t.TempDir(), "bin", "pkudisk-sync")
	if err := os.MkdirAll(filepath.Dir(executable), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(executable, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	app.executablePath = func() (string, error) { return executable, nil }
	fake := &fakeServiceManager{status: userservice.StatusActive}
	app.newService = func(got string) (userservice.Manager, error) {
		if got != executable {
			t.Fatalf("service executable = %q, want %q", got, executable)
		}
		return fake, nil
	}

	for _, command := range []string{"install", "status", "start", "stop", "uninstall"} {
		if err := app.Run(context.Background(), []string{"service", command}); err != nil {
			t.Fatalf("service %s: %v", command, err)
		}
	}
	wantCalls := []string{"install", "status", "start", "stop", "uninstall"}
	if strings.Join(fake.calls, ",") != strings.Join(wantCalls, ",") {
		t.Fatalf("service calls = %v, want %v", fake.calls, wantCalls)
	}
	for _, want := range []string{"service installed", "active", "service started", "service stopped", "service uninstalled"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("service output %q missing %q", stdout.String(), want)
		}
	}
}

func TestServiceStartRefusesForegroundDaemonBeforeManagerStart(t *testing.T) {
	paths := cliTestPaths(t)
	if err := paths.PrepareRuntime(); err != nil {
		t.Fatal(err)
	}
	held, err := daemonlock.Acquire(paths.RuntimeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()

	app := New(paths, &bytes.Buffer{}, &bytes.Buffer{})
	app.servicePaths = func() (apppaths.Paths, error) { return paths, nil }
	app.executablePath = func() (string, error) { return filepath.Join(t.TempDir(), "pkudisk-sync"), nil }
	fake := &fakeServiceManager{}
	app.newService = func(string) (userservice.Manager, error) { return fake, nil }
	err = app.Run(context.Background(), []string{"service", "start"})
	if err == nil || !errors.Is(err, daemonlock.ErrAlreadyRunning) || !strings.Contains(err.Error(), "foreground daemon") {
		t.Fatalf("service start error = %v", err)
	}
	if len(fake.calls) != 0 {
		t.Fatalf("service manager was invoked despite occupied daemon lease: %v", fake.calls)
	}
}

func TestServiceInstallRefusesRunningDaemonBeforeManagerInstall(t *testing.T) {
	for _, name := range []string{
		"PKUDISK_SYNC_STATE_DB",
		"PKUDISK_SYNC_RCLONE_CONFIG",
		"PKUDISK_SYNC_CACHE_DIR",
		"PKUDISK_SYNC_RUNTIME_DIR",
	} {
		t.Setenv(name, "")
	}
	paths := cliTestPaths(t)
	if err := paths.PrepareRuntime(); err != nil {
		t.Fatal(err)
	}
	held, err := daemonlock.Acquire(paths.RuntimeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()

	app := New(paths, &bytes.Buffer{}, &bytes.Buffer{})
	app.servicePaths = func() (apppaths.Paths, error) { return paths, nil }
	app.executablePath = func() (string, error) { return filepath.Join(t.TempDir(), "pkudisk-sync"), nil }
	fake := &fakeServiceManager{}
	app.newService = func(string) (userservice.Manager, error) { return fake, nil }
	err = app.Run(context.Background(), []string{"service", "install"})
	if err == nil || !errors.Is(err, daemonlock.ErrAlreadyRunning) || !strings.Contains(err.Error(), "user service") {
		t.Fatalf("service install error = %v", err)
	}
	if len(fake.calls) != 0 {
		t.Fatalf("service manager install was invoked despite occupied daemon lease: %v", fake.calls)
	}
}

func TestServiceInstallAndStartRejectNonDefaultPathAuthorityBeforeProvider(t *testing.T) {
	for _, command := range []string{"install", "start"} {
		t.Run(command, func(t *testing.T) {
			paths := cliTestPaths(t)
			servicePaths := paths
			servicePaths.StateDB = filepath.Join(t.TempDir(), "native", "state.db")
			app := New(paths, &bytes.Buffer{}, &bytes.Buffer{})
			app.servicePaths = func() (apppaths.Paths, error) { return servicePaths, nil }
			app.executablePath = func() (string, error) {
				t.Fatal("service provider resolution ran before path-authority rejection")
				return "", nil
			}
			err := app.Run(context.Background(), []string{"service", command})
			if err == nil || !strings.Contains(err.Error(), "requires OS-native default") {
				t.Fatalf("service %s error = %v", command, err)
			}
		})
	}
}

func TestParseRemoteSpec(t *testing.T) {
	name, root, err := parseRemoteSpec("pkudisk:Personal/Path:WithColon")
	if err != nil {
		t.Fatal(err)
	}
	if name != "pkudisk" || root != "Personal/Path:WithColon" {
		t.Fatalf("parseRemoteSpec = %q, %q", name, root)
	}
	for _, bad := range []string{"pkudisk", ":Personal/Data", "pkudisk:", " pkudisk:Personal/Data"} {
		if _, _, err := parseRemoteSpec(bad); err == nil {
			t.Fatalf("invalid remote spec %q accepted", bad)
		}
	}
}

func TestRandomUUIDShape(t *testing.T) {
	value, err := randomUUID()
	if err != nil {
		t.Fatal(err)
	}
	if len(value) != 36 || value[14] != '4' || (value[19] != '8' && value[19] != '9' && value[19] != 'a' && value[19] != 'b') {
		t.Fatalf("randomUUID() = %q", value)
	}
}

type fakeDaemonRunner struct {
	called bool
}

func (r *fakeDaemonRunner) Run(context.Context) error {
	r.called = true
	return nil
}

type fakeServiceManager struct {
	calls  []string
	status userservice.Status
}

func (m *fakeServiceManager) Install(context.Context) error {
	m.calls = append(m.calls, "install")
	return nil
}

func (m *fakeServiceManager) Uninstall(context.Context) error {
	m.calls = append(m.calls, "uninstall")
	return nil
}

func (m *fakeServiceManager) Start(context.Context) error {
	m.calls = append(m.calls, "start")
	return nil
}

func (m *fakeServiceManager) Stop(context.Context) error {
	m.calls = append(m.calls, "stop")
	return nil
}

func (m *fakeServiceManager) Status(context.Context) (userservice.Status, error) {
	m.calls = append(m.calls, "status")
	return m.status, nil
}

func cliTestPaths(t *testing.T) apppaths.Paths {
	t.Helper()
	base := t.TempDir()
	return apppaths.Paths{
		StateDB:      filepath.Join(base, "state", "state.db"),
		RcloneConfig: filepath.Join(base, "config", "rclone.conf"),
		CacheDir:     filepath.Join(base, "cache"),
		RuntimeDir:   filepath.Join(base, "runtime"),
	}
}

func TestReporterSuppressesNoopCycles(t *testing.T) {
	paths := cliTestPaths(t)
	var stdout, stderr bytes.Buffer
	app := New(paths, &stdout, &stderr)
	app.reportRootEvent(daemon.RootEvent{RootID: 1, Component: "cycle", HasResult: true})
	app.reportRootEvent(daemon.RootEvent{RootID: 1, Component: "cycle", HasResult: true, Result: syncer.CycleResult{Initialized: true}})
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("noop cycle produced output: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	app.reportRootEvent(daemon.RootEvent{RootID: 1, Component: "cycle", HasResult: true, Result: syncer.CycleResult{Applied: 1}})
	if !strings.Contains(stdout.String(), "applied=1") {
		t.Fatalf("meaningful cycle output = %q", stdout.String())
	}
}
