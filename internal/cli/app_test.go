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
	"github.com/rijuyuezhu/pkudisk-sync/internal/reconcile"
	"github.com/rijuyuezhu/pkudisk-sync/internal/rootmarker"
	"github.com/rijuyuezhu/pkudisk-sync/internal/store"
	"github.com/rijuyuezhu/pkudisk-sync/internal/syncer"
	"github.com/rijuyuezhu/pkudisk-sync/internal/userservice"
)

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
	defer held.Close()

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

func TestServiceInstallRejectsPathOverridesBeforeProvider(t *testing.T) {
	t.Setenv("PKUDISK_SYNC_STATE_DB", filepath.Join(t.TempDir(), "state.db"))
	app := New(cliTestPaths(t), &bytes.Buffer{}, &bytes.Buffer{})
	app.executablePath = func() (string, error) {
		t.Fatal("service provider resolution ran before override rejection")
		return "", nil
	}
	err := app.Run(context.Background(), []string{"service", "install"})
	if err == nil || !strings.Contains(err.Error(), "requires default") {
		t.Fatalf("service install error = %v", err)
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
