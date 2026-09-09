package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
	"github.com/rijuyuezhu/pkudisk-sync/internal/reconcile"
	"github.com/rijuyuezhu/pkudisk-sync/internal/store"
	"github.com/rijuyuezhu/pkudisk-sync/internal/syncer"
)

func TestIsContextTerminationRequiresMatchingEndedContext(t *testing.T) {
	if isContextTermination(context.Background(), context.Canceled) {
		t.Fatal("live context treated a cancellation error as expected termination")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !isContextTermination(ctx, errors.Join(errors.New("wrapped state error"), context.Canceled)) {
		t.Fatal("wrapped cancellation from an ended context was not recognized")
	}
	if isContextTermination(ctx, errors.New("sqlite corruption")) {
		t.Fatal("unrelated state error was hidden by context termination")
	}
}

func TestRunnerCoalescesWatcherHintsWithoutOverlappingCycles(t *testing.T) {
	state := openDaemonTestStore(t)
	root := daemonTestRoot(t, "root-1", "Personal/Data")
	root.PollIntervalSeconds = 0
	stored, err := SetupRoot(context.Background(), state, root)
	if err != nil {
		t.Fatal(err)
	}

	watchers := newFakeWatcherFactory()
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondDone := make(chan struct{})
	var calls atomic.Int32
	var active atomic.Int32
	var maxActive atomic.Int32
	cycle := func(context.Context, int64, *store.Store, syncer.DataPlane, reconcile.DeletePolicy) (syncer.CycleResult, error) {
		call := calls.Add(1)
		nowActive := active.Add(1)
		for {
			old := maxActive.Load()
			if nowActive <= old || maxActive.CompareAndSwap(old, nowActive) {
				break
			}
		}
		defer active.Add(-1)
		if call == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		if call == 2 {
			close(secondDone)
		}
		return syncer.CycleResult{Passes: 1}, nil
	}
	runner := testRunner(t, state, watchers.new, cycle)
	ctx, cancel := context.WithCancel(context.Background())
	done := runRunner(t, ctx, runner)

	waitChannel(t, firstStarted, "first cycle did not start")
	watch := watchers.wait(t, stored.LocalRoot)
	watch.signal()
	watch.signal()
	watch.signal()
	close(releaseFirst)
	waitChannel(t, secondDone, "coalesced follow-up cycle did not run")
	time.Sleep(40 * time.Millisecond)
	if got := calls.Load(); got != 2 {
		t.Fatalf("cycle calls = %d, want exactly 2", got)
	}
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("maximum concurrent cycles = %d, want 1", got)
	}
	cancel()
	waitRunner(t, done)
}

func TestRunnerObservesPauseAndResumeWithoutRestart(t *testing.T) {
	state := openDaemonTestStore(t)
	root := daemonTestRoot(t, "root-1", "Personal/Data")
	root.Enabled = false
	root.PollIntervalSeconds = 0
	stored, err := SetupRoot(context.Background(), state, root)
	if err != nil {
		t.Fatal(err)
	}

	watchers := newFakeWatcherFactory()
	var calls atomic.Int32
	cycle := func(context.Context, int64, *store.Store, syncer.DataPlane, reconcile.DeletePolicy) (syncer.CycleResult, error) {
		calls.Add(1)
		return syncer.CycleResult{}, nil
	}
	runner := testRunner(t, state, watchers.new, cycle)
	ctx, cancel := context.WithCancel(context.Background())
	done := runRunner(t, ctx, runner)
	defer func() {
		cancel()
		waitRunner(t, done)
	}()

	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != 0 {
		t.Fatalf("disabled root ran %d cycles", got)
	}
	if err := state.SetSyncRootEnabled(context.Background(), stored.ID, true); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return calls.Load() == 1 }, "resumed root did not run")

	if err := state.SetSyncRootEnabled(context.Background(), stored.ID, false); err != nil {
		t.Fatal(err)
	}
	watchers.wait(t, stored.LocalRoot).signal()
	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("paused root reacted to watcher hint; cycle calls = %d", got)
	}
	if err := state.SetSyncRootEnabled(context.Background(), stored.ID, true); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return calls.Load() == 2 }, "second resume did not run")
}

func TestRunnerPauseCancelsActiveCycleAndResumeWaitsForWorkerExit(t *testing.T) {
	state := openDaemonTestStore(t)
	root := daemonTestRoot(t, "pause-active-root", "Personal/PauseActive")
	root.PollIntervalSeconds = 0
	stored, err := SetupRoot(context.Background(), state, root)
	if err != nil {
		t.Fatal(err)
	}

	watchers := newFakeWatcherFactory()
	firstStarted := make(chan struct{})
	firstCanceled := make(chan struct{})
	releaseCanceledWorker := make(chan struct{})
	secondStarted := make(chan struct{})
	var calls atomic.Int32
	var active atomic.Int32
	var maxActive atomic.Int32
	cycle := func(ctx context.Context, _ int64, _ *store.Store, _ syncer.DataPlane, _ reconcile.DeletePolicy) (syncer.CycleResult, error) {
		call := calls.Add(1)
		nowActive := active.Add(1)
		for {
			old := maxActive.Load()
			if nowActive <= old || maxActive.CompareAndSwap(old, nowActive) {
				break
			}
		}
		defer active.Add(-1)
		switch call {
		case 1:
			close(firstStarted)
			<-ctx.Done()
			close(firstCanceled)
			// Deliberately delay unwinding after cancellation. A correct runner
			// must not create the resumed worker until this goroutine is gone.
			<-releaseCanceledWorker
			return syncer.CycleResult{}, ctx.Err()
		case 2:
			close(secondStarted)
		}
		return syncer.CycleResult{}, nil
	}
	runner := testRunner(t, state, watchers.new, cycle)
	ctx, cancel := context.WithCancel(context.Background())
	done := runRunner(t, ctx, runner)
	defer func() {
		cancel()
		select {
		case <-releaseCanceledWorker:
		default:
			close(releaseCanceledWorker)
		}
		waitRunner(t, done)
	}()

	waitChannel(t, firstStarted, "initial cycle did not start")
	if err := state.SetSyncRootEnabled(context.Background(), stored.ID, false); err != nil {
		t.Fatal(err)
	}
	waitChannel(t, firstCanceled, "pause did not cancel active cycle")

	if err := state.SetSyncRootEnabled(context.Background(), stored.ID, true); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("resume started another cycle before canceled worker exited: calls=%d", got)
	}
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("maximum concurrent cycles before worker exit = %d, want 1", got)
	}

	close(releaseCanceledWorker)
	waitChannel(t, secondStarted, "resume did not start a fresh cycle after canceled worker exited")
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("maximum concurrent cycles = %d, want 1", got)
	}
}

func TestRunnerHealthCheckNoticesQueuedDurableOperation(t *testing.T) {
	state := openDaemonTestStore(t)
	root := daemonTestRoot(t, "queued-op-root", "Personal/Queued")
	root.PollIntervalSeconds = 0
	stored, err := SetupRoot(context.Background(), state, root)
	if err != nil {
		t.Fatal(err)
	}

	watchers := newFakeWatcherFactory()
	var calls atomic.Int32
	cycle := func(ctx context.Context, rootID int64, state *store.Store, _ syncer.DataPlane, _ reconcile.DeletePolicy) (syncer.CycleResult, error) {
		call := calls.Add(1)
		if call > 1 {
			operations, err := state.ListOperations(ctx, rootID)
			if err != nil {
				return syncer.CycleResult{}, err
			}
			for _, operation := range operations {
				if err := state.DeleteOperation(ctx, operation.ID); err != nil {
					return syncer.CycleResult{}, err
				}
			}
		}
		return syncer.CycleResult{}, nil
	}
	runner := testRunner(t, state, watchers.new, cycle)
	ctx, cancel := context.WithCancel(context.Background())
	done := runRunner(t, ctx, runner)
	defer func() {
		cancel()
		waitRunner(t, done)
	}()
	waitFor(t, func() bool { return calls.Load() == 1 }, "initial root cycle did not run")

	if _, err := state.CreateOperation(context.Background(), domain.Operation{
		SyncRootID:     stored.ID,
		Kind:           domain.OperationEnsureRemote,
		EntryKind:      domain.KindFile,
		SrcPath:        "queued.txt",
		ExpectedLocal:  domain.LocalFingerprint{Present: true, Kind: domain.KindFile, Size: 1, MtimeNS: 1},
		ExpectedRemote: domain.RemoteExpectation{Absent: true},
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return calls.Load() >= 2 }, "queued durable operation did not trigger a cycle")
	time.Sleep(40 * time.Millisecond)
	if got := calls.Load(); got != 2 {
		t.Fatalf("queued operation caused %d cycles, want exactly 2", got)
	}
	if _, err := state.CreateOperation(context.Background(), domain.Operation{
		SyncRootID:     stored.ID,
		Kind:           domain.OperationEnsureRemote,
		EntryKind:      domain.KindFile,
		SrcPath:        "blocked.txt",
		ExpectedLocal:  domain.LocalFingerprint{Present: true, Kind: domain.KindFile, Size: 1, MtimeNS: 1},
		ExpectedRemote: domain.RemoteExpectation{Absent: true},
		Phase:          domain.OperationBlocked,
		Attempts:       1,
		LastError:      "needs attention",
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != 2 {
		t.Fatalf("blocked operation triggered health-loop cycles: %d", got)
	}
}

func TestRunnerDiscoversRootAddedAfterStartup(t *testing.T) {
	state := openDaemonTestStore(t)
	watchers := newFakeWatcherFactory()
	var calls atomic.Int32
	cycle := func(context.Context, int64, *store.Store, syncer.DataPlane, reconcile.DeletePolicy) (syncer.CycleResult, error) {
		calls.Add(1)
		return syncer.CycleResult{}, nil
	}
	runner := testRunner(t, state, watchers.new, cycle)
	ctx, cancel := context.WithCancel(context.Background())
	done := runRunner(t, ctx, runner)
	defer func() {
		cancel()
		waitRunner(t, done)
	}()

	time.Sleep(30 * time.Millisecond)
	if got := calls.Load(); got != 0 {
		t.Fatalf("empty daemon ran %d cycles", got)
	}
	root := daemonTestRoot(t, "later-root", "Personal/Later")
	root.PollIntervalSeconds = 0
	if _, err := SetupRoot(context.Background(), state, root); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return calls.Load() == 1 }, "new root was not discovered")
}

func TestRunnerFallsBackToPollingWhenWatcherSetupFails(t *testing.T) {
	state := openDaemonTestStore(t)
	root := daemonTestRoot(t, "root-1", "Personal/Data")
	root.PollIntervalSeconds = 0
	if _, err := SetupRoot(context.Background(), state, root); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	cycle := func(context.Context, int64, *store.Store, syncer.DataPlane, reconcile.DeletePolicy) (syncer.CycleResult, error) {
		calls.Add(1)
		return syncer.CycleResult{}, nil
	}
	deps := testRunnerDeps(nil, cycle)
	deps.watch = func(string) (rootWatcher, error) { return nil, errors.New("watch unavailable") }
	deps.defaultPollInterval = 25 * time.Millisecond
	runner, err := newRunnerWithDeps(state, reconcile.DeletePolicy{MaxCount: 100}, deps)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := runRunner(t, ctx, runner)
	waitFor(t, func() bool { return calls.Load() >= 2 }, "watcher failure prevented periodic polling")
	cancel()
	waitRunner(t, done)
}

func testRunner(t *testing.T, state *store.Store, watch watcherFactory, cycle cycleFunc) *Runner {
	t.Helper()
	deps := testRunnerDeps(watch, cycle)
	runner, err := newRunnerWithDeps(state, reconcile.DeletePolicy{MaxCount: 100}, deps)
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func testRunnerDeps(watch watcherFactory, cycle cycleFunc) runnerDeps {
	if watch == nil {
		watch = func(string) (rootWatcher, error) { return newFakeWatcher(), nil }
	}
	return runnerDeps{
		dataPlane: func(context.Context, domain.SyncRoot) (syncer.DataPlane, error) {
			return stubDataPlane{}, nil
		},
		watch:               watch,
		cycle:               cycle,
		rootRefreshInterval: 10 * time.Millisecond,
		healthInterval:      10 * time.Millisecond,
		defaultPollInterval: 10 * time.Second,
	}
}

func runRunner(t *testing.T, ctx context.Context, runner *Runner) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	return done
}

func waitRunner(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runner returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop")
	}
}

func waitFor(t *testing.T, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(message)
}

func waitChannel(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal(message)
	}
}

type fakeWatcherFactory struct {
	mu       sync.Mutex
	watchers map[string]*fakeWatcher
}

func newFakeWatcherFactory() *fakeWatcherFactory {
	return &fakeWatcherFactory{watchers: make(map[string]*fakeWatcher)}
}

func (f *fakeWatcherFactory) new(root string) (rootWatcher, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := newFakeWatcher()
	f.watchers[root] = w
	return w, nil
}

func (f *fakeWatcherFactory) wait(t *testing.T, root string) *fakeWatcher {
	t.Helper()
	var w *fakeWatcher
	waitFor(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		w = f.watchers[root]
		return w != nil
	}, "watcher was not created")
	return w
}

type fakeWatcher struct {
	hints     chan struct{}
	errs      chan error
	done      chan struct{}
	closeOnce sync.Once
}

func newFakeWatcher() *fakeWatcher {
	return &fakeWatcher{
		hints: make(chan struct{}, 1),
		errs:  make(chan error, 1),
		done:  make(chan struct{}),
	}
}

func (w *fakeWatcher) Hints() <-chan struct{} { return w.hints }
func (w *fakeWatcher) Errors() <-chan error   { return w.errs }
func (w *fakeWatcher) Done() <-chan struct{}  { return w.done }
func (w *fakeWatcher) Close() error {
	w.closeOnce.Do(func() {
		close(w.done)
	})
	return nil
}
func (w *fakeWatcher) signal() {
	select {
	case w.hints <- struct{}{}:
	default:
	}
}

type stubDataPlane struct{}

func (stubDataPlane) ScanLocal(context.Context, []domain.Operation, []string) (map[string]domain.LocalFingerprint, []string, map[string]domain.FollowedPhysicalClaim, map[string]domain.CopyProjectionEvidence, error) {
	return nil, nil, nil, nil, nil
}
func (stubDataPlane) ScanRemote(context.Context) (map[string]domain.RemoteFingerprint, bool, error) {
	return nil, true, nil
}
func (stubDataPlane) ObserveLocalEntry(context.Context, string) (domain.LocalFingerprint, error) {
	return domain.LocalFingerprint{}, nil
}
func (stubDataPlane) ObservePinnedLocalEntry(context.Context, domain.Operation) (domain.LocalFingerprint, bool, error) {
	return domain.LocalFingerprint{}, true, nil
}
func (stubDataPlane) ObserveRemoteEntry(context.Context, string) (domain.RemoteFingerprint, error) {
	return domain.RemoteFingerprint{}, nil
}
func (stubDataPlane) ResolveLocalMutationTarget(context.Context, string, domain.LocalFingerprint, domain.EntryKind, []string) (domain.LocalMutationTarget, error) {
	return domain.LocalMutationTarget{Path: filepath.Join(os.TempDir(), "pkudisk-sync-daemon-stub"), AnchorIdentity: "stub", Authority: domain.LocalMutationLexical}, nil
}
func (stubDataPlane) PinnedLocalPreconditionHolds(context.Context, domain.Operation) (bool, error) {
	return true, nil
}
func (stubDataPlane) LocalRecoveryArtifact(context.Context, domain.Operation) (string, bool, error) {
	return "", false, nil
}
func (stubDataPlane) CleanupLocalDownloadArtifact(context.Context, domain.Operation) error {
	return nil
}
func (stubDataPlane) CleanupLocalRecoveryArtifact(context.Context, domain.Operation) error {
	return nil
}
func (stubDataPlane) CompareFileContent(context.Context, string, domain.LocalFingerprint, domain.RemoteExpectation) (bool, error) {
	return false, nil
}
func (stubDataPlane) ComparePinnedFileContent(context.Context, domain.Operation, domain.RemoteExpectation) (bool, error) {
	return false, nil
}
func (stubDataPlane) Upload(context.Context, string, domain.LocalFingerprint, domain.RemoteExpectation) (domain.RemoteFingerprint, error) {
	return domain.RemoteFingerprint{}, nil
}
func (stubDataPlane) EnsureRemoteDir(context.Context, string, domain.RemoteExpectation) error {
	return nil
}
func (stubDataPlane) EnsureLocalFile(context.Context, domain.Operation, []string, func() error) (domain.LocalFingerprint, error) {
	return domain.LocalFingerprint{}, nil
}
func (stubDataPlane) EnsureLocalDir(context.Context, domain.Operation, []string, func() error) error {
	return nil
}
func (stubDataPlane) DeleteRemoteFile(context.Context, domain.RemoteExpectation) error { return nil }
func (stubDataPlane) DeleteRemoteDir(context.Context, domain.RemoteExpectation) error  { return nil }
func (stubDataPlane) DeleteLocal(context.Context, domain.Operation, []string, func() error) error {
	return nil
}
