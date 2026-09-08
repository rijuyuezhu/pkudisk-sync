package daemon

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
	"github.com/rijuyuezhu/pkudisk-sync/internal/executor"
	"github.com/rijuyuezhu/pkudisk-sync/internal/reconcile"
	"github.com/rijuyuezhu/pkudisk-sync/internal/store"
	"github.com/rijuyuezhu/pkudisk-sync/internal/syncer"
	"github.com/rijuyuezhu/pkudisk-sync/internal/watcher"
)

const (
	defaultPollInterval        = 60 * time.Second
	defaultRootRefreshInterval = 2 * time.Second
	defaultHealthInterval      = 1 * time.Second
)

// RootEvent reports daemon progress without making synchronization semantics
// depend on logs. Reporter callbacks may be invoked concurrently for different
// roots and must therefore be concurrency-safe.
type RootEvent struct {
	RootID    int64
	Component string
	Result    syncer.CycleResult
	HasResult bool
	Err       error
}

type Reporter func(RootEvent)

type Runner struct {
	state        *store.Store
	deletePolicy reconcile.DeletePolicy
	deps         runnerDeps
}

type rootWatcher interface {
	Hints() <-chan struct{}
	Errors() <-chan error
	Done() <-chan struct{}
	Close() error
}

type dataPlaneFactory func(context.Context, domain.SyncRoot) (syncer.DataPlane, error)
type watcherFactory func(string) (rootWatcher, error)
type cycleFunc func(context.Context, int64, *store.Store, syncer.DataPlane, reconcile.DeletePolicy) (syncer.CycleResult, error)

type runnerDeps struct {
	dataPlane           dataPlaneFactory
	watch               watcherFactory
	cycle               cycleFunc
	rootRefreshInterval time.Duration
	healthInterval      time.Duration
	defaultPollInterval time.Duration
	report              Reporter
}

// NewRunner constructs the continuous multi-root daemon core. The embedded
// rclone configuration must already be installed through executor.InstallRcloneConfig.
func NewRunner(state *store.Store, deletePolicy reconcile.DeletePolicy, report Reporter) (*Runner, error) {
	if state == nil {
		return nil, fmt.Errorf("state store must not be nil")
	}
	deps := runnerDeps{
		dataPlane: func(ctx context.Context, root domain.SyncRoot) (syncer.DataPlane, error) {
			remoteConfig, err := executor.RemoteConfig(root.RemoteName)
			if err != nil {
				return nil, err
			}
			return executor.NewRoot(ctx, root, remoteConfig)
		},
		watch: func(root string) (rootWatcher, error) {
			return watcher.New(root)
		},
		cycle:               syncer.RunRootCycle,
		rootRefreshInterval: defaultRootRefreshInterval,
		healthInterval:      defaultHealthInterval,
		defaultPollInterval: defaultPollInterval,
		report:              report,
	}
	return newRunnerWithDeps(state, deletePolicy, deps)
}

func newRunnerWithDeps(state *store.Store, deletePolicy reconcile.DeletePolicy, deps runnerDeps) (*Runner, error) {
	if state == nil {
		return nil, fmt.Errorf("state store must not be nil")
	}
	if deps.dataPlane == nil || deps.watch == nil || deps.cycle == nil {
		return nil, fmt.Errorf("runner dependencies must not be nil")
	}
	if deps.rootRefreshInterval <= 0 || deps.healthInterval <= 0 || deps.defaultPollInterval <= 0 {
		return nil, fmt.Errorf("runner intervals must be positive")
	}
	return &Runner{state: state, deletePolicy: deletePolicy, deps: deps}, nil
}

// Run continuously reconciles every configured sync root. Root additions are
// discovered without daemon restart. One worker goroutine owns each root, so a
// root can never have overlapping reconciliation cycles.
func (r *Runner) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workers := make(map[int64]context.CancelFunc)
	var wg sync.WaitGroup
	stopWorkers := func() {
		for _, workerCancel := range workers {
			workerCancel()
		}
		wg.Wait()
	}
	syncWorkers := func() error {
		roots, err := r.state.ListSyncRoots(ctx)
		if err != nil {
			return fmt.Errorf("list sync roots: %w", err)
		}
		seen := make(map[int64]struct{}, len(roots))
		for _, root := range roots {
			seen[root.ID] = struct{}{}
			if _, exists := workers[root.ID]; exists {
				continue
			}
			workerCtx, workerCancel := context.WithCancel(ctx)
			workers[root.ID] = workerCancel
			wg.Add(1)
			go func(root domain.SyncRoot) {
				defer wg.Done()
				r.runRoot(workerCtx, root)
			}(root)
		}
		for id, workerCancel := range workers {
			if _, exists := seen[id]; exists {
				continue
			}
			workerCancel()
			delete(workers, id)
		}
		return nil
	}

	if err := syncWorkers(); err != nil {
		if ctx.Err() != nil {
			stopWorkers()
			return nil
		}
		return err
	}
	refresh := time.NewTicker(r.deps.rootRefreshInterval)
	defer refresh.Stop()

	for {
		select {
		case <-ctx.Done():
			stopWorkers()
			return nil
		case <-refresh.C:
			if err := syncWorkers(); err != nil {
				stopWorkers()
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}

func (r *Runner) runRoot(ctx context.Context, initial domain.SyncRoot) {
	pollInterval := r.deps.defaultPollInterval
	if initial.PollIntervalSeconds > 0 {
		pollInterval = time.Duration(initial.PollIntervalSeconds) * time.Second
	}
	poll := time.NewTicker(pollInterval)
	defer poll.Stop()
	health := time.NewTicker(r.deps.healthInterval)
	defer health.Stop()

	var (
		data        syncer.DataPlane
		wat         rootWatcher
		hints       <-chan struct{}
		watchErrors <-chan error
		watchDone   <-chan struct{}
	)
	closeWatcher := func() {
		if wat != nil {
			if err := wat.Close(); err != nil {
				r.report(initial.ID, "watcher-close", syncer.CycleResult{}, false, err)
			}
		}
		wat = nil
		hints = nil
		watchErrors = nil
		watchDone = nil
	}
	defer closeWatcher()

	lastEnabled := initial.Enabled
	pending := initial.Enabled

	for {
		if err := ctx.Err(); err != nil {
			return
		}
		if pending {
			root, ok, err := r.state.GetSyncRoot(ctx, initial.ID)
			if err != nil {
				r.report(initial.ID, "state", syncer.CycleResult{}, false, err)
				pending = false
			} else if !ok {
				return
			} else if !root.Enabled {
				lastEnabled = false
				pending = false
			} else {
				lastEnabled = true
				if data == nil {
					data, err = r.deps.dataPlane(ctx, root)
					if err != nil {
						r.report(root.ID, "executor", syncer.CycleResult{}, false, err)
						data = nil
						pending = false
					}
				}
				if data != nil && wat == nil {
					newWatcher, watchErr := r.deps.watch(root.LocalRoot)
					if watchErr != nil {
						r.report(root.ID, "watcher", syncer.CycleResult{}, false, watchErr)
					} else {
						wat = newWatcher
						hints = wat.Hints()
						watchErrors = wat.Errors()
						watchDone = wat.Done()
					}
				}
				if data != nil {
					result, cycleErr := r.deps.cycle(ctx, root.ID, r.state, data, r.deletePolicy)
					r.report(root.ID, "cycle", result, true, cycleErr)
					pending = false
				}
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-poll.C:
			pending = true
		case <-health.C:
			root, ok, err := r.state.GetSyncRoot(ctx, initial.ID)
			if err != nil {
				r.report(initial.ID, "state", syncer.CycleResult{}, false, err)
				continue
			}
			if !ok {
				return
			}
			if root.Enabled && !lastEnabled {
				pending = true
			} else if root.Enabled && !pending {
				operations, listErr := r.state.ListOperations(ctx, root.ID)
				if listErr != nil {
					r.report(initial.ID, "state", syncer.CycleResult{}, false, listErr)
				} else {
					for _, operation := range operations {
						if operation.Phase == domain.OperationPlanned {
							pending = true
							break
						}
					}
				}
			}
			lastEnabled = root.Enabled
		case _, ok := <-hints:
			if !ok {
				hints = nil
			} else {
				pending = true
			}
		case err, ok := <-watchErrors:
			if !ok {
				watchErrors = nil
			} else {
				r.report(initial.ID, "watcher", syncer.CycleResult{}, false, err)
				pending = true
			}
		case _, ok := <-watchDone:
			if !ok {
				watchDone = nil
				wat = nil
				hints = nil
				watchErrors = nil
				pending = true
			}
		}
	}
}

func (r *Runner) report(rootID int64, component string, result syncer.CycleResult, hasResult bool, err error) {
	if r.deps.report == nil {
		return
	}
	r.deps.report(RootEvent{
		RootID:    rootID,
		Component: component,
		Result:    result,
		HasResult: hasResult,
		Err:       err,
	})
}
