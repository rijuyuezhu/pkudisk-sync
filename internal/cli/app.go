package cli

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/rijuyuezhu/pkudisk-sync/internal/apppaths"
	"github.com/rijuyuezhu/pkudisk-sync/internal/buildinfo"
	"github.com/rijuyuezhu/pkudisk-sync/internal/daemon"
	"github.com/rijuyuezhu/pkudisk-sync/internal/daemonlock"
	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
	"github.com/rijuyuezhu/pkudisk-sync/internal/executor"
	"github.com/rijuyuezhu/pkudisk-sync/internal/reconcile"
	"github.com/rijuyuezhu/pkudisk-sync/internal/store"
	"github.com/rijuyuezhu/pkudisk-sync/internal/syncer"
	"github.com/rijuyuezhu/pkudisk-sync/internal/userservice"
)

const (
	defaultMaxDeleteCount    = 100
	defaultMaxDeleteFraction = 0
)

type daemonRunner interface {
	Run(context.Context) error
}

type Application struct {
	paths  apppaths.Paths
	stdout io.Writer
	stderr io.Writer

	newUUID             func() (string, error)
	installRcloneConfig func(string) error
	configureRemote     func(context.Context, string, string) error
	validateRemote      func(string) error
	newRunner           func(*store.Store, reconcile.DeletePolicy, daemon.Reporter) (daemonRunner, error)
	executablePath      func() (string, error)
	newService          func(string) (userservice.Manager, error)
	servicePaths        func() (apppaths.Paths, error)
}

func New(paths apppaths.Paths, stdout, stderr io.Writer) *Application {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	return &Application{
		paths:               paths,
		stdout:              stdout,
		stderr:              stderr,
		newUUID:             randomUUID,
		installRcloneConfig: executor.InstallRcloneConfig,
		configureRemote:     executor.ConfigureRemote,
		validateRemote: func(name string) error {
			_, err := executor.RemoteConfig(name)
			return err
		},
		newRunner: func(state *store.Store, policy reconcile.DeletePolicy, report daemon.Reporter) (daemonRunner, error) {
			return daemon.NewRunner(state, policy, report)
		},
		executablePath: os.Executable,
		newService:     userservice.New,
		servicePaths:   apppaths.ServiceDefault,
	}
}

func (a *Application) Run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		a.printUsage()
		return nil
	}
	switch args[0] {
	case "help", "-h", "--help":
		a.printUsage()
		return nil
	case "paths":
		return a.runPaths(args[1:])
	case "version":
		return a.runVersion(args[1:])
	case "remote":
		return a.runRemote(ctx, args[1:])
	case "status":
		return a.runStatus(ctx, args[1:])
	case "conflict":
		return a.runConflict(ctx, args[1:])
	case "root":
		return a.runRoot(ctx, args[1:])
	case "daemon":
		return a.runDaemon(ctx, args[1:])
	case "service":
		return a.runService(ctx, args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func (a *Application) printUsage() {
	fmt.Fprintln(a.stdout, "Usage: pkudisk-sync <command> [options]")
	fmt.Fprintln(a.stdout)
	fmt.Fprintln(a.stdout, "Commands:")
	fmt.Fprintln(a.stdout, "  paths                              Show app-owned state/config/cache/runtime paths")
	fmt.Fprintln(a.stdout, "  version                            Show build version and provenance")
	fmt.Fprintln(a.stdout, "  remote configure                   Configure or re-authenticate the PKU Disk account")
	fmt.Fprintln(a.stdout, "  status                             Summarize roots, operations, and conflicts")
	fmt.Fprintln(a.stdout, "  conflict list [--root ID]          List unresolved conflicts")
	fmt.Fprintln(a.stdout, "  conflict resolve ID --keep-local   Queue exact-state resolution using local data")
	fmt.Fprintln(a.stdout, "  conflict resolve ID --keep-remote  Queue exact-state resolution using remote data")
	fmt.Fprintln(a.stdout, "  root add --local PATH --remote pkudisk:P Add one selected directory pair")
	fmt.Fprintln(a.stdout, "  root list                          List selected directory pairs")
	fmt.Fprintln(a.stdout, "  root pause ID                      Pause one selected pair")
	fmt.Fprintln(a.stdout, "  root resume ID                     Resume one selected pair")
	fmt.Fprintln(a.stdout, "  root remove ID                     Unregister one paused pair without deleting data")
	fmt.Fprintln(a.stdout, "  daemon                             Run the foreground sync daemon")
	fmt.Fprintln(a.stdout, "  service install|start|stop|status  Manage the current user's background daemon")
	fmt.Fprintln(a.stdout, "  service uninstall                  Remove the current user's background daemon")
}

func (a *Application) runPaths(args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("paths takes no arguments")
	}
	fmt.Fprintf(a.stdout, "state_db\t%s\n", a.paths.StateDB)
	fmt.Fprintf(a.stdout, "rclone_config\t%s\n", a.paths.RcloneConfig)
	fmt.Fprintf(a.stdout, "cache_dir\t%s\n", a.paths.CacheDir)
	fmt.Fprintf(a.stdout, "runtime_dir\t%s\n", a.paths.RuntimeDir)
	return nil
}

func (a *Application) runVersion(args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("version takes no arguments")
	}
	fmt.Fprintf(a.stdout, "pkudisk-sync %s\n", buildinfo.Version)
	fmt.Fprintf(a.stdout, "commit\t%s\n", buildinfo.Commit)
	fmt.Fprintf(a.stdout, "built\t%s\n", buildinfo.BuildDate)
	return nil
}

func (a *Application) runRemote(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "configure" {
		return fmt.Errorf("remote requires: configure")
	}
	if len(args) != 1 {
		return fmt.Errorf("remote configure takes no remote name; v0.1 uses the single app-owned remote %q", domain.AppRemoteName)
	}
	if err := a.paths.PrepareRuntime(); err != nil {
		return err
	}
	lease, err := daemonlock.Acquire(a.paths.RuntimeDir)
	if err != nil {
		return fmt.Errorf("remote configure requires the foreground daemon and user service to be stopped: %w", err)
	}
	defer lease.Close()
	if err := a.paths.PrepareConfig(); err != nil {
		return err
	}
	if err := a.configureRemote(ctx, a.paths.RcloneConfig, domain.AppRemoteName); err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "configured PKU Disk remote %s in %s\n", domain.AppRemoteName, a.paths.RcloneConfig)
	return nil
}

func (a *Application) runStatus(ctx context.Context, args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("status takes no arguments")
	}
	state, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer state.Close()
	roots, err := state.ListSyncRoots(ctx)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(a.stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tSTATE\tINIT\tPLANNED\tRUNNING\tRECOVERING\tBLOCKED\tCONFLICTS\tLOCAL\tREMOTE")
	for _, root := range roots {
		operations, err := state.ListOperations(ctx, root.ID)
		if err != nil {
			return err
		}
		conflicts, err := state.ListConflicts(ctx, root.ID, true)
		if err != nil {
			return err
		}
		counts := map[domain.OperationPhase]int{}
		for _, operation := range operations {
			counts[operation.Phase]++
		}
		rootState := "paused"
		if root.Enabled {
			rootState = "enabled"
		}
		initialized := "no"
		if root.Initialized {
			initialized = "yes"
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%s\t%s:%s\n",
			root.ID,
			rootState,
			initialized,
			counts[domain.OperationPlanned],
			counts[domain.OperationRunning],
			counts[domain.OperationRecovering],
			counts[domain.OperationBlocked],
			len(conflicts),
			root.LocalRoot,
			root.RemoteName,
			root.RemoteRoot,
		)
	}
	return w.Flush()
}

func (a *Application) runConflict(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("conflict requires one of: list, resolve")
	}
	switch args[0] {
	case "list":
		return a.runConflictList(ctx, args[1:])
	case "resolve":
		return a.runConflictResolve(ctx, args[1:])
	default:
		return fmt.Errorf("unknown conflict command %q", args[0])
	}
}

func (a *Application) runConflictList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("conflict list", flag.ContinueOnError)
	fs.SetOutput(a.stderr)
	rootID := fs.Int64("root", 0, "limit to one sync root ID")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("conflict list takes no positional arguments")
	}
	if *rootID < 0 {
		return fmt.Errorf("--root must be a positive root ID")
	}

	state, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer state.Close()
	roots, err := state.ListSyncRoots(ctx)
	if err != nil {
		return err
	}
	if *rootID > 0 {
		found := false
		for _, root := range roots {
			if root.ID == *rootID {
				roots = []domain.SyncRoot{root}
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("sync root %d not found", *rootID)
		}
	}
	w := tabwriter.NewWriter(a.stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tROOT\tKIND\tPATH\tLOCAL\tREMOTE\tCREATED")
	for _, root := range roots {
		conflicts, err := state.ListConflicts(ctx, root.ID, true)
		if err != nil {
			return err
		}
		for _, conflict := range conflicts {
			fmt.Fprintf(w, "%d\t%d\t%s\t%s\t%s\t%s\t%s\n",
				conflict.ID,
				root.ID,
				conflict.Kind,
				conflict.RelPath,
				describeLocalConflictState(conflict.Local),
				describeRemoteConflictState(conflict.Remote),
				conflict.CreatedAt.Local().Format(time.RFC3339),
			)
		}
	}
	return w.Flush()
}

func (a *Application) runConflictResolve(ctx context.Context, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("conflict resolve requires a conflict ID and exactly one of --keep-local or --keep-remote")
	}
	var (
		id         int64
		resolution syncer.ConflictResolution
	)
	for _, arg := range args {
		switch arg {
		case "--keep-local":
			if resolution != "" {
				return fmt.Errorf("conflict resolve requires exactly one resolution choice")
			}
			resolution = syncer.ConflictKeepLocal
		case "--keep-remote":
			if resolution != "" {
				return fmt.Errorf("conflict resolve requires exactly one resolution choice")
			}
			resolution = syncer.ConflictKeepRemote
		default:
			parsed, err := strconv.ParseInt(arg, 10, 64)
			if err != nil || parsed <= 0 || id != 0 {
				return fmt.Errorf("invalid conflict resolve argument %q", arg)
			}
			id = parsed
		}
	}
	if id == 0 || resolution == "" {
		return fmt.Errorf("conflict resolve requires a conflict ID and exactly one of --keep-local or --keep-remote")
	}

	state, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer state.Close()
	conflict, ok, err := state.GetConflict(ctx, id)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("conflict %d not found", id)
	}
	if conflict.Resolved {
		return fmt.Errorf("conflict %d is already resolved", id)
	}
	root, ok, err := state.GetSyncRoot(ctx, conflict.SyncRootID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("sync root %d for conflict %d not found", conflict.SyncRootID, id)
	}
	operations, err := state.ListOperations(ctx, conflict.SyncRootID)
	if err != nil {
		return err
	}
	for _, operation := range operations {
		if operation.SrcPath == conflict.RelPath {
			return fmt.Errorf("conflict %d path %q already has pending operation %d (%s)", id, conflict.RelPath, operation.ID, operation.Phase)
		}
	}
	operation, err := syncer.OperationForConflictResolution(conflict, resolution)
	if err != nil {
		return err
	}
	operation, err = state.CreateOperation(ctx, operation)
	if err != nil {
		return fmt.Errorf("queue conflict %d resolution: %w", id, err)
	}
	fmt.Fprintf(a.stdout, "queued conflict %d %s as operation %d\n", id, resolution, operation.ID)
	if !root.Enabled {
		fmt.Fprintf(a.stdout, "root %d is paused; resume it to apply the queued resolution\n", root.ID)
	}
	return nil
}

func describeLocalConflictState(state domain.LocalFingerprint) string {
	if !state.Present {
		return "missing"
	}
	if state.Kind == domain.KindFile {
		return fmt.Sprintf("file:%dB", state.Size)
	}
	return string(state.Kind)
}

func describeRemoteConflictState(state domain.RemoteFingerprint) string {
	if !state.Present {
		return "missing"
	}
	if state.Kind == domain.KindFile {
		return fmt.Sprintf("file:%dB@%s", state.Size, state.Rev)
	}
	return string(state.Kind)
}

func (a *Application) runService(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("service requires exactly one of: install, uninstall, start, stop, status")
	}
	if (args[0] == "install" || args[0] == "start") && apppaths.OverridesActive() {
		return fmt.Errorf("service %s requires default pkudisk-sync paths; unset PKUDISK_SYNC_* path overrides first", args[0])
	}
	manager, err := a.serviceManager()
	if err != nil {
		return err
	}
	switch args[0] {
	case "install":
		if err := a.paths.PrepareRuntime(); err != nil {
			return err
		}
		lease, err := daemonlock.Acquire(a.paths.RuntimeDir)
		if err != nil {
			if errors.Is(err, daemonlock.ErrAlreadyRunning) {
				return fmt.Errorf("service install requires the foreground daemon and user service to be stopped: %w", err)
			}
			return fmt.Errorf("preflight service install daemon lease: %w", err)
		}
		if err := manager.Install(ctx); err != nil {
			_ = lease.Close()
			return err
		}
		if err := lease.Close(); err != nil {
			return fmt.Errorf("release service install daemon lease preflight: %w", err)
		}
		fmt.Fprintln(a.stdout, "service installed")
	case "uninstall":
		if err := manager.Uninstall(ctx); err != nil {
			return err
		}
		fmt.Fprintln(a.stdout, "service uninstalled")
	case "start":
		if err := a.paths.PrepareRuntime(); err != nil {
			return err
		}
		preflight, err := daemonlock.Acquire(a.paths.RuntimeDir)
		if err != nil {
			if errors.Is(err, daemonlock.ErrAlreadyRunning) {
				return fmt.Errorf("service start requires any foreground daemon to be stopped: %w", err)
			}
			return fmt.Errorf("preflight service start daemon lease: %w", err)
		}
		if err := preflight.Close(); err != nil {
			return fmt.Errorf("release service start daemon lease preflight: %w", err)
		}
		if err := manager.Start(ctx); err != nil {
			return err
		}
		fmt.Fprintln(a.stdout, "service started")
	case "stop":
		if err := manager.Stop(ctx); err != nil {
			return err
		}
		fmt.Fprintln(a.stdout, "service stopped")
	case "status":
		status, err := manager.Status(ctx)
		if err != nil {
			return err
		}
		fmt.Fprintln(a.stdout, status)
	default:
		return fmt.Errorf("unknown service command %q", args[0])
	}
	return nil
}

func (a *Application) serviceManager() (userservice.Manager, error) {
	executable, err := a.executablePath()
	if err != nil {
		return nil, fmt.Errorf("resolve pkudisk-sync executable: %w", err)
	}
	if !filepath.IsAbs(executable) {
		executable, err = filepath.Abs(executable)
		if err != nil {
			return nil, fmt.Errorf("make pkudisk-sync executable absolute: %w", err)
		}
	}
	manager, err := a.newService(filepath.Clean(executable))
	if err != nil {
		return nil, err
	}
	return manager, nil
}

func (a *Application) runRoot(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("root requires one of: add, list, pause, resume, remove")
	}
	switch args[0] {
	case "add":
		return a.runRootAdd(ctx, args[1:])
	case "list":
		return a.runRootList(ctx, args[1:])
	case "pause":
		return a.runRootEnabled(ctx, args[1:], false)
	case "resume":
		return a.runRootEnabled(ctx, args[1:], true)
	case "remove":
		return a.runRootRemove(ctx, args[1:])
	default:
		return fmt.Errorf("unknown root command %q", args[0])
	}
}

func (a *Application) runRootAdd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("root add", flag.ContinueOnError)
	fs.SetOutput(a.stderr)
	localArg := fs.String("local", "", "local directory")
	remoteArg := fs.String("remote", "", "PKU Disk root in remote:path form")
	poll := fs.Duration("poll", 0, "periodic repair interval; 0 uses daemon default")
	recoverOrphanMarker := fs.Bool("recover-orphan-marker", false, "replace an unowned reserved root marker left by an interrupted prior add")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("root add takes no positional arguments")
	}
	if *localArg == "" || *remoteArg == "" {
		return fmt.Errorf("root add requires --local and --remote")
	}
	if *poll < 0 || *poll%time.Second != 0 {
		return fmt.Errorf("--poll must be zero or a non-negative whole number of seconds")
	}

	localRoot, err := filepath.Abs(*localArg)
	if err != nil {
		return fmt.Errorf("resolve local root: %w", err)
	}
	localRoot = filepath.Clean(localRoot)
	info, err := os.Lstat(localRoot)
	if err != nil {
		return fmt.Errorf("stat local root %q: %w", localRoot, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("local root %q must be a real directory", localRoot)
	}

	remoteName, remoteRoot, err := parseRemoteSpec(*remoteArg)
	if err != nil {
		return err
	}
	if remoteName != domain.AppRemoteName {
		return fmt.Errorf("remote name %q is unsupported; v0.1 uses the single app-owned remote %q", remoteName, domain.AppRemoteName)
	}
	if err := a.paths.PrepareConfig(); err != nil {
		return err
	}
	if err := a.installRcloneConfig(a.paths.RcloneConfig); err != nil {
		return fmt.Errorf("install app rclone config: %w", err)
	}
	if err := a.validateRemote(remoteName); err != nil {
		return fmt.Errorf("validate PKU Disk remote %q in %q: %w", remoteName, a.paths.RcloneConfig, err)
	}

	uuid, err := a.newUUID()
	if err != nil {
		return fmt.Errorf("generate sync root UUID: %w", err)
	}
	root := domain.SyncRoot{
		UUID:                uuid,
		LocalRoot:           localRoot,
		RemoteName:          remoteName,
		RemoteRoot:          remoteRoot,
		Enabled:             true,
		PollIntervalSeconds: int64(*poll / time.Second),
	}
	if err := root.Validate(); err != nil {
		return err
	}

	state, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer state.Close()
	var stored domain.SyncRoot
	if *recoverOrphanMarker {
		stored, err = daemon.SetupRootRecoveringOrphanMarker(ctx, state, root)
	} else {
		stored, err = daemon.SetupRoot(ctx, state, root)
	}
	if err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "added root %d\t%s\t<->\t%s:%s\n", stored.ID, stored.LocalRoot, stored.RemoteName, stored.RemoteRoot)
	return nil
}

func (a *Application) runRootList(ctx context.Context, args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("root list takes no arguments")
	}
	state, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer state.Close()
	roots, err := state.ListSyncRoots(ctx)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(a.stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tSTATE\tINIT\tPOLL\tLOCAL\tREMOTE")
	for _, root := range roots {
		status := "paused"
		if root.Enabled {
			status = "enabled"
		}
		initialized := "no"
		if root.Initialized {
			initialized = "yes"
		}
		poll := "default"
		if root.PollIntervalSeconds > 0 {
			poll = (time.Duration(root.PollIntervalSeconds) * time.Second).String()
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s:%s\n", root.ID, status, initialized, poll, root.LocalRoot, root.RemoteName, root.RemoteRoot)
	}
	return w.Flush()
}

func (a *Application) runRootEnabled(ctx context.Context, args []string, enabled bool) error {
	if len(args) != 1 {
		if enabled {
			return fmt.Errorf("root resume requires exactly one root ID")
		}
		return fmt.Errorf("root pause requires exactly one root ID")
	}
	id, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil || id <= 0 {
		return fmt.Errorf("invalid sync root ID %q", args[0])
	}
	state, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer state.Close()
	if err := state.SetSyncRootEnabled(ctx, id, enabled); err != nil {
		return err
	}
	action := "paused"
	if enabled {
		action = "resumed"
	}
	fmt.Fprintf(a.stdout, "%s root %d\n", action, id)
	return nil
}

func (a *Application) runRootRemove(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("root remove requires exactly one root ID")
	}
	id, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil || id <= 0 {
		return fmt.Errorf("invalid sync root ID %q", args[0])
	}
	if err := a.paths.PrepareRuntime(); err != nil {
		return err
	}
	lease, err := daemonlock.Acquire(a.paths.RuntimeDir)
	if err != nil {
		return fmt.Errorf("root remove requires the foreground daemon and user service to be stopped: %w", err)
	}
	defer lease.Close()

	state, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer state.Close()
	removed, err := daemon.RemoveRoot(ctx, state, id)
	if err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "removed root %d\t%s\t<->\t%s:%s\t(data left unchanged)\n", removed.ID, removed.LocalRoot, removed.RemoteName, removed.RemoteRoot)
	return nil
}

func (a *Application) runDaemon(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("daemon", flag.ContinueOnError)
	fs.SetOutput(a.stderr)
	serviceMode := fs.Bool("service", false, "internal: daemon is supervised by the installed user service")
	maxDeleteCount := fs.Int("max-delete-count", defaultMaxDeleteCount, "block a cycle proposing more deletions than this; 0 disables this threshold")
	maxDeleteFraction := fs.Float64("max-delete-fraction", defaultMaxDeleteFraction, "block a cycle proposing a larger baseline deletion fraction; 0 disables this threshold")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("daemon takes no positional arguments")
	}
	policy := reconcile.DeletePolicy{MaxCount: *maxDeleteCount, MaxFraction: *maxDeleteFraction}
	if policy.MaxCount < 0 || policy.MaxFraction < 0 || policy.MaxFraction > 1 {
		return fmt.Errorf("invalid daemon delete thresholds")
	}
	if policy.MaxCount == 0 && policy.MaxFraction == 0 {
		return fmt.Errorf("at least one daemon delete threshold must remain enabled")
	}

	paths := a.paths
	if *serviceMode {
		servicePaths, err := a.servicePaths()
		if err != nil {
			return fmt.Errorf("resolve service daemon paths: %w", err)
		}
		paths = servicePaths
	}
	if err := paths.PrepareRuntime(); err != nil {
		return err
	}
	lease, err := daemonlock.Acquire(paths.RuntimeDir)
	if err != nil {
		if *serviceMode && errors.Is(err, daemonlock.ErrAlreadyRunning) {
			return nil
		}
		return err
	}
	defer lease.Close()

	if err := paths.PrepareConfig(); err != nil {
		return err
	}
	if err := a.installRcloneConfig(paths.RcloneConfig); err != nil {
		return fmt.Errorf("install app rclone config: %w", err)
	}
	state, err := a.openStateAt(ctx, paths)
	if err != nil {
		return err
	}
	defer state.Close()
	runner, err := a.newRunner(state, policy, a.reportRootEvent)
	if err != nil {
		return err
	}
	return runner.Run(ctx)
}

func (a *Application) openState(ctx context.Context) (*store.Store, error) {
	return a.openStateAt(ctx, a.paths)
}

func (a *Application) openStateAt(ctx context.Context, paths apppaths.Paths) (*store.Store, error) {
	if err := paths.PrepareState(); err != nil {
		return nil, err
	}
	state, err := store.Open(ctx, paths.StateDB)
	if err != nil {
		return nil, fmt.Errorf("open app state: %w", err)
	}
	return state, nil
}

func (a *Application) reportRootEvent(event daemon.RootEvent) {
	if event.Err != nil {
		fmt.Fprintf(a.stderr, "root %d %s: %v\n", event.RootID, event.Component, event.Err)
		return
	}
	if !event.HasResult {
		return
	}
	result := event.Result
	if result.Blocked {
		fmt.Fprintf(a.stderr, "root %d blocked: %s %s\n", event.RootID, result.BlockReason, result.BlockDetail)
		return
	}
	if result.Applied != 0 || result.Conflicts != 0 || result.Recovered != 0 || (result.Initial && result.Initialized) {
		fmt.Fprintf(a.stdout, "root %d cycle: applied=%d conflicts=%d recovered=%d initialized=%t\n", event.RootID, result.Applied, result.Conflicts, result.Recovered, result.Initialized)
	}
}

func parseRemoteSpec(spec string) (string, string, error) {
	name, root, ok := strings.Cut(spec, ":")
	if !ok || strings.TrimSpace(name) == "" || root == "" {
		return "", "", fmt.Errorf("remote %q must use remote:path form", spec)
	}
	if name != strings.TrimSpace(name) {
		return "", "", fmt.Errorf("remote name must not have leading or trailing whitespace")
	}
	return name, root, nil
}

func randomUUID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", value[0:4], value[4:6], value[6:8], value[8:10], value[10:16]), nil
}
