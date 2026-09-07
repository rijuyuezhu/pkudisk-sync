package cli

import (
	"context"
	"crypto/rand"
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
	"github.com/rijuyuezhu/pkudisk-sync/internal/daemon"
	"github.com/rijuyuezhu/pkudisk-sync/internal/daemonlock"
	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
	"github.com/rijuyuezhu/pkudisk-sync/internal/executor"
	"github.com/rijuyuezhu/pkudisk-sync/internal/reconcile"
	"github.com/rijuyuezhu/pkudisk-sync/internal/store"
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
	validateRemote      func(string) error
	newRunner           func(*store.Store, reconcile.DeletePolicy, daemon.Reporter) (daemonRunner, error)
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
		validateRemote: func(name string) error {
			_, err := executor.RemoteConfig(name)
			return err
		},
		newRunner: func(state *store.Store, policy reconcile.DeletePolicy, report daemon.Reporter) (daemonRunner, error) {
			return daemon.NewRunner(state, policy, report)
		},
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
	case "root":
		return a.runRoot(ctx, args[1:])
	case "daemon":
		return a.runDaemon(ctx, args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func (a *Application) printUsage() {
	fmt.Fprintln(a.stdout, "Usage: pkudisk-sync <command> [options]")
	fmt.Fprintln(a.stdout)
	fmt.Fprintln(a.stdout, "Commands:")
	fmt.Fprintln(a.stdout, "  paths                              Show app-owned state/config/cache/runtime paths")
	fmt.Fprintln(a.stdout, "  root add --local PATH --remote R:P Add one selected directory pair")
	fmt.Fprintln(a.stdout, "  root list                          List selected directory pairs")
	fmt.Fprintln(a.stdout, "  root pause ID                      Pause one selected pair")
	fmt.Fprintln(a.stdout, "  root resume ID                     Resume one selected pair")
	fmt.Fprintln(a.stdout, "  daemon                             Run the foreground sync daemon")
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

func (a *Application) runRoot(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("root requires one of: add, list, pause, resume")
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
	stored, err := daemon.SetupRoot(ctx, state, root)
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

func (a *Application) runDaemon(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("daemon", flag.ContinueOnError)
	fs.SetOutput(a.stderr)
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

	if err := a.paths.PrepareRuntime(); err != nil {
		return err
	}
	lease, err := daemonlock.Acquire(a.paths.RuntimeDir)
	if err != nil {
		return err
	}
	defer lease.Close()

	if err := a.paths.PrepareConfig(); err != nil {
		return err
	}
	if err := a.installRcloneConfig(a.paths.RcloneConfig); err != nil {
		return fmt.Errorf("install app rclone config: %w", err)
	}
	state, err := a.openState(ctx)
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
	if err := a.paths.PrepareState(); err != nil {
		return nil, err
	}
	state, err := store.Open(ctx, a.paths.StateDB)
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
