# Operations guide

This guide covers day-to-day configuration, recovery, and native background-service behavior. The project is currently preparing its first v0.1 release; until a release is published, build from source.

## Build from source

The repository pins Go 1.26.8 in `.go-version`.

```bash
git clone https://github.com/rijuyuezhu/pkudisk-sync.git
cd pkudisk-sync
GOTOOLCHAIN=go1.26.8 go build -o ./pkudisk-sync ./cmd/pkudisk-sync
./pkudisk-sync version
```

Move the resulting binary to a stable per-user location before installing a native background service. The service definition records the executable's absolute path.

## App-owned paths

`pkudisk-sync` deliberately does not reuse the user's global rclone configuration. Inspect the active paths with:

```bash
pkudisk-sync paths
```

The command reports the SQLite state database, app-owned rclone configuration, cache directory, and runtime directory.

Foreground CLI commands may honor supported path overrides. Native service installation/start, however, requires the OS-native default app authority so the preflight lease and supervised daemon cannot silently resolve different state/config locations.

## Configure PKU Disk authentication

```bash
pkudisk-sync remote configure
```

This creates or re-authenticates the single app-owned remote named `pkudisk` and drives the embedded rclone OAuth flow. No separate rclone or `rclone-pkudisk` executable is required.

Stop any foreground daemon or installed user service before reconfiguring authentication. The command acquires the same single-instance lease and fails closed if synchronization is still running.

Never copy the generated rclone configuration into issues or commits; it contains credentials.

## Add sync roots

```bash
pkudisk-sync root add --local ~/Seafile/Data --remote pkudisk:Personal/Data
pkudisk-sync root list
pkudisk-sync status
```

The local root must be an actual directory, not a symlink. Local roots cannot overlap one another, and remote roots cannot overlap one another.

### Symlink policy

Each root has an explicit local symlink policy. The default is `follow`:

```bash
pkudisk-sync root add \
  --local ~/Seafile/Data \
  --remote pkudisk:Personal/Data \
  --symlinks follow
```

The modes are:

- `follow` — dereference file and directory symlinks into the synchronized virtual namespace, including links whose targets are outside the selected local root. Remote updates and deletes mutate the resolved target while preserving the symlink object. If a followed target is deleted, the dangling final link remains and can be rehydrated if that remote path later reappears. Cycles and links back to a physical ancestor are excluded instead of traversed.
- `reject` — any symlink makes the complete local scan fail closed.
- `ignore` — the symlink path and its virtual subtree are excluded from the local namespace for that cycle. Excluded paths carry no download or deletion authority, so ignoring a link cannot be mistaken for deleting it.

Change the policy only while the root is paused and has no pending operation:

```bash
pkudisk-sync root pause 1
pkudisk-sync root config 1 --symlinks ignore
pkudisk-sync root resume 1
```

Filesystem watchers deliberately do not follow symlink targets. In particular, a target outside the selected root does not expand the watcher's ownership into another directory tree. Changes there are discovered by the authoritative periodic repair scan; configure a shorter `--poll` interval when lower detection latency is needed.

`root add` writes an app-owned `.pkudisk-sync-root` marker. If the previous add was interrupted after writing that marker but before committing SQLite state, rerun exactly the intended add with:

```bash
pkudisk-sync root add \
  --local ~/Seafile/Data \
  --remote pkudisk:Personal/Data \
  --recover-orphan-marker
```

The recovery flag only replaces the reserved marker; it does not delete ordinary user data.

## Initial pairing and a missing remote directory

The first pairing is non-destructive. Existing content on both sides is merged and absence is not treated as deletion.

If `pkudisk:Personal/Data` does not exist yet, an uninitialized root treats it as an empty remote. Existing local content can therefore create the remote directory through ordinary journaled operations. If both sides are empty, the root remains dormant and uninitialized until content appears.

After the root has initialized, disappearance of its selected remote directory is a hard error. It is never interpreted as permission to delete local files.

## Run in the foreground

```bash
pkudisk-sync daemon
```

Useful inspection commands can be run from another shell:

```bash
pkudisk-sync status
pkudisk-sync conflict list
```

Only one daemon can own the current user's runtime lease. Starting a second foreground daemon fails rather than creating two synchronization writers.

### Deletion thresholds

The daemon defaults to a maximum of 100 planned deletions in one cycle. You can choose a different count or enable a fractional baseline guard:

```bash
pkudisk-sync daemon --max-delete-count 50 --max-delete-fraction 0.20
```

A value of zero disables that individual threshold, but at least one deletion threshold must remain enabled.

## Pause and resume

```bash
pkudisk-sync root pause 1
pkudisk-sync root resume 1
```

Pause is durable. The daemon cancels an active worker and waits for it to unwind before a later resume may start a replacement. If cancellation occurs after a mutation entered the durable `running` phase, restart/resume uses normal postcondition recovery rather than replaying the mutation blindly.

## Conflicts

List unresolved conflicts:

```bash
pkudisk-sync conflict list
pkudisk-sync conflict list --root 1
```

For supported file conflicts, choose one observed side:

```bash
pkudisk-sync conflict resolve 12 --keep-local
# or
pkudisk-sync conflict resolve 12 --keep-remote
```

The command queues a guarded operation; it does not immediately rewrite conflict bookkeeping. A later reconciliation verifies the exact observed states and removes the conflict only after convergence is proven.

If the root is paused, the resolution remains queued until the root is resumed. If either side changed after the conflict was recorded, the stale resolution is discarded rather than overwriting new data.

Directory/subtree conflicts and file/directory kind mismatches are intentionally not auto-resolved in v0.1.

## Native user service

Install and control the same daemon as the current user's background service:

```bash
pkudisk-sync service install
pkudisk-sync service start
pkudisk-sync service status
pkudisk-sync service stop
pkudisk-sync service uninstall
```

`service install` registers the service but deliberately does not start synchronization immediately. `service status` returns one stable product state:

- `active`
- `inactive`
- `not-installed`

The implementation is:

- Linux: `systemd --user` unit;
- macOS: LaunchAgent in `~/Library/LaunchAgents`;
- Windows: current-user Scheduled Task registered through the ScheduledTasks API with Interactive logon and Limited privileges.

Windows installation is designed to work without elevation. macOS may require Files & Folders or Full Disk Access permission when roots live in privacy-protected locations.

Reinstall/upgrade is fail-closed while any daemon owns the runtime lease. Stop the foreground daemon or user service before replacing the binary or reinstalling the service.

## Detach a root without deleting data

Detach is explicitly non-destructive:

```bash
pkudisk-sync root pause 1
pkudisk-sync service stop  # or stop the foreground daemon
pkudisk-sync root remove 1
```

The root must be paused and have no pending operation intents. `root remove` unregisters the pair and removes its private marker; local and remote user data remain unchanged. If removal is interrupted after the marker disappears, rerun the command.

## Troubleshooting checklist

When synchronization stops or blocks:

1. Run `pkudisk-sync status`.
2. Run `pkudisk-sync conflict list`.
3. Check whether the root is paused.
4. Verify authentication with `pkudisk-sync remote configure` if the token is known to be expired or replaced; stop the daemon first.
5. Treat an initialized missing remote root as an error to investigate, not as a delete request.
6. Do not manually remove operation/conflict rows from SQLite to "unstick" a root; those records are part of crash-safety authority.

For semantic details, see [architecture.md](architecture.md).
