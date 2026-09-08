# pkudisk-sync

Cross-platform stateful sync client for PKU Disk, targeting Linux, Windows, and macOS.

The project directly embeds the Go `rclone-pkudisk` backend and rclone libraries as the PKU Disk transport/API implementation. There is no internal CLI/`rcd`/IPC execution layer. `pkudisk-sync` owns synchronization state, reconciliation, conflict policy, delete safety, crash recovery, watchers, and the long-running daemon.

## Status

The Phase B/C core and the first Phase D/E product loop are implemented: pure three-way reconciliation, initial merge, SQLite baseline/operation/conflict authority, deletion gates, crash recovery, multiple selected sync roots, full local/remote scans, guarded in-process file/directory mutations, root-marker safety, durable journal-to-postcondition execution, recursive filesystem watcher hints, periodic repair polling, active pause/resume cancellation, dynamic discovery of newly configured roots, a cross-platform single-instance daemon lease, a minimal CLI, and native per-user background-service wiring for Linux, macOS, and Windows.

The executor directly imports rclone and `rclone-pkudisk`; it starts no rclone subprocess and exposes no internal RC/IPC boundary. Live PKU Disk smoke tests have validated expected-absent create, conditional update, stale-revision rejection, expected-absent collision rejection, exact-revision download, exact-ID file delete, and guarded exact-ID empty-directory delete. Non-empty remote directories are refused and preserved.

The required backend safety contract is released as `rclone-pkudisk v1.75.1-pkudisk.3`: #16 adds stateful file CAS primitives and #17 adds guarded exact-ID empty-directory delete. The Go dependency is pinned to that release tag rather than to a moving pseudo-version.

## Selected directory pairs

One daemon owns any number of explicitly selected, independent directory pairs. A sync root is one bidirectional mapping:

```text
<absolute local directory>  <->  pkudisk:<remote directory>
```

For example:

```text
~/Seafile/Data  <->  pkudisk:Personal/Data
~/Seafile/Work  <->  pkudisk:Personal/Work
~/Notes         <->  pkudisk:Personal/Notes
```

The selected directory names are not hard-coded. v0.1 deliberately has one app-owned PKU Disk authentication profile, fixed as `pkudisk`; all selected roots share that account authority. Multiple accounts/remotes require a future stable account/root identity model rather than treating an arbitrary config name as identity. Each root has its own baseline, operations, conflicts, marker, polling state, and enabled/paused state. `SetupRoot` first acquires an authoritative SQLite `BEGIN IMMEDIATE` ownership reservation, then establishes the local marker, then commits the durable root row. This serializes overlapping root creation across independent CLI/GUI processes without leaving markers behind for deterministic ownership conflicts. Local roots may not overlap each other, and selected remote paths may not overlap.

The daemon installs recursive local filesystem watches only as low-latency hints. Every hint runs the same complete root cycle; watcher setup/errors never become authoritative state. A root with `poll_interval_seconds = 0` uses the daemon safety default of 60 seconds, so periodic repair remains enabled even when no watcher event arrives. `root pause` durably disables the root immediately; the daemon's ordinary root-refresh loop then cancels any active worker and waits for that worker to finish unwinding before a later resume may start a replacement. If cancellation lands after a durable operation was marked `running`, its outcome is treated as unknown and the next resumed cycle uses the normal postcondition-recovery path rather than blindly replaying the mutation.

## Core invariants

- Reconciliation is three-way: committed baseline vs current local vs current remote.
- Normal correctness is path-based; remote `docid` is a secondary identity for rename/move detection, not the database primary key.
- Conflicts preserve data; v1 does not silently use last-writer-wins.
- Initial pairing is non-destructive and does not infer deletion.
- Deletion requires complete observations, a healthy sync-root marker, and mass-delete guards.
- Watcher events will be hints, not durable truth. Startup/overflow/resume repair scans recover missed events.
- Durable operation records represent semantic external side effects. A `running` operation has unknown outcome after a crash and is never blindly replayed.
- The embedded `rclone-pkudisk` backend remains responsible for OAuth, PKU Disk API semantics, byte transfer, multipart upload, and transport retries.

## CLI

Portable release archives target Linux, macOS, and Windows on both amd64 and arm64. After extracting an archive, move `pkudisk-sync` (or `pkudisk-sync.exe`) to a stable per-user executable path before installing the background service. Do not run `service install` from a temporary extraction/download directory: the native service definition records the executable's absolute path. v0.1 portable binaries are not yet platform-signed: macOS may require a one-time Gatekeeper approval in System Settings → Privacy & Security after the first attempted launch; Windows may show a SmartScreen warning, while Smart App Control or enterprise policy can block an unsigned binary entirely. Verify the release `SHA256SUMS` before approving a downloaded binary; do not disable Gatekeeper/SmartScreen globally. On macOS, roots under privacy-protected locations such as Desktop, Documents, Downloads, iCloud Drive, network volumes, or removable volumes may also require explicit Files & Folders or Full Disk Access permission for the installed executable/LaunchAgent. Signed/notarized native installers and smoother permission onboarding remain future packaging work. Verify the installed binary with:

```bash
pkudisk-sync version
```

`pkudisk-sync` owns its SQLite state and rclone configuration instead of reading the user's global rclone config. Configure OAuth directly through the embedded PKU Disk backend; no separate rclone/rclone-pkudisk binary is required:

```bash
pkudisk-sync paths
pkudisk-sync remote configure
```

`remote configure` creates or re-authenticates the single app-owned remote named `pkudisk` and drives rclone's normal browser OAuth flow in-process. Stop the foreground daemon/user service before running it so the next daemon process loads the newly written token; the command enforces this with the same single-instance lease. v0.1 intentionally does not accept custom remote names because config aliases are not a safe account identity.

Then select independent directory pairs and run the foreground daemon:

```bash
pkudisk-sync root add --local ~/Seafile/Data --remote pkudisk:Personal/Data
pkudisk-sync root list
pkudisk-sync status
pkudisk-sync conflict list
pkudisk-sync conflict list --root 1
# Choose exactly one side for a listed file conflict:
pkudisk-sync conflict resolve 12 --keep-local
# or: pkudisk-sync conflict resolve 12 --keep-remote
pkudisk-sync root pause 1
pkudisk-sync root resume 1
pkudisk-sync daemon
```

`root add` refuses a local symlink root and accepts only the app-owned `pkudisk:` remote authority. It also refuses an unexpected existing `.pkudisk-sync-root` marker. If a previous `root add` was interrupted after writing that marker but before committing SQLite state, rerun the same add with `--recover-orphan-marker`; this explicit flag replaces only the reserved marker and leaves all ordinary local data untouched. `status` and `conflict list` are read-only views of durable SQLite state, so they remain useful offline. `conflict resolve` is not a bookkeeping shortcut: for a supported file conflict it queues an ordinary durable operation carrying the exact local and remote fingerprints recorded by that conflict. The daemon revalidates both sides before mutation and uses the existing guarded upload/download/delete and crash-recovery path. The conflict stays unresolved until a later reconciliation observes convergence and commits the new baseline. If either side changed after the conflict was observed, the stale planned operation is discarded rather than overwriting newer data; reconciliation then replaces the stale conflict record with a new current conflict ID. Directory/subtree conflicts and file/directory kind mismatches are intentionally refused for automatic keep-local/keep-remote resolution until a safe multi-operation subtree choreography exists. A resolution may be queued while its root is paused, but the root must be resumed before the daemon applies it. `root remove ID` is likewise deliberately non-destructive: it only unregisters a selected pair and removes its private marker. The root must already be paused, have no pending operation intents, and the daemon/service must be stopped; local and remote user data are never deleted by detach. An interrupted remove can simply be retried even if its marker was already deleted. `--poll 0` uses the daemon's 60-second repair default. The foreground daemon combines filesystem hints with full repair scans and, by default, blocks a cycle proposing more than 100 deletions. The fractional guard is disabled by default so ordinary deletes in small roots are not blocked; enable it explicitly with `--max-delete-fraction` when desired. At least one delete threshold must remain enabled. Only one daemon may own a user's runtime directory at a time. A second manually launched foreground daemon fails immediately; `service start` also refuses to launch while a foreground owner exists. The supervisor-only daemon mode treats a lock conflict as a clean no-op so systemd/launchd cannot enter a restart storm if ownership changes in the small interval after the start preflight.

During initial pairing, a selected remote path that does not exist yet is treated as an empty remote only while that root is still uninitialized, allowing the first local file or directory to create the remote path through the ordinary journaled mutation flow. If the local root is also empty, the pair stays dormant and uninitialized until local content appears or the remote path is created; pkudisk-sync does not invent a special unjournaled root-creation mutation. After initialization, disappearance of the selected remote root is a hard scan error and is never interpreted as a request to delete local data.

To run the same daemon as a current-user background service:

```bash
pkudisk-sync service install
pkudisk-sync service start
pkudisk-sync service status
pkudisk-sync service stop
pkudisk-sync service uninstall
```

To detach a selected root without deleting either side, pause it first and stop the daemon/service before unregistering it:

```bash
pkudisk-sync root pause 1
pkudisk-sync service stop
pkudisk-sync root remove 1
```

`install` registers but deliberately does not start synchronization immediately. Linux uses a `systemd --user` unit, macOS uses a LaunchAgent in `~/Library/LaunchAgents`, and Windows registers an interactive current-user Scheduled Task with limited privileges through the native ScheduledTasks PowerShell API, which works without elevation; later start/stop/uninstall operations use the same task under the current user. `service status` is normalized to `active`, `inactive`, or `not-installed` instead of exposing platform- or locale-specific task states. Reinstall/upgrade is fail-closed while any sync daemon owns the default runtime lease: stop the foreground daemon or user service before `service install`, then start it again after the new executable/definition is in place. On macOS, `service start` unloads any inactive cached LaunchAgent before bootstrap so launchd re-reads the current plist rather than retaining stale `ProgramArguments`. The installed service always uses OS-native platform-default app paths rather than shell-selected path authorities. `service install` and `service start` therefore fail closed if the invoking CLI resolves to a different authority because of `PKUDISK_SYNC_*`, custom XDG paths, or an altered home/profile environment. The final supervisor-only `daemon --service` process independently resolves the native user home or Windows Known Folder path, so inherited systemd, launchd, or Task Scheduler environment variables cannot redirect its lease, SQLite state, or rclone config. This keeps the start preflight lease, SQLite state, rclone config, service definition, and running daemon on the same authority. Linux and macOS restart genuine daemon failures, but an already-owned single-instance lease is an intentional clean service exit rather than a restartable failure.

## v0.1 implementation status

1. Pure domain model and three-way planner — **implemented.**
2. SQLite baseline / operation-intent / conflict persistence, including multiple selected sync roots — **implemented.**
3. Deletion guards and crash-recovery decision model — **implemented.**
4. In-process executor using the Go rclone / `rclone-pkudisk` APIs directly — **implemented.**
5. Native watchers and continuous multi-root daemon — **implemented with foreground CLI and per-user background-service integration.**
6. Product/release surface — **Linux/macOS/Windows user-service control, explicit file-conflict resolution, and a six-target portable release pipeline are implemented. Richer GUI UX plus signed/notarized native installers remain later packaging work rather than v0.1 synchronization requirements.**

## Development

```bash
go test ./...
go vet ./...
go test -race ./...
```

The default branch is `main`. Changes should be kept in small reviewable commits; larger features should use dedicated branches once the repository is published or collaborative work starts.
