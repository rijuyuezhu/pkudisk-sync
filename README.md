# pkudisk-sync

Cross-platform stateful sync client for PKU Disk, targeting Linux, Windows, and macOS.

The project directly embeds the Go `rclone-pkudisk` backend and rclone libraries as the PKU Disk transport/API implementation. There is no internal CLI/`rcd`/IPC execution layer. `pkudisk-sync` owns synchronization state, reconciliation, conflict policy, delete safety, crash recovery, watchers, and the long-running daemon.

## Status

The Phase B/C core and the first Phase D/E product loop are implemented: pure three-way reconciliation, initial merge, SQLite baseline/operation/conflict authority, deletion gates, crash recovery, multiple selected sync roots, full local/remote scans, guarded in-process file/directory mutations, root-marker safety, durable journal-to-postcondition execution, recursive filesystem watcher hints, periodic repair polling, pause/resume observation, dynamic discovery of newly configured roots, a cross-platform single-instance daemon lease, a minimal CLI, and native per-user background-service wiring for Linux, macOS, and Windows.

The executor directly imports rclone and `rclone-pkudisk`; it starts no rclone subprocess and exposes no internal RC/IPC boundary. Live PKU Disk smoke tests have validated expected-absent create, conditional update, stale-revision rejection, expected-absent collision rejection, exact-revision download, exact-ID file delete, and guarded exact-ID empty-directory delete. Non-empty remote directories are refused and preserved.

The required backend safety contract is on `rclone-pkudisk` main through `cd624f1`: #16 adds stateful file CAS primitives and #17 adds guarded exact-ID empty-directory delete. The Go dependency is pinned to a pseudo-version containing `cd624f1`.

## Selected directory pairs

One daemon owns any number of explicitly selected, independent directory pairs. A sync root is one bidirectional mapping:

```text
<absolute local directory>  <->  <rclone remote name>:<remote directory>
```

For example:

```text
~/Seafile/Data  <->  pkudisk:Personal/Data
~/Seafile/Work  <->  pkudisk:Personal/Work
~/Notes         <->  pkudisk:Personal/Notes
```

The names are not hard-coded. Each root has its own baseline, operations, conflicts, marker, polling state, and enabled/paused state. `SetupRoot` first acquires an authoritative SQLite `BEGIN IMMEDIATE` ownership reservation, then establishes the local marker, then commits the durable root row. This serializes overlapping root creation across independent CLI/GUI processes without leaving markers behind for deterministic ownership conflicts. Local roots may not overlap each other, and two roots on the same rclone remote may not own overlapping remote paths.

The daemon installs recursive local filesystem watches only as low-latency hints. Every hint runs the same complete root cycle; watcher setup/errors never become authoritative state. A root with `poll_interval_seconds = 0` uses the daemon safety default of 60 seconds, so periodic repair remains enabled even when no watcher event arrives. Disabled roots remain configured and are resumed without restarting the daemon.

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

`pkudisk-sync` owns its SQLite state and rclone configuration instead of reading the user's global rclone config. Show the platform-specific paths first:

```bash
pkudisk-sync paths
```

Configure a PKU Disk remote in the printed `rclone_config` file using the compatible `rclone-pkudisk` binary, for example:

```bash
rclone-pkudisk config --config /path/from/pkudisk-sync-paths/rclone.conf
```

Then select independent directory pairs and run the foreground daemon:

```bash
pkudisk-sync root add --local ~/Seafile/Data --remote pkudisk:Personal/Data
pkudisk-sync root list
pkudisk-sync status
pkudisk-sync conflict list
pkudisk-sync conflict list --root 1
pkudisk-sync root pause 1
pkudisk-sync root resume 1
pkudisk-sync daemon
```

`root add` refuses a local symlink root and refuses a remote name that is not configured as a `pkudisk` remote in the app-owned rclone config. `status` and `conflict list` are read-only views of durable SQLite state, so they remain useful offline; they do not contact PKU Disk or mutate conflict records. A conflict is only marked resolved by reconciliation after the conflicting state is actually gone—there is intentionally no bookkeeping-only `conflict resolve` command. `--poll 0` uses the daemon's 60-second repair default. The foreground daemon combines filesystem hints with full repair scans and, by default, blocks a cycle proposing more than 100 deletions. The fractional guard is disabled by default so ordinary deletes in small roots are not blocked; enable it explicitly with `--max-delete-fraction` when desired. At least one delete threshold must remain enabled. Only one daemon may own a user's runtime directory at a time; a second foreground/service instance fails immediately instead of reconciling concurrently.

To run the same daemon as a current-user background service:

```bash
pkudisk-sync service install
pkudisk-sync service start
pkudisk-sync service status
pkudisk-sync service stop
pkudisk-sync service uninstall
```

`install` registers but deliberately does not start synchronization immediately. Linux uses a `systemd --user` unit, macOS uses a LaunchAgent in `~/Library/LaunchAgents`, and Windows uses an interactive current-user Scheduled Task with limited privileges. The installed service always uses the platform default app paths; `service install` therefore refuses `PKUDISK_SYNC_*` path overrides rather than silently starting later with a different state database or rclone config.

## Planned implementation order

1. Pure domain model and three-way planner.
2. SQLite baseline / operation-intent / conflict persistence, including multiple selected sync roots.
3. Deletion guards and crash-recovery decision model.
4. In-process executor using the Go rclone / `rclone-pkudisk` APIs directly. **Implemented.**
5. Native watchers and continuous multi-root daemon. **Implemented with foreground CLI.**
6. Per-user service packaging and CLI/UI polish. **Native Linux/macOS/Windows user-service control is implemented; richer UX and release packaging remain.**

## Development

```bash
go test ./...
go vet ./...
go test -race ./...
```

The default branch is `main`. Changes should be kept in small reviewable commits; larger features should use dedicated branches once the repository is published or collaborative work starts.
