# pkudisk-sync

Cross-platform stateful sync client for PKU Disk, targeting Linux, Windows, and macOS.

The project directly embeds the Go `rclone-pkudisk` backend and rclone libraries as the PKU Disk transport/API implementation. There is no internal CLI/`rcd`/IPC execution layer. `pkudisk-sync` owns synchronization state, reconciliation, conflict policy, delete safety, crash recovery, watchers, and the long-running daemon.

## Status

Development is currently in **Phase B: pure reconciliation engine**. This phase has no filesystem watcher, long-running service, GUI, or live PKU Disk executor. It is intentionally testable using only fake local/remote snapshots and SQLite state.

The required backend safety contract was merged into `rclone-pkudisk` main at `0ccacea` (`feat(pkudisk): add stateful sync safety contract (#16)`). The Go dependency used by this project must include that commit or a later compatible version.

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

The names are not hard-coded. Each root has its own baseline, operations, conflicts, marker, polling state, and enabled/paused state. Local roots may not overlap each other, and two roots on the same rclone remote may not own overlapping remote paths. This prevents one file from being reconciled by two roots.

## Core invariants

- Reconciliation is three-way: committed baseline vs current local vs current remote.
- Normal correctness is path-based; remote `docid` is a secondary identity for rename/move detection, not the database primary key.
- Conflicts preserve data; v1 does not silently use last-writer-wins.
- Initial pairing is non-destructive and does not infer deletion.
- Deletion requires complete observations, a healthy sync-root marker, and mass-delete guards.
- Watcher events will be hints, not durable truth. Startup/overflow/resume repair scans recover missed events.
- Durable operation records represent semantic external side effects. A `running` operation has unknown outcome after a crash and is never blindly replayed.
- The embedded `rclone-pkudisk` backend remains responsible for OAuth, PKU Disk API semantics, byte transfer, multipart upload, and transport retries.

## Planned implementation order

1. Pure domain model and three-way planner.
2. SQLite baseline / operation-intent / conflict persistence, including multiple selected sync roots.
3. Deletion guards and crash-recovery decision model.
4. In-process executor using the Go rclone / `rclone-pkudisk` APIs directly.
5. Native watchers and continuous multi-root daemon.
6. Per-user service packaging and CLI/UI polish.

## Development

```bash
go test ./...
go vet ./...
go test -race ./...
```

The default branch is `main`. Changes should be kept in small reviewable commits; larger features should use dedicated branches once the repository is published or collaborative work starts.
