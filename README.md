# pkudisk-sync

Cross-platform stateful sync client for PKU Disk, targeting Linux, Windows, and macOS.

The project deliberately keeps `rclone-pkudisk` as the PKU Disk transport/API authority. `pkudisk-sync` owns synchronization state, reconciliation, conflict policy, delete safety, crash recovery, and later daemon/service integration.

## Status

Development is currently in **Phase B: pure reconciliation engine**. This phase has no filesystem watcher, long-running service, GUI, or live `rclone rcd` executor. It is intentionally testable using only fake local/remote snapshots and SQLite state.

The required backend safety contract was merged into `rclone-pkudisk` main at `0ccacea` (`feat(pkudisk): add stateful sync safety contract (#16)`). The published `v1.75.1-pkudisk.2` release predates that contract and must not be considered compatible by a future production binary manager.

## Core invariants

- Reconciliation is three-way: committed baseline vs current local vs current remote.
- Normal correctness is path-based; remote `docid` is a secondary identity for rename/move detection, not the database primary key.
- Conflicts preserve data; v1 does not silently use last-writer-wins.
- Initial pairing is non-destructive and does not infer deletion.
- Deletion requires complete observations, a healthy sync-root marker, and mass-delete guards.
- Watcher events will be hints, not durable truth. Startup/overflow/resume repair scans recover missed events.
- Durable operation records represent semantic external side effects. A `running` operation has unknown outcome after a crash and is never blindly replayed.
- `rclone-pkudisk` remains responsible for OAuth, PKU Disk API semantics, byte transfer, multipart upload, and transport retries.

## Planned implementation order

1. Pure domain model and three-way planner.
2. SQLite baseline / operation-intent / conflict persistence.
3. Deletion guards and crash-recovery decision model.
4. Supervised `rclone-pkudisk rcd` executor.
5. Native watchers and continuous daemon.
6. Per-user service packaging and CLI/UI polish.

## Development

```bash
go test ./...
go vet ./...
go test -race ./...
```

The default branch is `main`. Changes should be kept in small reviewable commits; larger features should use dedicated branches once the repository is published or collaborative work starts.
