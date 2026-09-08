# Architecture

`pkudisk-sync` is a stateful bidirectional synchronization engine for explicitly selected PKU Disk directory pairs. It directly embeds rclone and `rclone-pkudisk` as Go libraries; there is no rclone subprocess, `rcd`, or internal RPC layer.

The architectural split is simple:

- `internal/domain` defines durable synchronization concepts and invariants.
- `internal/store` is the SQLite semantic authority for selected roots, committed baselines, operation intents, and conflicts.
- `internal/reconcile` computes safe plans from baseline/local/remote observations.
- `internal/executor` performs guarded local and PKU Disk observations and mutations through embedded rclone APIs.
- `internal/syncer` connects planning, durable operation state, execution, crash recovery, and baseline commit.
- `internal/daemon` owns continuous multi-root scheduling.
- `internal/watcher` supplies low-latency local-change hints; it is never authoritative state.
- `internal/userservice` integrates the daemon with native per-user service managers.

## Sync roots

A sync root is one explicit mapping:

```text
<absolute local directory>  <->  pkudisk:<remote directory>
```

One daemon can own multiple independent roots. Local roots may not overlap each other, and selected remote paths may not overlap. v0.1 intentionally uses one app-owned PKU Disk authentication profile named `pkudisk`; a config alias is not treated as account identity.

Each root has its own:

- committed baseline;
- durable operation intents;
- unresolved conflicts;
- local ownership marker;
- polling state;
- enabled/paused state;
- initialization state.

## Three-way reconciliation

Correctness is based on three observations:

1. the last committed baseline;
2. the current local namespace;
3. the current remote namespace.

A two-way "copy whichever side is newer" model cannot distinguish a real delete from an unseen or stale side and cannot safely recover after an interrupted mutation. `pkudisk-sync` therefore plans from all three states and commits a new baseline only after the planned external effects have been observed as complete.

Normal namespace authority is path-based. PKU Disk `docid` values are secondary identity used where useful for guarded mutations or move/rename detection; they are not the durable primary key for a path.

## Initial pairing

An uninitialized root is deliberately non-destructive. Existing content on either side is merged, and absence on one side is not interpreted as a deletion request.

A selected remote root that does not exist yet is treated as an empty remote only while the root remains uninitialized. This permits an existing local tree to create the remote path through the ordinary journaled mutation flow. If both sides are empty, the pair remains uninitialized until content appears. Once initialization commits, disappearance of the selected remote root is a hard observation error, never a request to erase local data.

## Durable operation journal

Every external mutation is represented by a durable operation record before execution. Operations move through semantic phases such as `planned`, `running`, `recovering`, and `blocked`.

The important crash rule is:

> A `running` operation has an unknown external outcome after process loss and must never be blindly replayed.

On restart, the syncer observes operation-specific preconditions and postconditions. Recovery can then:

- commit when the postcondition is already satisfied;
- retry only when observation proves the original effect did not happen and its preconditions still hold;
- fall back to reconciliation when the outcome is ambiguous or the original assumptions are stale;
- block when the observed state has diverged in a way that cannot be safely automated.

This same path is used when pause/shutdown cancellation lands after an operation was durably marked `running`.

## Guarded mutations

The executor does not perform unconditional overwrite/delete operations. The embedded `rclone-pkudisk` backend supplies the PKU Disk primitives needed to enforce expected-absent creation, conditional revision updates, exact-revision reads, and exact-ID deletion.

Examples of fail-closed behavior include:

- stale remote revisions reject an update rather than overwriting newer data;
- expected-absent collisions reject a create;
- exact-revision download refuses a different remote revision;
- remote file deletion uses the observed object identity;
- remote directory deletion is allowed only for the exact observed empty directory;
- non-empty remote directories are preserved.

The backend safety contract is pinned through the `rclone-pkudisk` version in `go.mod`; downstream code must not assume stronger remote semantics than that dependency exposes.

## Deletion safety

Deletion is never inferred from an incomplete observation. A destructive plan requires:

- a complete local/remote observation for the relevant root;
- a healthy app-owned root marker;
- an initialized root;
- baseline evidence that the path previously existed;
- configured mass-delete limits.

The default daemon blocks a cycle proposing more than 100 deletions. A fractional threshold can also be enabled. At least one deletion threshold must remain active.

## Conflicts

v0.1 does not use last-writer-wins. A true concurrent change becomes a durable conflict and preserves both sides.

For supported file conflicts, `conflict resolve --keep-local` or `--keep-remote` queues an ordinary guarded operation carrying the exact fingerprints recorded by the conflict. The conflict is not marked resolved merely because a user chose a side; it disappears only after later reconciliation observes convergence and commits it.

If either side changed after the conflict was recorded, the stale resolution operation is discarded instead of overwriting the newer state. Directory/subtree conflicts and file/directory kind mismatches remain manual because they require a safe multi-operation namespace choreography.

## Watchers and periodic repair

Recursive filesystem watchers are latency hints only. Every hint requests the same complete root cycle used by periodic polling. Startup, watcher overflow/failure, resume, or missed events are repaired by full scans.

A configured poll interval of zero means "use the daemon safety default", currently 60 seconds; it does not disable repair polling.

Each root has at most one active worker. Pausing a root cancels its active worker and the runner waits for that worker to unwind before a later resume can start a replacement.

## Single daemon and native services

A per-user runtime lease ensures that only one daemon owns synchronization state at a time. A second foreground daemon fails immediately. `service start` also performs the same preflight so a native service cannot race an already-running foreground owner.

The supervisor-only `daemon --service` mode treats a lock conflict as a clean no-op. This prevents systemd/launchd restart storms if ownership changes in the narrow interval between service-start preflight and the supervised process acquiring the lease.

Installed services use OS-native per-user paths rather than shell-selected path overrides. This keeps the service definition, daemon lease, SQLite database, and app-owned rclone configuration on one authority even when a shell has custom XDG or application path variables.

Platform integration is intentionally native and per-user:

- Linux: `systemd --user`;
- macOS: LaunchAgent;
- Windows: current-user Scheduled Task with Interactive logon and Limited privileges.

See [operations.md](operations.md) for operational details and [development.md](development.md) for validation expectations.
