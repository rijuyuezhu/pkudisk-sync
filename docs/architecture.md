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

One daemon can own multiple independent roots. Local roots may not overlap each other, and selected remote paths may not overlap. Within one complete root scan, one physical filesystem identity may be owned by only one logical path, and followed symlinks that resolve directly inside another configured root are rejected. v0.1 does **not** maintain a daemon-wide device/inode registry across roots, so cross-root bind-mount aliases and cross-root hard links are unsupported and must not be used to select overlapping physical data. v0.1 intentionally uses one app-owned PKU Disk authentication profile named `pkudisk`; a config alias is not treated as account identity.

Each root has its own:

- committed baseline;
- durable operation intents;
- unresolved conflicts;
- local ownership marker;
- polling state;
- enabled/paused state;
- initialization state;
- symlink policy;
- durable physical identities for followed-directory boundaries.

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

AnyShare delete is weaker than upload/download CAS. The pinned backend revalidates an exact file `docid` and expected revision immediately before deletion, but the AnyShare delete request itself accepts only the object ID, not a revision precondition. There is therefore an unavoidable narrow TOCTOU window between the last revision check and the server-side delete. Directory deletion likewise checks that the exact observed directory is empty before issuing the exact-ID delete, but a concurrent child create can race that final request. These are explicit API limitations; multi-writer remote deployments cannot claim strict revision-CAS deletion.

### Followed symlinks and physical local targets

The synchronization namespace remains lexical even when a root uses the default `follow` policy, but a local mutation is applied to the resolved physical target rather than replacing the symlink object. A followed target may live outside the selected root or on a different filesystem. Direct resolution into another configured root is rejected; cross-root bind-mount and hard-link aliases that do not preserve pathname ancestry are unsupported in v0.1 rather than claimed as globally deduplicated.

Before a local create/update/delete can enter `running`, the syncer resolves and durably pins its canonical absolute physical target in the operation journal. File staging and preservation slots are then created in that target's physical parent directory, keeping no-replace rename operations on the same filesystem.

For replacement or deletion of an existing target, the executor first atomically moves the exact expected target to an operation-ID-specific recovery slot and validates the object that actually moved. A symlink retarget between planning and execution therefore fails its physical-identity fence rather than mutating the new target. During crash recovery the syncer first proves whether the desired postcondition already holds. If it does, any operation-owned stale recovery slot is removed and the journal is committed; only when completion cannot be proven does a preserved recovery artifact block automatic replay.

`ignore` is modeled as an excluded namespace rather than absence. Reconciliation skips the excluded prefix and descendants entirely, including deletion-gate accounting. The scanner applies this policy from `Lstat` before resolving the symlink, so an ignored link never gains authority merely because its target is another root or temporarily unavailable. `reject` instead makes the local observation incomplete by returning an error. Follow-mode cycles and aliases to a physical ancestor are excluded so recursive traversal cannot loop.

A followed link whose final referent is missing is non-authoritative for that lexical prefix. A single `ENOENT` cannot distinguish an intentional referent delete from a temporarily unavailable external mount, so dangling followed targets never grant remote deletion authority. For followed **directory** boundaries, the successful initial pairing also records a durable platform physical identity (device+inode on Linux/macOS; volume+file ID on Windows). Later scans must prove the same identity before descendants regain deletion authority. A different identity at the same pathname, or a newly introduced followed-directory boundary on an initialized root, blocks reconciliation and requires deliberate re-pairing instead of being learned automatically. An unavailable boundary remains excluded while its prior durable identity is retained.

Complete scans also enforce one logical owner per physical file/directory identity **within that root scan**. Sibling aliases, hard links to the same file, or a symlink expansion that would claim an object already reached through another logical path fail closed instead of creating independent baselines/journals for one physical object. This is intentionally not described as a daemon-wide guarantee across separate configured roots.

Operation staging/recovery files are internal only when a durable local-mutation operation explicitly owns their exact physical path. A filename that merely resembles `.pkudisk-sync-tmp-op-<id>-download` or `...-recovery` is not silently hidden; without matching journal ownership it is a reserved-namespace error. The remote scanner rejects the same reserved namespace, keeping local and remote authority symmetric.

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

Watchers recurse only through real directories under the selected local root; they never follow symlink targets. This is deliberate: following links would create a second, mutable watcher graph over arbitrary external directories and platform-specific reparse behavior. The authoritative repair scan still dereferences `follow` links, so changes under an external target are eventually observed without making watcher coverage part of correctness.

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
