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

One daemon can own multiple independent roots. Local roots may not overlap each other, and selected remote paths may not overlap. Within one complete root scan, one physical filesystem identity may be owned by only one logical path. In addition, every followed symlink target (file or directory) is reserved in the shared SQLite authority before planning or mutation: the same current physical identity or canonical physical target pathname cannot be owned by two configured roots. Followed symlinks that resolve directly inside another configured root are also rejected. Ordinary cross-root bind-mount or hard-link overlap that does not pass through a followed symlink remains unsupported rather than globally enumerated. v0.1 intentionally uses one app-owned PKU Disk authentication profile named `pkudisk`; a config alias is not treated as account identity.

Each root has its own:

- committed baseline;
- durable operation intents;
- unresolved conflicts;
- local ownership marker;
- polling state;
- enabled/paused state;
- initialization state;
- symlink policy;
- durable physical ownership claims for followed symlink targets.

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

The synchronization namespace remains lexical even when a root uses the default `follow` policy, but a local mutation is applied to the resolved physical target rather than replacing the symlink object. A followed target may live outside the selected root or on a different filesystem. A complete scan first reserves every followed file/directory target as a daemon-wide physical claim in SQLite before planning. Physical directory ownership is hierarchical: claiming a canonical directory target owns that entire canonical pathname subtree, so another root cannot separately claim that directory, any descendant file/directory, or a parent directory that would contain it. Exact physical identity checks remain in place for aliases that pathname containment cannot prove. A followed directory and a configured local root are also rejected on overlap in either direction. Ordinary cross-root bind-mount and hard-link overlap that is not reached through a followed symlink remains unsupported in v0.1.

Before a local create/update/delete can enter `running`, the executor re-resolves the current mutation path and reports every followed symlink boundary actually traversed. In one SQLite transaction the store verifies those boundaries against the scan-time durable claims and global subtree ownership, safely refreshes same-target file identity when allowed, and pins both the canonical absolute physical target and a physical anchor identity in the operation journal. Existing targets use the target object's identity as the anchor; absent targets use the physical parent directory. A retarget between scan and pin therefore cannot borrow another root's physical ownership even when the ordinary file fingerprint is equivalent. File staging and preservation slots are then created in the target's physical parent directory, keeping no-replace rename operations on the same filesystem.

Hierarchical ownership checks are serialized under SQLite writer authority before their read-check-write sequence. Scan-time claim reservation, mutation-time claim refresh/pin, and sync-root creation therefore cannot concurrently observe an unclaimed parent/child namespace and both commit ownership. Exact-identity/target triggers remain defense-in-depth for equality conflicts; subtree conflicts are enforced by the serialized authority transaction.

Immediately before each local side effect, and again after a potentially long download before commit, the executor verifies the durable physical anchor identity in addition to the lexical/physical target fence. This covers pin-to-mutation retargets and same-path inode/file-ID replacement, including replacement of the parent of an absent target. For replacement or deletion of an existing target, the executor then atomically moves the exact expected target to an operation-ID-specific recovery slot and validates both the fingerprint and physical identity of the object that actually moved. During crash recovery the syncer first proves whether the desired postcondition already holds. If it does, a stale recovery slot is removed only when it still has the operation's pinned identity before the journal is committed; a replaced or ambiguous slot is preserved and fails closed. When completion cannot be proven, a preserved recovery artifact blocks automatic replay. Explicit restore likewise verifies the artifact identity for v6+ operations and rolls an unexpected moved object back into the recovery slot. Legacy operations that started before a physical identity was durably pinned are inspectable but block automatic replay.

`ignore` is modeled as an excluded namespace rather than absence. Reconciliation skips the excluded prefix and descendants entirely, including deletion-gate accounting. The scanner applies this policy from `Lstat` before resolving the symlink, so an ignored link never gains authority merely because its target is another root or temporarily unavailable. `reject` instead makes the local observation incomplete by returning an error. Follow-mode cycles and aliases to a physical ancestor are excluded so recursive traversal cannot loop.

A followed link whose final referent is missing is non-authoritative for that lexical prefix. A single `ENOENT` cannot distinguish an intentional referent delete from a temporarily unavailable external mount, so dangling followed targets never grant remote deletion authority. Every followed file/directory stores a durable global ownership claim. For followed **directories**, successful initial pairing fixes the platform physical identity (device+inode on Linux/macOS; volume+file ID on Windows) and the canonical target subtree; later scans must prove the same identity before descendants regain deletion authority. A different directory identity at the same pathname, a parent/descendant subtree overlap, or a newly introduced followed path on an initialized root blocks reconciliation and requires deliberate re-pairing. For followed **files**, normal atomic-save patterns may replace the file inode/file-ID, so the current identity may advance only while the canonical target pathname remains the same and global identity/path/subtree ownership remains uncontested. An unavailable followed target remains excluded while its prior durable ownership claim is retained.

Complete scans also enforce one logical owner per physical file/directory identity within a root scan. Sibling aliases, hard links to the same file, or a symlink expansion that would claim an object already reached through another logical path fail closed instead of creating independent baselines/journals for one physical object. Followed symlink targets have the additional daemon/store-wide subtree claim described above, so two roots cannot independently follow a parent/descendant pair such as `/shared` and `/shared/subdir` or `/shared/file.txt`. The implementation still does not globally enumerate arbitrary non-symlink bind-mount/hard-link overlap between separately selected roots.

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
