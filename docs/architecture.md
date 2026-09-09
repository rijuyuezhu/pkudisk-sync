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

One daemon can own multiple independent roots. Local roots may not overlap each other, and selected remote paths may not overlap. Outside a `copy` projection, a complete root scan assigns one logical owner to each physical filesystem identity. `copy` deliberately relaxes that within its projected subtree so duplicate aliases and aliases back into the same selected root can coexist as independent lexical projections. Every `follow` symlink target (file or directory) is additionally reserved in shared SQLite authority before planning or mutation: the same current physical identity or canonical physical target pathname cannot be owned by two configured roots, and a followed target cannot overlap another configured local root. `copy` projections create no such global claim, but they still fail closed when their resolved target overlaps another configured local root or crosses a foreign root marker. Ordinary cross-root bind-mount or hard-link overlap that does not pass through these symlink policies remains unsupported rather than globally enumerated. v0.1 intentionally uses one app-owned PKU Disk authentication profile named `pkudisk`; a config alias is not treated as account identity.

Each root has its own:

- committed baseline;
- durable operation intents;
- unresolved conflicts;
- local ownership marker;
- polling state;
- enabled/paused state;
- initialization state;
- symlink policy;
- durable physical ownership claims for followed symlink targets;
- operation-local pinned local-mutation authority for lexical, followed-physical, and copy-physical destinations.

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

Every external mutation is represented by a durable operation record before execution. Operations move through semantic phases such as `planned`, `running`, `recovering`, and `blocked`. For local mutations, `running` is deliberately not synonymous with "the coordinator started calling the executor": the operation stays `planned` while first-attempt lexical/pin/precondition checks run, and is durably advanced to `running` only immediately before the executor may mutate user data. A known failure before that boundary discards the stale planned pin and requires a fresh scan/replan; only an operation that crossed the boundary is treated as a possibly-unknown outcome.

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

### Symlink projections and physical local targets

The synchronization namespace remains lexical even when a root uses the default `follow` policy, but a local mutation is applied to the resolved physical target rather than replacing the symlink object. A followed target may live outside the selected root or on a different filesystem. A complete scan first reserves every followed file/directory target as a daemon-wide physical claim in SQLite before planning. Physical directory ownership is hierarchical: claiming a canonical directory target owns that entire canonical pathname subtree, so another root cannot separately claim that directory, any descendant file/directory, or a parent directory that would contain it. Exact physical identity checks remain in place for aliases that pathname containment cannot prove. A followed directory and a configured local root are also rejected on overlap in either direction. Ordinary cross-root bind-mount and hard-link overlap that is not reached through a followed symlink remains unsupported in v0.1.

Before a `follow` local create/update/delete can enter `running`, the executor re-resolves the current mutation path and reports every followed symlink boundary actually traversed. In one SQLite transaction the store verifies those boundaries against the scan-time durable claims and global subtree ownership, safely refreshes same-target file identity when allowed, and pins both the canonical absolute physical target and a physical anchor identity in the operation journal. Existing targets use the target object's identity as the anchor; absent targets use the physical parent directory. A retarget between scan and pin therefore cannot borrow another root's physical ownership even when the ordinary file fingerprint is equivalent. File staging and preservation slots are then created in the target's physical parent directory, keeping no-replace rename operations on the same filesystem.

`copy` uses the same lexical namespace and source-byte projection as rclone `--copy-links`, but it intentionally does not acquire global physical ownership. A final copied symlink is pinned with `lexical` authority: a local delete removes only that symlink object, and a remote file update materializes a regular file at the lexical pathname. A descendant reached through a copied directory symlink is pinned with `copy-physical` authority to the exact resolved referent destination for that operation. The current configured peer roots and foreign root-marker fence are checked again while resolving this pin. Duplicate/internal copy projections remain valid because the pin is operation-local rather than a durable claim.

The journal therefore distinguishes `lexical`, `follow-physical`, and `copy-physical` local authority. Before the **first** user-data side effect, the executor still revalidates the current logical path against the newly chosen pin, so a pin-to-side-effect retarget fails closed. A final `copy` symlink is also resolved at pin time for fencing only: its referent must still be available, supported, non-cyclic, outside peer roots, and outside foreign root markers, even though the mutation target itself remains the lexical symlink object. The coordinator then durably enters `running` at the side-effect boundary. Once that boundary has been crossed, the journaled destination becomes the only recovery authority: postcondition observation, safe-replay preconditions, recovery content comparison, and replay use the pinned physical path/anchor and do not re-resolve the current alias. This prevents a concurrent `copy` directory retarget from redirecting an unknown-outcome recovery to a different referent. A later complete scan observes the retarget normally after the prior journal is resolved.

Hierarchical ownership checks are serialized under SQLite writer authority before their read-check-write sequence. Scan-time claim reservation, mutation-time claim refresh/pin, and sync-root creation therefore cannot concurrently observe an unclaimed parent/child namespace and both commit ownership. Exact-identity/target triggers remain defense-in-depth for equality conflicts; subtree conflicts are enforced by the serialized authority transaction.

Immediately before each first-attempt local side effect, and again after a potentially long download before commit, the executor verifies the durable physical anchor identity together with the current logical-to-pinned target fence. During unknown-outcome recovery it verifies that same durable anchor and pinned precondition **without** consulting a possibly retargeted lexical alias. This covers pin-to-mutation retargets and same-path inode/file-ID replacement, including replacement of the parent of an absent target. For replacement or deletion of an existing target, the executor then atomically moves the exact expected target to an operation-ID-specific recovery slot and validates the state of the object that actually moved. Ordinary targets validate fingerprint plus physical identity; final copied symlinks validate both the raw `readlink` identity and the projected referent fingerprint after the move, so a referent-only change cannot slip through the final CAS window. During crash recovery the syncer first proves whether the desired postcondition already holds at the pinned destination. If it does, a stale recovery slot is removed only when it still has the operation's pinned identity before the journal is committed; a replaced or ambiguous slot is preserved and fails closed. When completion cannot be proven, a preserved recovery artifact blocks automatic replay. Explicit restore likewise verifies the artifact identity for v6+ operations and rolls an unexpected moved object back into the recovery slot. The v6→v7 migration does not guess missing lexical-vs-follow authority: unattempted planned local pins are discarded for fresh v7 resolution, while already-started legacy pins remain inspectable but authority-less and therefore block automatic replay.

`ignore` is modeled as an excluded namespace rather than absence. Reconciliation skips the excluded prefix and descendants entirely, including deletion-gate accounting. The scanner applies this policy from `Lstat` before resolving the symlink, so an ignored link never gains authority merely because its target is another root or temporarily unavailable. `reject` instead makes the local observation incomplete by returning an error. `follow` and `copy` cycles or aliases to a physical ancestor are excluded so recursive traversal cannot loop.

A followed link whose final referent is missing is non-authoritative for that lexical prefix. A single `ENOENT` cannot distinguish an intentional referent delete from a temporarily unavailable external mount, so dangling followed targets never grant remote deletion authority. Every followed file/directory stores a durable global ownership claim. For followed **directories**, successful initial pairing fixes the platform physical identity (device+inode on Linux/macOS; volume+file ID on Windows) and the canonical target subtree; later scans must prove the same identity before descendants regain deletion authority. A different directory identity at the same pathname, a parent/descendant subtree overlap, or a newly introduced followed path on an initialized root blocks reconciliation and requires deliberate re-pairing. For followed **files**, normal atomic-save patterns may replace the file inode/file-ID, so the current identity may advance only while the canonical target pathname remains the same and global identity/path/subtree ownership remains uncontested. An unavailable followed target remains excluded while its prior durable ownership claim is retained.

Outside a `copy` projection, complete scans enforce one logical owner per physical file/directory identity within a root scan. Sibling aliases, hard links to the same file, or a `follow` expansion that would claim an object already reached through another logical path fail closed instead of creating independent baselines/journals for one physical object. Inside a copy-projection subtree that deduplication is intentionally bypassed, allowing duplicate and internal aliases to form separate lexical projections. Followed symlink targets retain the additional daemon/store-wide subtree claim described above, so two roots cannot independently follow a parent/descendant pair such as `/shared` and `/shared/subdir` or `/shared/file.txt`. Copy projections still may not cross into another configured root. The implementation does not globally enumerate arbitrary non-symlink bind-mount/hard-link overlap between separately selected roots.

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

Watchers recurse only through real directories under the selected local root; they never follow symlink targets. This is deliberate: following links would create a second, mutable watcher graph over arbitrary external directories and platform-specific reparse behavior. The authoritative repair scan still dereferences `follow` and `copy` links according to their policies, so changes under an external target are eventually observed without making watcher coverage part of correctness.

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
