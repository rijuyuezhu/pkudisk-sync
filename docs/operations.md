# Operations guide

This guide covers day-to-day configuration, recovery, and native background-service behavior. The current public pre-release is `v0.1.0-alpha.2`; newer development commits may still require building from source until another release is published.

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

- `follow` — dereference file and directory symlinks into the synchronized virtual namespace, including links whose targets are outside the selected local root. Remote updates and deletes mutate the resolved target while preserving the symlink object. A dangling/unavailable final referent is excluded rather than treated as local deletion evidence. Every followed file/directory target first reserves daemon-wide physical ownership in SQLite. A followed **directory** owns its complete canonical physical pathname subtree, so another root cannot separately follow that directory, a descendant directory/file, or a parent directory that would contain it; configured local roots are checked against followed directory subtrees in both directions. Immediately before a local mutation is pinned, the current symlink chain is resolved again and checked atomically against those durable claims; a retarget to another root's physical target therefore fails closed even when the normal fingerprint is equivalent. The operation also pins a physical target/parent identity that is rechecked immediately before the side effect. Followed directory boundaries additionally keep an immutable physical identity for the pairing; if the same logical boundary later resolves to a different directory object, synchronization blocks rather than inferring descendant deletions. Followed files keep ownership of their canonical target pathname while allowing the current inode/file-ID to advance for ordinary atomic-save replacement before a mutation is pinned, provided global identity/path/subtree ownership remains uncontested. Cycles and links back to a physical ancestor are excluded instead of traversed. Arbitrary non-symlink cross-root bind-mount/hard-link overlap remains unsupported in v0.1.
- `copy` — expose each symlink as a Seafile/rclone-style **projection** of its referent. File uploads and content comparison read referent bytes; directory symlinks expose a projected subtree. Duplicate aliases and aliases back into the same selected root are intentionally allowed as independent lexical projections, so a copy-projection subtree bypasses physical deduplication and creates no daemon-wide `FollowedPhysicalClaim`. Copy does **not** grant unrestricted access to arbitrary storage: dangling/unavailable targets and cycles are excluded, and targets that overlap another configured local root or cross a foreign `.pkudisk-sync-root` marker fail closed. A complete scan also records non-owning evidence for each copied boundary (kind, physical identity, canonical target pathname). Before an operation can be pinned, that exact boundary evidence must still match; a retarget between scan and pin therefore invalidates the old plan even if the projected fingerprint happens to be identical. If an unpinned copy local operation survives into a later cycle after its scan evidence has been lost, it is discarded for a fresh complete scan instead of reconstructing authority from fingerprints. Mutation-time peer-root fences are checked again, including for a **final** symlink; resolving the final referent is validation only, while the mutation target remains the lexical symlink object. Deleting that projected entry removes only the symlink, while a remote file update replaces it with a regular materialized file. Descendants reached through a copied directory symlink instead get operation-local `copy-physical` authority: immediately before mutation the current alias is resolved and the exact physical destination/anchor is journaled, without acquiring global ownership. While that pin remains in the operation journal it also temporarily fences creation of any overlapping configured sync root, and the pin transaction conversely rechecks roots before committing; this operation-lifetime arbitration is released when the operation ends and does not become root-lifetime ownership. Before the first user-data side effect, pathname continuity and the pinned physical anchor are both rechecked. Parent spelling is canonicalized for native aliases, but distinct hard-link leaf pathnames remain distinct mutation destinations. A known failure there keeps the operation out of unknown-outcome recovery and requires a fresh scan/replan. Once the durable `running` boundary has actually been crossed, postcondition proof, recovery, replay, and file-content proof use only the journaled physical destination and never re-resolve the current alias. Final-symlink replacement/delete additionally revalidates the projected referent fingerprint after atomically preserving the lexical symlink, closing the last check-to-mutation window. A later complete scan treats any retarget as fresh logical state. Whole-projection local deletes execute descendants before directories and remove the lexical projection-root symlink last; the normal plan-level mass-delete gate counts the complete delete plan and blocks before any journal or filesystem mutation.
- `reject` — any symlink makes the complete local scan fail closed.
- `ignore` — the symlink path and its virtual subtree are excluded from the local namespace for that cycle. Excluded paths carry no download or deletion authority, so ignoring a link cannot be mistaken for deleting it.

Changing among `follow`, `reject`, and `ignore` requires a paused root with no pending operation. `copy` changes pairing semantics more deeply: once a root is initialized, any transition **to or from** `copy` is rejected and requires deliberate remove/re-add (re-pairing). An uninitialized paused root with no pending operation may still be configured as `copy` directly.

```bash
pkudisk-sync root pause 1
pkudisk-sync root config 1 --symlinks ignore
pkudisk-sync root resume 1
```

Filesystem watchers deliberately do not follow symlink targets. In particular, a target outside the selected root does not expand the watcher's ownership into another directory tree. Changes under `follow` or `copy` projections are discovered by the authoritative periodic repair scan; configure a shorter `--poll` interval when lower detection latency is needed.

If a followed directory reports a physical-identity change, first verify whether an external mount disappeared or was replaced. Do not resume automatic sync against the replacement just because the pathname is unchanged. If the replacement is intentional and there are no pending operations, deliberately re-pair the root: record its local/remote selection from `root list`, pause it, stop the daemon/service, `root remove ID`, then add the same pair again. Detach/re-add does not delete local or remote user data; it creates a fresh initial pairing and therefore a new durable boundary identity. If pending operations still exist, do **not** re-pair around them: restore the original physical boundary so those journaled operations can be completed or safely proven first, or leave them blocked for explicit inspection.

If a cycle blocks because another root already owns a followed physical target, choose exactly one configured root to own that target and remove/reconfigure the duplicate mapping. Do not edit SQLite to transfer ownership; claim rows are part of synchronization authority and are released automatically when the owning root is removed.

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
pkudisk-sync operation list
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

### Blocked operation recovery

When crash recovery cannot prove a safe automatic action, the durable operation becomes `blocked`. Inspect the exact intent and diagnostic first:

```bash
pkudisk-sync operation list --root 1
pkudisk-sync operation show 42
```

`operation show` includes the semantic operation, pinned physical local target, deterministic recovery-artifact location (when applicable), attempts, and the last recovery error.

Stop the foreground daemon/user service before an explicit recovery action. `--retry` does **not** blindly replay the mutation; it returns the blocked intent to the normal guarded recovery state machine, which must again prove the previous postcondition or validate that retry preconditions still hold:

```bash
pkudisk-sync operation resolve 42 --retry
```

If `operation show` reports that pre-mutation local data is preserved in a recovery artifact, restore it only with:

```bash
pkudisk-sync operation resolve 42 --restore-recovery
```

This action requires the artifact to still match the journaled expected local fingerprint, requires the pinned target to be absent, restores with no-replace rename semantics, verifies the restored target, and then returns the operation to guarded recovery. It never overwrites a newly created target. There is intentionally no `operation delete` escape hatch. Conversely, if restart can already prove that the desired current target/postcondition was installed before the crash, the daemon removes the operation-owned stale recovery artifact and commits the journal automatically; the presence of an old recovery slot alone no longer forces a permanent block.

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
6. If `BLOCKED` is non-zero, use `pkudisk-sync operation list` and `operation show ID` before taking action.
7. Do not manually remove operation/conflict rows from SQLite to "unstick" a root; those records are part of crash-safety authority.

For semantic details, see [architecture.md](architecture.md).
