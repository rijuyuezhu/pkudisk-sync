# pkudisk-sync

Cross-platform, stateful, bidirectional synchronization for selected PKU Disk directories on Linux, macOS, and Windows.

`pkudisk-sync` directly embeds rclone and [`rclone-pkudisk`](https://github.com/rijuyuezhu/rclone-pkudisk) as Go libraries. It does **not** assemble an internal rclone CLI/`rcd` pipeline: this project owns synchronization state, three-way reconciliation, conflict policy, deletion safety, crash recovery, filesystem watching, and the long-running daemon.

> **Status:** v0.1 is a pre-release candidate. The synchronization core, native per-user services, six-target build pipeline, real PKU Disk smoke tests, and native Windows service lifecycle have been validated, but no GitHub Release has been published yet.

## What it does

One daemon can synchronize multiple explicitly selected directory pairs:

```text
<absolute local directory>  <->  pkudisk:<remote directory>
```

For example:

```text
~/Seafile/Data  <->  pkudisk:Personal/Data
~/Seafile/Work  <->  pkudisk:Personal/Work
~/Notes         <->  pkudisk:Personal/Notes
```

Each root has independent baseline, operation-journal, conflict, marker, polling, and pause/resume state. Local roots cannot overlap one another, and selected remote roots cannot overlap one another.

The implementation includes:

- three-way reconciliation: committed baseline vs current local vs current remote;
- non-destructive initial pairing;
- durable operation intents and postcondition-based crash recovery;
- guarded create/update/download/delete through the embedded PKU Disk backend;
- explicit conflict preservation and file-conflict resolution;
- mass-delete safety gates;
- recursive local watchers as hints plus periodic full repair scans;
- multiple independently selected roots in one daemon;
- native current-user services on Linux, macOS, and Windows;
- portable build targets for amd64/arm64 on all three platforms.

## Quick start

### 1. Build from source

The repository pins Go 1.26.8 in `.go-version`.

```bash
git clone https://github.com/rijuyuezhu/pkudisk-sync.git
cd pkudisk-sync
GOTOOLCHAIN=go1.26.8 go build -o ./pkudisk-sync ./cmd/pkudisk-sync
./pkudisk-sync version
```

Move the resulting executable to a stable per-user location before installing a background service. Until the first release is published, source builds are the supported installation path.

### 2. Configure PKU Disk authentication

`pkudisk-sync` owns a dedicated rclone configuration instead of using the user's global rclone config:

```bash
pkudisk-sync paths
pkudisk-sync remote configure
```

`remote configure` creates or re-authenticates the single app-owned remote named `pkudisk` and runs the embedded rclone OAuth flow. No separate rclone or `rclone-pkudisk` executable is required.

### 3. Select directories

```bash
pkudisk-sync root add --local ~/Seafile/Data --remote pkudisk:Personal/Data
pkudisk-sync root list
pkudisk-sync status
```

The first pairing is non-destructive. If the selected remote directory does not yet exist, an uninitialized root can create it through the ordinary journaled synchronization path; after initialization, disappearance of that remote root is a hard error rather than a request to delete local data.

### 4. Run synchronization

Foreground:

```bash
pkudisk-sync daemon
```

Or install the same daemon as the current user's native background service:

```bash
pkudisk-sync service install
pkudisk-sync service start
pkudisk-sync service status
```

`service status` is normalized across platforms to `active`, `inactive`, or `not-installed`.

## Safety model

The project is intentionally conservative around destructive or ambiguous state:

- Watcher events are latency hints, never durable truth.
- Initial pairing never infers deletion from one-sided absence.
- A destructive plan requires complete observations, an initialized root, a healthy app-owned marker, baseline evidence, and mass-delete limits.
- A mutation is journaled before the external effect can happen.
- A `running` operation has unknown outcome after a crash and is never blindly replayed.
- Stale remote revisions and expected-absent collisions fail closed instead of overwriting newer data.
- True concurrent changes become durable conflicts; v0.1 does not use last-writer-wins.
- `root remove` detaches configuration only; it never deletes local or remote user data.

For the full reasoning and state machine, see [docs/architecture.md](docs/architecture.md).

## Conflicts and root lifecycle

```bash
pkudisk-sync conflict list
pkudisk-sync conflict list --root 1
pkudisk-sync conflict resolve 12 --keep-local
# or: pkudisk-sync conflict resolve 12 --keep-remote

pkudisk-sync root pause 1
pkudisk-sync root resume 1
```

A file-conflict resolution queues an ordinary guarded operation against the exact states recorded by the conflict. If either side changed in the meantime, the stale resolution is discarded instead of overwriting new data. Directory/subtree conflicts and file/directory kind mismatches remain manual in v0.1.

To detach a root without deleting either side, pause it and stop synchronization first:

```bash
pkudisk-sync root pause 1
pkudisk-sync service stop
pkudisk-sync root remove 1
```

See [docs/operations.md](docs/operations.md) for orphan-marker recovery, deletion thresholds, service details, authentication maintenance, and troubleshooting.

## Platform notes

Native background integration is per-user and does not require a system-wide daemon:

- Linux: `systemd --user`;
- macOS: LaunchAgent;
- Windows: current-user Scheduled Task with Interactive logon and Limited privileges.

Installed services deliberately use OS-native app paths rather than shell-selected path overrides so the service definition, runtime lease, SQLite state, and rclone configuration resolve to one authority.

No public binaries are signed/notarized yet. Signing, native installers, and richer GUI/tray UX are packaging/product follow-up rather than v0.1 synchronization requirements.

## Current scope

v0.1 deliberately keeps several boundaries narrow:

- one app-owned PKU Disk account profile, fixed as `pkudisk`;
- explicit selected-directory synchronization rather than whole-drive mirroring;
- file conflicts can be resolved through the CLI, while subtree namespace conflicts remain manual;
- no rclone subprocess/`rcd` composition layer;
- no public binary release yet.

## Documentation

- [Architecture and correctness model](docs/architecture.md)
- [Operations and recovery guide](docs/operations.md)
- [Development guide](docs/development.md)
- [Release process](docs/releasing.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)

## Development

```bash
GOTOOLCHAIN=go1.26.8 go test ./...
GOTOOLCHAIN=go1.26.8 go vet ./...
GOTOOLCHAIN=go1.26.8 go test -race ./...
```

The stable branch is `main`. Correctness fixes should include regression tests, and release-sensitive changes should validate all targets in `scripts/release-targets.txt`.

## License

MIT. See [LICENSE](LICENSE).
