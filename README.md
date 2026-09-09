# pkudisk-sync

Cross-platform, stateful, bidirectional synchronization for selected PKU Disk directories on Linux, macOS, and Windows.

`pkudisk-sync` embeds rclone and [`rclone-pkudisk`](https://github.com/rijuyuezhu/rclone-pkudisk) directly as Go libraries and manages synchronization state itself. No separate rclone process is required.

> **Status:** current development version is `0.1.0-alpha.2`.

## Features

- Synchronize multiple explicitly selected local/PKU Disk directory pairs.
- Three-way reconciliation with conflict preservation and deletion safeguards.
- Per-root symlink policy: `follow` by default, Seafile-style `copy` projection, or explicit `reject`/`ignore`.
- Filesystem watching plus periodic repair scans.
- Native per-user background service support on Linux, macOS, and Windows.

A sync root looks like:

```text
~/Documents  <->  pkudisk:Personal/Documents
```

## Quick start

Prebuilt alpha archives are published on the GitHub Releases page. To build from source instead, use the repository-pinned Go toolchain:

```bash
git clone https://github.com/rijuyuezhu/pkudisk-sync.git
cd pkudisk-sync
GOTOOLCHAIN=go1.26.8 go build -o ./pkudisk-sync ./cmd/pkudisk-sync
```

Configure PKU Disk authentication and add a directory pair:

```bash
./pkudisk-sync remote configure
./pkudisk-sync root add --local ~/Documents --remote pkudisk:Personal/Documents
./pkudisk-sync root list
```

Run synchronization in the foreground:

```bash
./pkudisk-sync daemon
```

Or install it as a current-user background service:

```bash
./pkudisk-sync service install
./pkudisk-sync service start
```

## Documentation

- [Operations guide](docs/operations.md) — configuration, root management, conflicts, services, and recovery.
- [Architecture](docs/architecture.md) — reconciliation, persistence, safety invariants, and crash recovery.
- [Development guide](docs/development.md) — code structure, testing, and validation.
- [Release process](docs/releasing.md) — release artifacts and verification.
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)

## License

MIT. See [LICENSE](LICENSE).
