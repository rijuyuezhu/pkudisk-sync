# Contributing

`pkudisk-sync` is intentionally small and correctness-first. Changes should preserve the synchronization authority model rather than adding convenience layers that duplicate rclone or PKU Disk semantics.

## Development setup

Use the repository-pinned Go toolchain:

```bash
GOTOOLCHAIN=go1.26.8 go test ./...
GOTOOLCHAIN=go1.26.8 go vet ./...
GOTOOLCHAIN=go1.26.8 go test -race ./...
```

If `golangci-lint` is available, also run:

```bash
GOTOOLCHAIN=go1.26.8 golangci-lint run ./...
```

See [docs/development.md](docs/development.md) for architecture boundaries and the full validation matrix.

## Change discipline

- Keep commits small and reviewable.
- Add regression tests for correctness fixes.
- Do not treat watcher events, timestamps, or remote aliases as durable identity.
- Do not bypass the operation journal for external filesystem or PKU Disk mutations.
- Preserve fail-closed behavior when remote observations are incomplete or ambiguous.
- Keep the embedded `rclone-pkudisk` backend as the transport/API authority; do not add a subprocess or internal `rcd` layer.

## Pull requests

A pull request should explain the invariant or product behavior being changed, include tests, and note any platform-specific behavior. Changes to release packaging should validate all targets listed in `scripts/release-targets.txt`.
