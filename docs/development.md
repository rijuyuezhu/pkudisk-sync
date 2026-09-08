# Development guide

## Toolchain

The repository pins its release and CI Go toolchain in `.go-version`. Use that exact version when validating release-sensitive changes:

```bash
GOTOOLCHAIN=go1.26.8 go test ./...
GOTOOLCHAIN=go1.26.8 go vet ./...
GOTOOLCHAIN=go1.26.8 go test -race ./...
```

`go mod tidy -diff` should also be clean.

If `golangci-lint` is installed:

```bash
GOTOOLCHAIN=go1.26.8 golangci-lint run ./...
```

## Architecture boundaries

Keep these boundaries explicit:

- `domain` owns semantic types and validation.
- `store` owns durable SQLite authority.
- `reconcile` is pure planning/recovery policy where practical.
- `executor` is the only layer that translates plans into local/rclone observations and mutations.
- `syncer` journals and executes plans, then commits observed convergence.
- `watcher` supplies hints only.
- `daemon` schedules roots and owns worker lifecycle.
- `userservice` adapts the daemon to native per-user service managers.

Do not add a subprocess or `rcd` boundary around rclone. `pkudisk-sync` is intentionally a Go downstream of rclone/`rclone-pkudisk`.

## Correctness rules for changes

A change that mutates external state should answer all of these questions:

1. What exact local/remote observation authorizes the mutation?
2. Is the mutation represented durably before it can happen?
3. What happens if the process dies after the external effect but before SQLite commit?
4. How is a stale precondition rejected?
5. Can an incomplete scan be mistaken for deletion?
6. Does a paused/resumed worker preserve the same crash-recovery semantics?

Avoid adding a special "fast path" that bypasses these rules. KISS here means one semantic path, not fewer safety checks.

## Tests

Prefer tests at the narrowest meaningful layer:

- reconciliation tables for planner policy;
- store tests for transactions and durable state transitions;
- executor tests for filesystem/remote preconditions;
- syncer tests for journal/recovery behavior;
- daemon tests for worker scheduling, cancellation, polling, and watcher hints;
- user-service tests for native service definitions and status normalization.

Correctness bugs should get a regression test that fails for the original behavior.

## Cross-platform builds

The supported portable targets are listed in `scripts/release-targets.txt`:

- linux/amd64
- linux/arm64
- darwin/amd64
- darwin/arm64
- windows/amd64
- windows/arm64

Build one target with:

```bash
./scripts/build-release.sh linux/amd64 ./dist
```

The verifier checks archive structure, build metadata, source commit, platform, and the actual Go toolchain embedded in the executable:

```bash
./scripts/verify-release.sh \
  ./dist/pkudisk-sync-v0.1.0-linux-amd64.zip \
  linux/amd64 \
  v0.1.0 \
  "$(git rev-parse HEAD)" \
  "$(cat .go-version)"
```

See [releasing.md](releasing.md) for the complete release checklist.

## Native service validation

Cross-compilation is not enough for filesystem or service-manager changes. CI runs the full Go test suite natively on Linux, macOS, and Windows; the Windows job additionally exercises the per-user Scheduled Task lifecycle. Release-target cross-compilation remains a separate packaging/provenance check.

For Windows, the expected product lifecycle is:

```text
not-installed -> inactive -> active -> inactive -> not-installed
```

The Scheduled Task must be current-user, Interactive logon, and Limited privilege. Product status must remain locale-independent.

For Linux/macOS, verify that service installation resolves the same native state/config/runtime authority as the supervised `daemon --service` process.

## Git workflow

Use `main` as the stable development branch and short-lived topic branches for larger changes. Keep commits reviewable and avoid mixing formatting/docs cleanup with unrelated semantic changes unless the cleanup is necessary for the change.
