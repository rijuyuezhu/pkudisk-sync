# Release process

No public release is published yet. This document records the intended release process so packaging changes can be reviewed before the first tag is created.

## Version authority

- `VERSION` contains the product version without a leading `v`.
- `.go-version` contains the exact Go release toolchain.
- `scripts/release-targets.txt` is the target matrix authority.
- `scripts/build-release.sh` creates one portable archive.
- `scripts/verify-release.sh` verifies archive contents and provenance.

Release tags use `v<version>`, for example `v0.1.0`.

## Pre-release checklist

Before creating a tag:

```bash
GOTOOLCHAIN="go$(cat .go-version)" go test ./...
GOTOOLCHAIN="go$(cat .go-version)" go vet ./...
GOTOOLCHAIN="go$(cat .go-version)" go test -race ./...
GOTOOLCHAIN="go$(cat .go-version)" go mod tidy -diff
git diff --check
```

If available, also run `golangci-lint run ./...` with the pinned toolchain.

Then build and verify every target in `scripts/release-targets.txt`. Verification must use the exact clean source commit; a `-dirty` provenance string is appropriate for candidate testing but not for a published artifact.

For service-manager changes, also run the affected native lifecycle test. Cross-compilation alone is not sufficient evidence that systemd, launchd, or Windows Task Scheduler behavior is correct.

## Artifact contents

Each portable archive should contain only the expected release files and an executable built for the target tuple. Verification rejects unexpected entries, unsafe paths/symlinks, metadata mismatches, and a binary built by a Go toolchain different from `.go-version`.

The aggregate release workflow also publishes `SHA256SUMS` covering the target archives.

## Signing status

v0.1 packaging is currently unsigned. A future public release should clearly disclose this until code signing/notarization is added.

Consequences include:

- macOS Gatekeeper may require explicit user approval;
- Windows SmartScreen may warn, while Smart App Control or enterprise policy may refuse the binary;
- users should verify `SHA256SUMS` before approving an unsigned download.

Do not advise users to disable Gatekeeper, SmartScreen, or equivalent protections globally.

## GitHub workflow

`.github/workflows/release.yml` is intended to run only for a release tag and to build the same six-target matrix from repository authorities. Creating the repository or pushing `main` must not create a release by itself.

The first public release should be a separate deliberate operation after documentation, CI, native service gates, and artifact provenance have all been reviewed from the published repository.
