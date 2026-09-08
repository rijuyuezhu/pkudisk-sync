# Release process

This document defines the release process for stable and pre-release builds.

## Version authority

- `VERSION` contains the product version without a leading `v`.
- `.go-version` contains the exact Go release toolchain.
- `scripts/release-targets.txt` is the target matrix authority.
- `scripts/build-release.sh` creates one portable archive.
- `scripts/verify-release.sh` verifies archive contents and provenance.

Release tags use `v<version>`. Pre-releases follow SemVer identifiers such as `v0.1.0-alpha.1`, then `beta.1`, `rc.1`, and finally `v0.1.0`.

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

For service-manager changes, also run the affected native lifecycle test before tagging when that platform is available. Cross-compilation alone is not sufficient evidence that systemd, launchd, or Windows Task Scheduler behavior is correct. The GitHub release workflow provides the authoritative publish gate: metadata captures the tag-push event commit, verifies that the current tag still names that commit and that it is on `main`, then every quality/native/build/publish job checks out that immutable commit rather than resolving the tag name again. It always reruns macOS `go test ./...` and Windows `go test ./...` plus the Scheduled Task lifecycle before release archives are allowed to build.

## Artifact contents

Each portable archive should contain only the expected release files and an executable built for the target tuple. Verification rejects unexpected entries, unsafe paths/symlinks, metadata mismatches, and a binary built by a Go toolchain different from `.go-version`.

The aggregate release workflow also publishes `SHA256SUMS` covering the target archives.

## Signing status

v0.1 packaging is currently unsigned. Public pre-releases and releases should clearly disclose this until code signing/notarization is added.

Consequences include:

- macOS Gatekeeper may require explicit user approval;
- Windows SmartScreen may warn, while Smart App Control or enterprise policy may refuse the binary;
- users should verify `SHA256SUMS` before approving an unsigned download.

Do not advise users to disable Gatekeeper, SmartScreen, or equivalent protections globally.

## GitHub workflow

`.github/workflows/release.yml` runs only for a release tag. It validates tag metadata/main ancestry, records the validated commit, runs the Ubuntu quality gate and independent native macOS/Windows gates from that exact commit, and only then builds the six-target archive matrix from the same commit. Artifact verification uses that validated commit as provenance authority. Immediately before creating the GitHub Release, the workflow force-refreshes the tag and refuses publication if it no longer resolves to the validated commit. Release safety therefore does not depend on tag/branch protection or on an earlier `main` CI run. Creating the repository or pushing `main` does not create a release by itself.

When `VERSION` contains a pre-release suffix such as `-alpha.1`, the workflow publishes the GitHub Release with the pre-release flag. A plain version such as `0.1.0` publishes a normal release.
