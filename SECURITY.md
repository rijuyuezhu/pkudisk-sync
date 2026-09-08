# Security policy

`pkudisk-sync` handles PKU Disk OAuth credentials and can mutate both local and remote files, so synchronization correctness and credential handling are security-sensitive.

## Reporting a vulnerability

Please do not open a public issue for a vulnerability that could expose credentials, bypass mutation preconditions, or cause unintended data loss. Use GitHub's private security-advisory reporting for this repository when available.

Include enough information to reproduce the problem, especially the platform, command or daemon mode, relevant root state, and whether the issue affects local files, remote files, credentials, or service installation.

## Credential handling

`pkudisk-sync` owns a dedicated rclone configuration rather than using the user's global rclone config. OAuth tokens must never be committed to this repository, pasted into issues, or included in test fixtures. Use `pkudisk-sync paths` to locate the active app-owned configuration when debugging locally.

## Supported versions

The project is currently preparing its first v0.1 release. Until a release is published, security fixes apply to the current `main` branch.
