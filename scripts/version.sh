#!/usr/bin/env bash
set -euo pipefail

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
version=$(tr -d '[:space:]' < "$root/VERSION")

if [[ ! $version =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
  echo "invalid VERSION: $version" >&2
  exit 1
fi

case "${1:-}" in
  version)
    printf '%s\n' "$version"
    ;;
  release|tag)
    printf 'v%s\n' "$version"
    ;;
  *)
    echo "usage: $0 {version|release|tag}" >&2
    exit 2
    ;;
esac
