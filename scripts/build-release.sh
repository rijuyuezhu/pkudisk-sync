#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 GOOS/GOARCH [OUTPUT_DIR]" >&2
  exit 2
fi

target=$1
output_dir=${2:-dist}
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

if ! grep -Fxq -- "$target" "$root/scripts/release-targets.txt"; then
  echo "unsupported release target: $target" >&2
  exit 1
fi

release=$("$root/scripts/version.sh" release)
goos=${target%%/*}
goarch=${target#*/}
source_commit=$(git -C "$root" rev-parse HEAD)
if [[ -n $(git -C "$root" status --porcelain) ]]; then
  source_commit+="-dirty"
fi
build_date=$(git -C "$root" show -s --format=%cI HEAD)
source_epoch=${SOURCE_DATE_EPOCH:-$(git -C "$root" show -s --format=%ct HEAD)}
package="pkudisk-sync-${release}-${goos}-${goarch}"
stage=$(mktemp -d "${TMPDIR:-/tmp}/pkudisk-sync-release-XXXXXX")
trap 'rm -rf -- "$stage"' EXIT
package_dir="$stage/$package"
mkdir -p "$package_dir" "$output_dir"
output_dir=$(cd -- "$output_dir" && pwd)

binary="$package_dir/pkudisk-sync"
if [[ $goos == windows ]]; then
  binary+=".exe"
fi

(
  cd "$root"
  env CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" \
    go build \
      -buildvcs=false \
      -trimpath \
      -ldflags "-s -w -X github.com/rijuyuezhu/pkudisk-sync/internal/buildinfo.Version=$release -X github.com/rijuyuezhu/pkudisk-sync/internal/buildinfo.Commit=$source_commit -X github.com/rijuyuezhu/pkudisk-sync/internal/buildinfo.BuildDate=$build_date" \
      -o "$binary" \
      ./cmd/pkudisk-sync
)

cp "$root/README.md" "$package_dir/"
{
  printf 'release: %s\n' "$release"
  printf 'target: %s\n' "$target"
  printf 'source-commit: %s\n' "$source_commit"
  printf 'build-date: %s\n' "$build_date"
  printf 'go: %s\n' "$(go version)"
  if module_info=$(go version -m "$binary" 2>/dev/null); then
    printf '\n'
    printf '%s\n' "$module_info" | sed '1s|^[^:]*:|binary:|'
  fi
} > "$package_dir/BUILDINFO.txt"

find "$package_dir" -exec touch -h -d "@$source_epoch" {} +
archive="$output_dir/$package.zip"
(
  cd "$stage"
  zip -X -9 -q -r "$archive" "$package"
)
printf '%s\n' "$archive"
