#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: $0 ARCHIVE GOOS/GOARCH RELEASE SOURCE_COMMIT GO_VERSION" >&2
  exit 2
fi

archive=$1
target=$2
release=$3
source_commit=$4
expected_go=$5
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

if [[ ! -f $archive ]]; then
  echo "release archive does not exist: $archive" >&2
  exit 1
fi
if ! grep -Fxq -- "$target" "$root/scripts/release-targets.txt"; then
  echo "unsupported release target: $target" >&2
  exit 1
fi
if [[ -z $release || -z $source_commit || -z $expected_go ]]; then
  echo "release, source commit and Go version must not be empty" >&2
  exit 1
fi
for command in unzip go; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "required command is unavailable: $command" >&2
    exit 1
  fi
done

goos=${target%%/*}
goarch=${target#*/}
package="pkudisk-sync-${release}-${goos}-${goarch}"
binary_name=pkudisk-sync
if [[ $goos == windows ]]; then
  binary_name+=".exe"
fi

if [[ $(basename -- "$archive") != "$package.zip" ]]; then
  echo "release archive name does not match target: $(basename -- "$archive")" >&2
  exit 1
fi

stage=$(mktemp -d "${TMPDIR:-/tmp}/pkudisk-sync-verify-XXXXXX")
trap 'rm -rf -- "$stage"' EXIT
entries="$stage/entries.txt"
expected="$stage/expected.txt"

unzip -tq "$archive" >/dev/null
unzip -Z1 "$archive" | LC_ALL=C sort > "$entries"
printf '%s\n' \
  "$package/" \
  "$package/BUILDINFO.txt" \
  "$package/README.md" \
  "$package/$binary_name" | LC_ALL=C sort > "$expected"
if ! diff -u "$expected" "$entries"; then
  echo "release archive contains an unexpected file set" >&2
  exit 1
fi

unzip -q "$archive" -d "$stage/unpacked"
package_dir="$stage/unpacked/$package"
if find "$package_dir" -type l -print -quit | grep -q .; then
  echo "release archive must not contain symlinks" >&2
  exit 1
fi
for file in BUILDINFO.txt README.md "$binary_name"; do
  if [[ ! -f "$package_dir/$file" ]]; then
    echo "release archive member is not a regular file: $file" >&2
    exit 1
  fi
done
if [[ $goos != windows && ! -x "$package_dir/$binary_name" ]]; then
  echo "release binary is not executable after extraction: $binary_name" >&2
  exit 1
fi

buildinfo="$package_dir/BUILDINFO.txt"
grep -Fxq "release: $release" "$buildinfo" || {
  echo "BUILDINFO release does not match $release" >&2
  exit 1
}
grep -Fxq "target: $target" "$buildinfo" || {
  echo "BUILDINFO target does not match $target" >&2
  exit 1
}
grep -Fxq "source-commit: $source_commit" "$buildinfo" || {
  echo "BUILDINFO source commit does not match $source_commit" >&2
  exit 1
}
grep -Fq "go: go version go${expected_go} " "$buildinfo" || {
  echo "BUILDINFO Go toolchain does not match go${expected_go}" >&2
  exit 1
}

binary_version=$(go version "$package_dir/$binary_name")
binary_go=${binary_version##* }
if [[ $binary_go != "go${expected_go}" ]]; then
  echo "binary Go toolchain does not match go${expected_go}: $binary_version" >&2
  exit 1
fi

module_info="$stage/module-info.txt"
go version -m "$package_dir/$binary_name" > "$module_info"
grep -Fxq $'\tbuild\tGOOS='"$goos" "$module_info" || {
  echo "binary GOOS does not match $goos" >&2
  exit 1
}
grep -Fxq $'\tbuild\tGOARCH='"$goarch" "$module_info" || {
  echo "binary GOARCH does not match $goarch" >&2
  exit 1
}

printf 'verified %s (%s, %s)\n' "$archive" "$target" "$source_commit"
