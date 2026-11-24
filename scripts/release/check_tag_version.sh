#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <tag>" >&2
  exit 1
fi

TAG="$1"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

version="$(sed -n 's/^project([^)]*VERSION \([0-9][0-9.]*\).*/\1/p' "${ROOT}/CMakeLists.txt" | head -n 1)"
if [[ -z "${version}" ]]; then
  echo "could not read the project VERSION from CMakeLists.txt" >&2
  exit 1
fi

if [[ "${TAG}" != "v${version}" ]]; then
  echo "tag ${TAG} does not match the build version ${version} in CMakeLists.txt (expected v${version})" >&2
  exit 1
fi

echo "tag ${TAG} matches build version ${version}"
