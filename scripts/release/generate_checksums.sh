#!/usr/bin/env bash
set -euo pipefail

if [[ $# -gt 1 ]]; then
  echo "Usage: $0 [artifacts_dir]" >&2
  exit 1
fi

ARTIFACTS_DIR="${1:-$(pwd)}"

if [[ ! -d "${ARTIFACTS_DIR}" ]]; then
  echo "artifacts directory not found: ${ARTIFACTS_DIR}" >&2
  exit 1
fi

hash_file() {
  local file="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${file}" > "${file}.sha256"
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "${file}" > "${file}.sha256"
    return
  fi
  if command -v openssl >/dev/null 2>&1; then
    openssl dgst -sha256 "${file}" | sed 's/^SHA2-256(//; s/)= /  /' > "${file}.sha256"
    return
  fi
  echo "no SHA256 tool found (sha256sum/shasum/openssl)" >&2
  exit 1
}

files=()
while IFS= read -r file; do
  files+=("${file}")
done < <(find "${ARTIFACTS_DIR}" -maxdepth 1 -type f \
  \( -name '*.zip' -o -name '*.tar.gz' -o -name '*.tgz' \) | sort)

if [[ ${#files[@]} -eq 0 ]]; then
  echo "no release artifacts found in ${ARTIFACTS_DIR}" >&2
  exit 1
fi

for file in "${files[@]}"; do
  hash_file "${file}"
  echo "sha256 -> ${file}.sha256"
done
