#!/usr/bin/env bash
# Installs Debian packages on a CI runner without waiting on a stalled
# mirror: packages already present are skipped, every apt command has a time
# limit, and update plus install are tried at most three times.
set -euo pipefail

missing=()
for package in "$@"; do
  dpkg -s "$package" >/dev/null 2>&1 || missing+=("$package")
done
if [ "${#missing[@]}" -eq 0 ]; then
  echo "already installed: $*"
  exit 0
fi

options=(-o Acquire::Retries=3 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30)
for attempt in 1 2 3; do
  if sudo timeout 90 apt-get "${options[@]}" update &&
     sudo timeout 120 apt-get "${options[@]}" install -y --no-install-recommends "${missing[@]}"; then
    exit 0
  fi
  echo "::warning::apt attempt ${attempt} of 3 failed for: ${missing[*]}"
done
echo "::error::could not install: ${missing[*]}"
exit 1
