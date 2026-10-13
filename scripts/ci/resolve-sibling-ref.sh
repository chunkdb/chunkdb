#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 2 ]]; then
    echo "usage: resolve-sibling-ref.sh repository-url branch" >&2
    exit 1
fi
repository_url=$1
candidate=$2
ref=main
if [[ -n "$candidate" ]]; then
    git check-ref-format --branch "$candidate" >/dev/null
    if records=$(git ls-remote --exit-code --heads "$repository_url" "refs/heads/$candidate"); then
        # ls-remote patterns also match tails at slash boundaries. Only the
        # exact full returned ref establishes that this branch exists.
        while IFS=$'\t' read -r object remote_ref; do
            if [[ "$remote_ref" == "refs/heads/$candidate" ]]; then
                ref=$candidate
                break
            fi
        done <<< "$records"
    else
        status=$?
        if [[ $status != 2 ]]; then
            echo "Cannot resolve sibling branch: git ls-remote exited $status" >&2
            exit "$status"
        fi
    fi
fi
printf 'ref=%s\n' "$ref"
