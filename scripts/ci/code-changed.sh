#!/usr/bin/env bash
# Prints code=true when a pull request changes anything besides documentation
# (docs/, Markdown files, recorded benchmark artifacts), and code=false when it
# changes documentation only. Pushes and unreadable diffs count as code.
set -euo pipefail

if [[ "${GITHUB_EVENT_NAME:-}" != "pull_request" ]]; then
    echo "code=true"
    exit 0
fi
# The pull_request checkout is the merge commit: its first parent is the base.
if ! changed=$(git diff --name-only HEAD^1 HEAD); then
    echo "code=true"
    exit 0
fi
code=false
while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    case "$path" in
        docs/*|*.md|bench/artifacts/*) ;;
        *) code=true ;;
    esac
done <<< "$changed"
if [[ -z "$changed" ]]; then
    code=true
fi
echo "code=$code"
