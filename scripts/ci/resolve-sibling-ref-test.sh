#!/usr/bin/env bash
set -euo pipefail

resolver=$(cd "$(dirname "$0")" && pwd)/resolve-sibling-ref.sh
export SIBLING_TEST_GIT=$(command -v git)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
export SIBLING_TEST_LOG="$scratch/arguments"
cat > "$scratch/git" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
if [[ $1 == check-ref-format ]]; then
    exec "$SIBLING_TEST_GIT" "$@"
fi
[[ $# == 5 && $1 == ls-remote && $2 == --exit-code && $3 == --heads ]]
printf '%s\n' "$4" "$5" >> "$SIBLING_TEST_LOG"
exit "$SIBLING_TEST_STATUS"
MOCK
chmod +x "$scratch/git"
export PATH="$scratch:$PATH"
export SIBLING_TEST_STATUS=0
candidate='topic/$(touch${IFS}unexpected)'
[[ $(bash "$resolver" https://example.invalid/sibling.git "$candidate") == "ref=$candidate" ]]
[[ $(tail -n 1 "$SIBLING_TEST_LOG") == "refs/heads/$candidate" ]]
[[ ! -e unexpected ]]
export SIBLING_TEST_STATUS=2
[[ $(bash "$resolver" https://example.invalid/sibling.git absent) == ref=main ]]
export SIBLING_TEST_STATUS=128
if bash "$resolver" https://example.invalid/sibling.git topic > "$scratch/output" 2> "$scratch/error"; then
    echo 'transport failure incorrectly succeeded' >&2; exit 1
else
    [[ $? == 128 && ! -s "$scratch/output" ]]
fi
if bash "$resolver" https://example.invalid/sibling.git 'invalid branch' > "$scratch/output" 2> "$scratch/error"; then
    echo 'invalid branch incorrectly succeeded' >&2; exit 1
fi
[[ $(bash "$resolver" https://example.invalid/sibling.git '') == ref=main ]]
echo 'sibling reference checks passed'
