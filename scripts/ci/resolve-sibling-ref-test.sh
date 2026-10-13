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
if [[ $SIBLING_TEST_STATUS == 0 ]]; then
    printf '%s\n' "$SIBLING_TEST_REFS"
elif [[ $SIBLING_TEST_STATUS != 2 ]]; then
    echo 'mock transport failure' >&2
fi
exit "$SIBLING_TEST_STATUS"
MOCK
chmod +x "$scratch/git"
export PATH="$scratch:$PATH"
export SIBLING_TEST_STATUS=0
sha=0123456789abcdef0123456789abcdef01234567
printf -v SIBLING_TEST_REFS '%s\trefs/heads/topic' "$sha"
export SIBLING_TEST_REFS
[[ $(bash "$resolver" https://example.invalid/sibling.git topic) == ref=topic ]]
printf -v SIBLING_TEST_REFS '%s\trefs/heads/archive/refs/heads/topic' "$sha"
[[ $(bash "$resolver" https://example.invalid/sibling.git topic) == ref=main ]]
printf -v SIBLING_TEST_REFS '%s\trefs/heads/archive/refs/heads/topic\n%s\trefs/heads/topic' "$sha" "$sha"
[[ $(bash "$resolver" https://example.invalid/sibling.git topic) == ref=topic ]]
candidate='topic/$(touch${IFS}unexpected)'
printf -v SIBLING_TEST_REFS '%s\trefs/heads/%s' "$sha" "$candidate"
[[ $(bash "$resolver" https://example.invalid/sibling.git "$candidate") == "ref=$candidate" ]]
[[ $(tail -n 1 "$SIBLING_TEST_LOG") == "refs/heads/$candidate" ]]
[[ ! -e unexpected ]]
export SIBLING_TEST_STATUS=2
[[ $(bash "$resolver" https://example.invalid/sibling.git absent) == ref=main ]]
for status in 1 128; do
    export SIBLING_TEST_STATUS=$status
    if bash "$resolver" https://example.invalid/sibling.git topic > "$scratch/output" 2> "$scratch/error"; then
        echo 'transport failure incorrectly succeeded' >&2; exit 1
    else
        [[ $? == "$status" && ! -s "$scratch/output" && -s "$scratch/error" ]]
    fi
done
if bash "$resolver" https://example.invalid/sibling.git 'invalid branch' > "$scratch/output" 2> "$scratch/error"; then
    echo 'invalid branch incorrectly succeeded' >&2; exit 1
fi
[[ $(bash "$resolver" https://example.invalid/sibling.git '') == ref=main ]]
echo 'sibling reference checks passed'
