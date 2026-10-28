#!/usr/bin/env bash
set -euo pipefail

fail() { echo "$*" >&2; exit 1; }

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
[[ $# == 5 && $1 == ls-remote && $2 == --exit-code && $3 == --heads ]] || {
    echo 'unexpected Git lookup arguments' >&2; exit 1;
}
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
[[ $(bash "$resolver" https://example.invalid/sibling.git topic) == ref=topic ]] || fail 'exact ref was not selected'
printf -v SIBLING_TEST_REFS '%s\trefs/heads/archive/refs/heads/topic' "$sha"
[[ $(bash "$resolver" https://example.invalid/sibling.git topic) == ref=main ]] || fail 'nested-only ref was selected'
printf -v SIBLING_TEST_REFS '%s\trefs/heads/archive/refs/heads/topic\n%s\trefs/heads/topic' "$sha" "$sha"
[[ $(bash "$resolver" https://example.invalid/sibling.git topic) == ref=topic ]] || fail 'exact ref in mixed records was not selected'
candidate='topic/$(touch${IFS}unexpected)'
printf -v SIBLING_TEST_REFS '%s\trefs/heads/%s' "$sha" "$candidate"
[[ $(bash "$resolver" https://example.invalid/sibling.git "$candidate") == "ref=$candidate" ]] || fail 'literal branch changed'
[[ $(tail -n 1 "$SIBLING_TEST_LOG") == "refs/heads/$candidate" ]] || fail 'lookup ref was not exact'
[[ ! -e unexpected ]] || fail 'literal branch text executed'
export SIBLING_TEST_STATUS=2
[[ $(bash "$resolver" https://example.invalid/sibling.git absent) == ref=main ]] || fail 'missing ref did not fall back'
for status in 1 128; do
    export SIBLING_TEST_STATUS=$status
    if bash "$resolver" https://example.invalid/sibling.git topic > "$scratch/output" 2> "$scratch/error"; then
        echo 'transport failure incorrectly succeeded' >&2; exit 1
    else
        [[ $? == "$status" && ! -s "$scratch/output" && -s "$scratch/error" ]] || fail 'lookup failure did not remain fatal'
    fi
done
export SIBLING_TEST_STATUS=0
printf -v SIBLING_TEST_REFS '%s\trefs/heads/invalid branch' "$sha"
lookup_before=$(wc -l < "$SIBLING_TEST_LOG")
if bash "$resolver" https://example.invalid/sibling.git 'invalid branch' > "$scratch/output" 2> "$scratch/error"; then
    echo 'invalid branch incorrectly succeeded' >&2; exit 1
else
    [[ ! -s "$scratch/output" && -s "$scratch/error" ]] || fail 'invalid branch emitted a ref or no error'
fi
[[ $(wc -l < "$SIBLING_TEST_LOG") == "$lookup_before" ]] || fail 'invalid branch reached a Git lookup'
[[ $(bash "$resolver" https://example.invalid/sibling.git '') == ref=main ]] || fail 'empty branch did not select main'
echo 'sibling reference checks passed'
