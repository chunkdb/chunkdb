#!/usr/bin/env bash
# Moves every changelog.d/*.md fragment (README.md excepted) under
# "## Unreleased" in CHANGELOG.md, in the order the fragments were added to
# the current branch, then removes the fragment files.
set -euo pipefail
root="$(git rev-parse --show-toplevel)"
cd "$root"
grep -q '^## Unreleased$' CHANGELOG.md || { echo "CHANGELOG.md has no '## Unreleased' heading" >&2; exit 1; }
fragments=()
while IFS= read -r file; do
    [[ "$file" == changelog.d/README.md ]] && continue
    [[ -f "$file" ]] && fragments+=("$file")
done < <(git log --reverse --diff-filter=A --name-only --format= -- 'changelog.d/*.md' | awk 'NF && !seen[$0]++')
for file in changelog.d/*.md; do
    [[ "$file" == changelog.d/README.md ]] && continue
    printf '%s\n' "${fragments[@]:-}" | grep -qxF "$file" || fragments+=("$file")
done
[[ ${#fragments[@]} -eq 0 ]] && { echo "no fragments"; exit 0; }
entries="$(mktemp)"
trap 'rm -f "$entries"' EXIT
for file in "${fragments[@]}"; do
    grep -v '^[[:space:]]*$' "$file" >> "$entries"
done
awk -v entries="$entries" '
    { print }
    /^## Unreleased$/ && !done { print ""; while ((getline line < entries) > 0) print line; done = 1; skip = 1; next }
' CHANGELOG.md | awk 'NR == 1 || !(prev == "" && $0 == "") { print } { prev = $0 }' > CHANGELOG.md.new
mv CHANGELOG.md.new CHANGELOG.md
git rm -q -- "${fragments[@]}"
echo "gathered ${#fragments[@]} fragments"
