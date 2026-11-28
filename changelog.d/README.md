# Changelog fragments

Each pull request adds its CHANGELOG entry as a file here instead of editing `CHANGELOG.md`, so parallel pull requests never conflict on it.

- Name the file after the branch, `changelog.d/<branch>.md` with `/` written as `-`, for example `changelog.d/fix-drop-gate.md`.
- Write the entry as it will appear under `## Unreleased`: one or more `- ` lines, each ending with its issue reference, such as `(#146)`.
- A release runs `scripts/release/gather-changelog.sh`, which moves every fragment under `## Unreleased` in `CHANGELOG.md` in the order the fragments were added to main and removes the files.
