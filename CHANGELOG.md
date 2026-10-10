# Changelog

All notable changes to this project will be documented in this file.

Release naming note:
- Starting with `v1.0.0`, `chunkdb` follows [Semantic Versioning](https://semver.org/)
  against the surface defined in `docs/COMPATIBILITY.md`.
- `preview`/`engineering alpha` describe the earlier `v0.1.x` line.

## Unreleased

- Add Docker first-start password generation, a shared data/backup volume, tagged binary archives and a runnable quick-start page (#67).
- Preserve completed migration history and matching schema/grant metadata in online backups; restore retains named-step retries under a fresh data-directory identity (#67).
