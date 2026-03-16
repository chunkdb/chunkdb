# Release Policy

`chunkdb` currently uses two public release channels:

- `preview`
- `stable`

## Preview Releases

A preview release is the public evaluation channel.

For `chunkdb`, `Pre-release` means:

- the engine is usable for evaluation, integration testing, and early workload validation
- durability and recovery behavior are documented and partially proven by targeted tests
- compatibility should not yet be treated as a stable long-term promise
- support claims remain narrower than the future stable channel

Preview releases are intentionally marked as GitHub `Pre-release` so they are not presented as the stable/default channel for new users.

## Stable Releases

A stable release will be the channel intended for users who want the strongest compatibility and support expectations that `chunkdb` can honestly provide at that time.

For `chunkdb`, a stable release means:

- release-facing behavior and compatibility promises are explicitly documented
- durability/recovery guarantees and non-guarantees are documented and tied to maintained tests
- platform support claims are narrowed to what is actually validated
- release packaging and verification flow are stable enough to recommend without preview caveats

## Why Preview Is Not Stable / Latest

The current preview release is not the stable/latest channel because the project is still in engineering-alpha maturity:

- protocol and format stability policy is not finalized
- support boundaries are still narrower than a future stable release
- deeper post-preview hardening work remains intentionally out of scope for the preview channel

There is currently no stable `chunkdb` release published yet.

## Conditions Before A Stable Release

Before the first stable release is published, the project must be ready to make narrower but firmer public claims about:

- protocol/storage compatibility expectations
- platform support boundaries
- durability contract and recovery behavior
- release packaging and verification workflow
- documented limitations that remain after the stable cut

This does not require full ACID semantics or a general-purpose database feature set.
It does require an honest, test-backed support boundary for the features and platforms that are claimed as stable.

## Current Platform Rule

The current stable-support rule is:

- Windows Native TLS is not part of stable support claims yet

Current preview support boundary:

- Linux native: supported
- macOS native: supported
- Windows native core non-TLS path: supported
- Windows Native TLS: not yet guaranteed as fully supported
