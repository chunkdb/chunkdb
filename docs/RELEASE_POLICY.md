# Release Policy

`chunkdb` uses two public release channels:

- `preview`
- `stable`

The current public release is **stable `v1.0.0`**. The stable channel is held to
the compatibility and support boundary in [COMPATIBILITY.md](COMPATIBILITY.md).
The `preview` channel (`v0.1.x`) remains documented below for historical context.

## Preview Releases

A preview release is the public evaluation channel.

For `chunkdb`, `Pre-release` means:

- the engine is usable for evaluation, integration testing, and early workload validation
- durability and recovery behavior are documented and partially proven by targeted tests
- compatibility should not yet be treated as a stable long-term promise
- support claims remain narrower than the stable channel

Preview releases are intentionally marked as GitHub `Pre-release` so they are not presented as the stable/default channel for new users.

## Stable Releases

The stable channel is intended for users who want the strongest compatibility and support expectations that `chunkdb` can honestly provide at that time.

For `chunkdb`, a stable release means:

- release-facing behavior and compatibility promises are explicitly documented
- durability/recovery guarantees and non-guarantees are documented and tied to maintained tests
- platform support claims are narrowed to what is actually validated
- release packaging and verification flow are stable enough to recommend without preview caveats

## Why Preview Was Not Stable / Latest

The earlier `v0.1.x` preview releases were not the stable/latest channel because
the project was still in engineering-alpha maturity:

- protocol and format stability policy was not finalized
- support boundaries were narrower than a stable release
- deeper post-preview hardening work remained intentionally out of scope for the preview channel

Stable `v1.0.0` has since been published; the notes above describe why the
earlier `v0.1.x` preview line was not stable.

## Stable Release Criteria

Before a stable release is published, the project must be ready to make narrow
but firm public claims about:

- protocol/storage compatibility expectations
- platform support boundaries
- durability contract and recovery behavior
- release packaging and verification workflow
- documented limitations that remain after the stable cut

This does not require full ACID semantics or a general-purpose database feature set.
It does require an honest, test-backed support boundary for the features and platforms that are claimed as stable.

## Current Platform Rule

The current stable-support rule is:

- Windows native TLS is a stable support claim for the MSYS2 MinGW64 toolchain
  with MSYS2 OpenSSL only; MSVC and other OpenSSL distributions are untested
  and not claimed

Historical preview support boundary:

- Linux native: supported
- macOS native: supported
- Windows native core non-TLS path: supported
- Windows Native TLS: not yet guaranteed as fully supported
