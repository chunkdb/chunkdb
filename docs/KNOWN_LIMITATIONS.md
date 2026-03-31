# Known Limitations

This list is intentionally explicit for release-preview readiness.

## Durability / Recovery

- durability is mode-dependent (`relaxed`, `fsync-wal`, `fsync-checkpoint`)
- no cross-chunk atomic transaction guarantee
- no replication/distributed durability
- on Windows, strict durability modes require directory-sync capability; if it is unavailable,
  strict writes fail instead of silently degrading to a weaker guarantee

## Runtime / Process Model

- single-writer / multi-reader process model (default)
- shared multi-writer on one data directory is unsupported
- lock metadata stale-takeover logic is conservative but not a distributed lease protocol

## Protocol / API

- protocol is alpha and may evolve before a stable compatibility policy
- no protocol-level transaction primitives

## Platform Support Boundaries

- Linux native: supported
- macOS native: supported
- Windows native (core non-TLS path): supported
- Windows native TLS: **not yet guaranteed** as fully supported for release-preview claims

## Packaging / Supply Chain

- archive packaging + SHA256 checksums are provided
- SBOM automation is not part of the current release-preview scope
