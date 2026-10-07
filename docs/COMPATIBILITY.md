# Compatibility & Stability Policy

This document defines what `chunkdb` promises — and does **not** promise — across
releases, starting from the first stable release `v1.0.0`. It is the policy that
the stable channel is held to (see `docs/RELEASE_POLICY.md`).

The goal is an honest, test-backed boundary: firm guarantees for the surface we
actually validate, and explicit non-guarantees for everything else.

## Versioning

`chunkdb` follows [Semantic Versioning](https://semver.org/) from `1.0.0`:

- **MAJOR** (`2.0.0`): a documented, intentional break in any stable surface
  below (wire protocol, on-disk format readability, CLI, or durability
  contract). Always called out in `CHANGELOG.md`.
- **MINOR** (`1.1.0`): backward-compatible additions (new protocol commands,
  new CLI flags, new optional config, new on-disk format versions that older
  data still upgrades into). Existing behavior is preserved.
- **PATCH** (`1.0.1`): bug fixes and internal changes with no stable-surface
  effect.

The engine, CLI (`chunk-cli`), and client (`chunkdb-js` / `@chunkdb/client`)
version independently; each follows semver against its own stable surface.

## Stable surface

### On-disk storage format (`fs_split_v1`)

- A `2.x` build opens only a data directory that has a data-directory manifest
  (`chunkdb.manifest`) and its tables under `tables/`, each with a table
  manifest (`table.manifest`) that records the geometry the table was created
  with; see `docs/STORAGE_FORMAT.md`. `2.0` does not read or convert data of
  `1.x` or of 2.0 development builds: it refuses such a directory without
  changing it, and data starts anew in `2.0`. A `1.x` build cannot read the
  images and WALs a `2.x` writer produces. This is why `2.0.0` is a MAJOR
  release.
- The geometry of a table is fixed when it is created. Opening it with any
  other geometry value fails and changes nothing on disk.
- Compressed checkpoint images (`checkpoint_compression zrle`) are read by
  every `2.x` build regardless of the table's setting.
- The server also maintains a small `chunkdb.version` bookkeeping file in the
  data directory (the persisted chunk-version clock ceiling). It is not chunk
  data, but it is required to preserve deterministic stale-version rejection.
  A valid initialized marker makes a missing, unreadable,
  or invalid clock a startup error; the clock is never reset when prior token
  exposure is provable. See `docs/STORAGE_FORMAT.md` for the checked record
  and the simultaneous-loss limitation.
- Current writers also maintain the checked `chunkdb.snapshot` monotonic
  generation used by concurrent read-only processes. The writer publishes odd
  before recovery and even afterward. Read-only opening remains non-mutating. A malformed record, exhausted
  generation, or odd generation left by a crashed writer fails closed until a
  current writer completes recovery. Older binaries ignore this bookkeeping
  file, so concurrent old-writer/current-reader operation is outside the
  supported SWMR compatibility boundary.
- Within `2.x`, newer builds read data written by earlier `2.x` builds. New
  on-disk features are recorded in the manifests' feature flags: a build
  refuses data that uses a feature it does not know instead of misreading it, or opens it read-only when the feature only forbids writing.
- **Not guaranteed:** forward compatibility. An older binary is not required to
  read data written by a newer one. Always upgrade the binary before the
  data.

### Wire protocol

- `2.x` speaks protocol 2 (`docs/PROTOCOL.md`) only. A connection starts with
  `HELLO 2`; a client of another protocol is refused at its first command with
  `-ERR PROTOCOL expected HELLO 2`.
- Within `2.x`, the commands, options and reply framing of protocol 2 are
  stable. New commands, new optional arguments and new `HELLO` capabilities
  may be added in a MINOR release; clients check `capabilities` instead of
  probing. Existing commands will not be removed, nor have their
  request/response shape changed incompatibly, without a deprecation period
  announced in `CHANGELOG.md` and a MAJOR bump to actually remove them.
- The `HELLO`, `INFO` and `TABLEINFO` payloads may gain new `key=value` lines
  in MINOR releases; existing keys keep their meaning. Clients ignore keys
  they do not know.
- `CHUNKVER` tokens keep their shape and their stale-token guarantee in `2.0`.
  What changes is that they are persisted, so eviction and restart no longer
  invalidate them; client code that treats a token as opaque and possibly
  invalidated by a reload keeps working unchanged. See `docs/PROTOCOL.md`.

### Durability contract

- The behavior of the durability modes (`relaxed`, `fsync-wal`,
  `fsync-checkpoint`), including their guarantees and explicit non-guarantees,
  is stable per `docs/DURABILITY_CONTRACT.md` and tied to the maintained
  crash/recovery test suite (`tests/durability_crash_hardening_tests.cpp`,
  `-L crash`). `2.0` does not weaken it: WAL frames only widen per-mutation
  crash atomicity to every geometry.

### CLI

- `chunk-cli` commands and flags documented in its README are stable within its
  own `1.x`. New commands/flags may be added; existing ones are not removed or
  repurposed without a MAJOR bump.

### Platform support boundary

Stable claims cover the surface we validate in CI on every change:

- Linux native — supported
- macOS native — supported
- Windows native core path — supported
- Windows native TLS with the MSYS2 MinGW64 toolchain and MSYS2 OpenSSL —
  supported (validated by the `Build and Test TLS (windows-latest)` job)

## Out of scope (explicitly NOT covered by stability)

These may change, break, or be removed in any release without a MAJOR bump:

- **Windows native TLS on other toolchains** — MSVC builds and OpenSSL
  distributions other than the MSYS2 MinGW64 package are untested and not a
  stable support claim.
- **Internal C++ API / headers** — `chunkdb::*` library symbols and the
  `include/chunkdb` headers are implementation detail; only the on-disk format,
  wire protocol, CLI, and durability contract are stable surfaces.
- **Log message text, metrics names, benchmark numbers** — informational and
  subject to change.
- **Cross-chunk atomic transactions / replication / full ACID** — not provided;
  see `docs/KNOWN_LIMITATIONS.md`.

## Deprecation process

When a stable surface element must change incompatibly:

1. The replacement is added (MINOR), and the old element is marked deprecated in
   `CHANGELOG.md` and the relevant doc.
2. The deprecated element keeps working for the remainder of the current MAJOR
   line.
3. Removal happens only at the next MAJOR release.

Within a MAJOR line, older on-disk format versions stay readable. A MAJOR
release may stop reading older data; `CHANGELOG.md` says so.
