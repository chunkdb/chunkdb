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

## Stable surface (what 1.x guarantees)

### On-disk storage format (`fs_split_v1`)

- A `1.x` build **reads** any chunk/WAL data written by any other `1.x` build,
  and the documented legacy versions it already accepts on read
  (`.chk` v1/v2/v3, `.wal` v2/v3) — see `docs/STORAGE_FORMAT.md`.
- Checkpoint image **v3** (added within `1.x`) stores the same header followed
  by a `zrle`-compressed state blob. It is written only when the server runs
  with `--checkpoint-compression zrle`; readers accept v1, v2, and v3
  regardless of the configured mode. A build that predates v3 cannot read v3
  images, which is the normal forward-compatibility boundary below.
- The server also maintains a small `chunkdb.version` bookkeeping file in the
  data directory (the persisted chunk-version clock ceiling). It is not chunk
  data, but it is required to preserve deterministic stale-version rejection.
  Stable-v1 stores that predate this bookkeeping migrate automatically without
  changing chunk data. A valid initialized marker makes a missing, unreadable,
  or invalid clock a startup error; the clock is never reset when prior token
  exposure is provable. See `docs/STORAGE_FORMAT.md` for the checked record,
  intermediate-ceiling upgrade, and simultaneous-loss limitation.
- Current writers also maintain the checked `chunkdb.snapshot` monotonic
  generation used by concurrent read-only processes. Stable-v1 stores with no
  record are generation zero and migrate automatically: the writer publishes
  odd before recovery and even afterward without changing chunk data.
  Read-only opening remains non-mutating. A malformed record, exhausted
  generation, or odd generation left by a crashed writer fails closed until a
  current writer completes recovery. Older binaries ignore this bookkeeping
  file, so concurrent old-writer/current-reader operation is outside the
  supported SWMR compatibility boundary.
- A format-version bump introduced within `1.x` stays **backward-readable**:
  newer builds read older data and migrate it on write. We never silently break
  readability of data written by an earlier `1.x`.
- **Not guaranteed:** forward compatibility. An older binary is not required to
  read data written by a newer one (for example v3 compressed images). Always
  upgrade the binary before the data.

### Wire protocol

- The text command protocol and framing in `docs/PROTOCOL.md` are stable within
  `1.x`: `PING`, `AUTH`, `QUIT`, `INFO`, `GET`, `EXISTS`, `SET`, `UNSET`,
  `MGET`, `MSET`, `CHUNKEXISTS`, `CHUNK`, `CHUNKSET`, `CHUNKBIN` (incl. their
  `STATE` forms), plus the `+`/`-`/`$`/`*` reply framing.
- The following commands were **added within `1.x`** as backward-compatible
  extensions and are covered by the same stability promise going forward:
  `CHUNKSCAN`, `CHUNKRANGE`, `CHUNKRADIUS`, `CHUNKVER`, `CHUNKCAS`,
  `CHUNKBATCH`, `CHUNKBINC` (incl. its `STATE` form), `WALFLUSH`, and
  `METRICS`, plus the `VERSION_MISMATCH` error code. Servers that predate a
  given command answer `-ERR UNKNOWN_COMMAND`; clients treat these as optional
  capabilities.
- New commands and new optional arguments may be added in a MINOR release.
  Existing commands will not be removed, nor have their request/response shape
  changed incompatibly, within `1.x` without a deprecation period announced in
  `CHANGELOG.md` and a MAJOR bump to actually remove them.
- The `INFO` payload may gain new `key=value` lines in MINOR releases; existing
  keys keep their meaning.

### Durability contract

- The behavior of the durability modes (`relaxed`, `fsync-wal`,
  `fsync-checkpoint`), including their guarantees and explicit non-guarantees,
  is stable per `docs/DURABILITY_CONTRACT.md` and tied to the maintained
  crash/recovery test suite (`tests/durability_crash_hardening_tests.cpp`,
  `-L crash`).

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

- **`fs_region_v1` storage backend** — experimental; its on-disk format and
  behavior are not stable and not part of `1.x` compatibility promises.
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

On-disk legacy format versions are an exception in the safe direction: they are
kept **readable** as long as practical even across MAJOR lines, so existing data
is not stranded.
