# Compatibility

This policy covers chunkdb 2.0's [protocol 3](PROTOCOL.md), [CQL](CQL.md), [on-disk format](STORAGE_FORMAT.md) and [durability contract](DURABILITY_CONTRACT.md).
Within 2.x, protocol and storage changes are additive.
Clients that use the 2.0 surface and data written by 2.0 work with every later 2.x server.
Removing or incompatibly changing that surface requires 3.0.

## Protocol and clients

Existing statements, parameters, response types and meanings remain compatible.
Later 2.x servers may add commands, optional clauses and map keys; clients must ignore unfamiliar map keys.
A connection begins with `HELLO 3`; another version is refused without executing statements.
Chunk versions are equality tokens and retain their meaning across eviction and restart.

The Go, JavaScript and CLI packages version independently.
Use a package that supports protocol 3 and the APIs your application needs; package version numbers need not equal the server's version.
This promise does not make new 2.x commands available on a 2.0 server.

## Stored data

Every later 2.x server reads valid data written by 2.0, preserving existing fields and their meanings.
Manifests record feature flags; sections, options and WAL records can grow behind those flags.

Unknown incompatible features prevent opening.
Unknown read-only-compatible features permit only read-only opening.
Compatible features may be ignored.

An older binary is not required to open data that uses a newer feature it does not implement.

Upgrade the binary before enabling such a feature.
Later 2.x servers still open valid 2.0 data.

Table geometry is immutable and validated against its manifest.
Keep manifests, revision clocks, initialized markers and all other required artifacts together in a consistent backup.
2.0 accepts only its documented directory and file layouts.
It does not import superseded development layouts.

An unsupported layout is refused, rather than converted in place.

## Boundaries

The durability modes and `FLUSH WAL` preserve their documented guarantees throughout 2.x.

Supported native platforms are Linux, macOS, and Windows 10 or later.
Native Windows TLS uses MSYS2 UCRT64 and its OpenSSL package.
MSVC and other Windows TLS distributions are outside the validated toolchain boundary.
Filesystem and device sync semantics remain prerequisites for durability.

Internal C++ symbols, diagnostic log text, metric names and benchmark results are not protocol or storage compatibility guarantees.
Only one server writes a data directory.
Replication and shared multi-writer operation are unavailable.
See [known limitations](KNOWN_LIMITATIONS.md) before selecting runtime options.
