# Transaction intent recovery admission

This admission rule implements [#136](https://github.com/chunkdb/chunkdb/issues/136) without changing the checked transaction intent encoding.
Recovery admits a nonzero unsigned 32-bit count by the dimensional entry width and exact measured record extent, independently of current statement limits.
The encoder retains explicit write-admission limits.
The caller supplies its independently expected table identity and dimension count.
The fixed header, expected dimensions and identity, revision and checked count arithmetic are admitted before any complete-record allocation.
A first seekable-stream pass checks every ordered coordinate and the complete CRC using constant-sized scratch storage.
Only a completely admitted record permits allocating its exact measured byte buffer or owned member array.
The loader rewinds the same pinned native handle, reads the exact record, and validates the bytes again before returning them.
Changed length, truncation, invalid ordering, checksum damage or conflicting header context refuses admission without publishing state.
The loader returns checked bytes, and read-only snapshot capture binds the expected store context in its reader callback.
Feed and conditional recovery forward their independently owned table context to that same admission path.
Offline verification uses this loader instead of a size cap derived from the current write limit.
Complete intent and WAL preflight still precedes every repair or unlink.
Tests cover historical counts above earlier limits, exact extent and CRC failures, sparse forged records, wrong-owner rejection before body allocation, bounded scratch reads, loader admission, offline verification and unchanged WALs after refusal.
