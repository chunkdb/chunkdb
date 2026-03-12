# chunk

`chunk` is a custom, high-performance, chunk-oriented game database written in C++.

This repository is developed from scratch with:
- a custom text protocol,
- packed bit-level block storage,
- token authentication,
- strict on-disk format and integrity guarantees,
- configurable geometry for large chunks, chunks, and block bit-size,
- test-driven development.

Detailed protocol and storage specification is documented in `docs/` and evolves with commits.
