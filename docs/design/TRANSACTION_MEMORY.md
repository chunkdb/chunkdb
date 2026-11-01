# Transaction accounting ownership

This contract defines the production accounting bridge for [#136](https://github.com/chunkdb/chunkdb/issues/136).
It follows the allocation and handoff contract in [prepared physical block staging](TRANSACTION_BLOCK_STAGING.md).

The engine owns a shared global transaction counter whose lifetime extends through every transaction allocation owner.
That fixed engine owner replaces the existing inline counter and is not a per-transaction allocation charge.
Each production BEGIN creates an owned private ledger with immutable private and global limits.
BEGIN still takes no table snapshot.
The ledger's exact allocation extent is checked against its private limit and reserved globally before its upstream allocation.
Its private count starts with this self-charge.
Creation failure leaves the session without a transaction and returns every acquired reservation.

The ledger uses intrusive ownership so its own control extent is known before construction.
An allocation account adapter is created lazily when an owning allocation domain is requested.
Its shared object and control allocation use a checked allocator retaining the ledger and admitting the actual upstream request before allocation.
Ledger, account and control storage count against the same private and global quotas as their buffers.
The engine, session and preparing stack are never the sole owners of counters referenced by escaped physical storage.
Production allocation domains use the process-lifetime upstream resource; another upstream must satisfy its existing explicit lifetime contract.

Private admission uses checked atomic reservation, and shared global admission uses checked compare-and-exchange.
Failure of global admission or upstream allocation returns only the reservation acquired by that operation.
Arithmetic never wraps before an admission decision.
Legacy logical changes, journal and sparse resource requests, and physical allocation accounts share this admission path.
The existing lifetime-bound borrowed resource and transaction constructors remain available without exporting an owning account from borrowed counters.

Every account and physical buffer returns its charge after its exact upstream allocation is freed.
The last ledger owner saves its global owner, destroys and frees the ledger, and then returns the global self-charge without accessing destroyed private state.
Abort and session destruction first destroy their journal and legacy state, then release only the legacy logical bucket once.
They never subtract a combined private count that includes physical allocations held elsewhere.
Physical regular-storage and retained-history transfers follow the existing explicit handoff contract without another subtraction at session teardown.
No allocation account points back to a session, engine or borrowed session resource.

The public legacy `bytes` field continues to describe logical bulk-write state.
Existing quota flags and refusal classification remain recognizable while the combined account also includes the new charged domains.
Legacy session-object allocation, standard read/write containers, initial bulk copies and saved bulk copies retain their existing paths.
Their allocation peaks are not made pre-admission safe by this bridge.
Sparse session writes, physical application, retained history and group publication are separate integration boundaries.

Checks cover exact mixed-domain limits, global contention, overflow, upstream and control-construction failure, and release after the final upstream free.
Escaped domains remain valid after session and engine destruction and return every charge once.
Tiny-limit checks distinguish ledger and account/control extents from the legacy logical bucket.
