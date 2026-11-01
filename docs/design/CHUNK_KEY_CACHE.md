# Coordinate hashing and large-chunk admission

Both coordinate hashers use the same nonlinear mixing function over all three signed axes, including a zero third axis.
A process seed is obtained once from the existing operating-system random source and captured by default-constructed hashers.
Seed initialization failure propagates before a map can use its hasher; there is no predictable fallback seed or random work during lookup.
The mixing function accepts a seed explicitly so distribution and collision-family checks can use fixed inputs.
Equality and coordinate ordering remain exact, and hashes are never persisted.

Stores sharing `StoreResources` also share a hard large-chunk container budget.
For regular-chunk capacity `L`, let `B = max(L - min(EvictionLowerWatermark(L), L), 1)`.
The container limit is `L + B + 1`, with overflow rejected while constructing the resource pool.
The final spare container allows an ordinary miss to load before regular-chunk eviction selects its victim.
This uses the existing cache setting and introduces no additional configuration flag.

Every new container reserves one place before publication in the large-chunk map or eviction ring.
Registered, constructing and retired containers consume the same budget until their last owner releases them.
Reservations keep their accounting owner alive and are released on allocation, load or publication failure.
An existing container remains accessible at the limit; a new miss fails immediately with the existing `BUSY` code and `reason=large_chunk_cache_full`.
The refusal allocates no producing revision and applies no mutation.
Metrics expose the shared current count, limit and cumulative refusals.

Registry publication rolls back as one unit if map, scan-catalog or ring allocation fails.
A failed first load retires an empty container under the existing registry-then-container lock order.
A loader drops a retired owner before attempting a replacement, so it cannot block itself with its old reservation.
Capacity admission performs no directory I/O, global eviction pass or capacity wait while holding the large-chunk registry mutex.
Drop and shutdown release ordinary ownership without resetting counters still held by in-flight readers.
Maintenance preparation that checkpoints cold chunks uses bounded temporary off-cache state under its existing exclusive ownership and publication locks.
In particular, tombstone compaction does not acquire new cache admission after entering its durable publication phase or fence a healthy table for this known capacity refusal.

Fixed-seed tests cover signed extremes, zero third axes, the former two-axis collisions, three-axis collision families and bucket distribution for both key types.
Lifecycle tests cover shared limits across tables, live and retired owners, allocation and load failure, empty-container retirement, drop and recovery, refusal identity and metrics.
Each regression is checked against the corresponding broken behavior.
Release warnings-as-errors, the full test suite, GCC compilation and independent review precede a ready pull request.
World, canvas and simulation budgets use the existing remote measurement procedure and remain separate from functional checks.
