# Transaction block observations and deltas

This foundation implements the row ownership and rebasing rules of [#136](https://github.com/chunkdb/chunkdb/issues/136) and [Commit groups](COMMIT_GROUPS.md).
It adds row and delta primitives without wiring session vectors, command dispatch, group publication or WAL decisions.

## Owned rows and snapshots

A canonical block row owns its presence flag, schema revision and schema-ordered cells identified by stable column IDs.
Each present cell owns its NULL flag and exact encoded bytes; fixed-width padding is zero and a NULL cell has no value bytes.
Floating-point values retain their raw stored bits, so NaN payloads and positive and negative zero never pass through variant or arithmetic equality.
An absent row has no cells, and NULL remains distinct from an empty text or byte value.
Chunk producing revision, time and written status are returned separately and do not participate in row-value equality.

`ChunkStore::ReadBlockAtSnapshot` accepts a chunk coordinate, local block index and optional standard memory resource, and returns one owned canonical row with its snapshot producing metadata.
It validates active dimensionality, block bounds and snapshot ownership before row access.
It selects history or current state under the existing chunk-to-history lock order and copies only the selected row while that source remains owned.
The history mutex remains innermost, and row capture performs no table admission, chunk load or callback acquiring another storage lock while holding it.
An optional memory resource must provide allocation-only, non-reentrant operations that never enter storage or acquire a storage lock during capture or deallocation.
This preserves snapshot expiration and eviction fences without copying a chunk or first allocating a larger current row that the historical row replaces.
The existing typed block read APIs and ordinary write paths remain unchanged.

## Sparse private state and accounting

A table-bound journal is keyed by chunk coordinate and local block index, and binds its original table epoch and schema revision.
Each entry retains its first snapshot preimage once and distinguishes observation, replacement and deletion.
Repeated writes replace only the private delta, while reads use that delta when present and never overwrite the original preimage.
Replacement rows contain complete canonical row values, including defaults when an absent block becomes present.

Rows, cells, value buffers, journal nodes, promotion nodes and prepared edits use standard polymorphic allocators backed by a transaction budget resource.
The resource charges actual requested allocation bytes, including container capacity and implementation node sizes, before upstream allocation.
It checks overflow and the existing per-transaction bound, then reserves the existing global transaction counter through checked compare-and-exchange without a wrapping speculative increment; upstream allocation failure releases both reservations.
Deallocation returns exactly those charges, and staging remains charged while it coexists with the published private state.
The budget borrows the transaction's private byte count and the engine's global count, and its owner must outlive every row, journal and prepared edit using it.
The same resource reaches every nested cell and value container, including failed or still-staged records.
Move assignment between rows, cells, snapshots or variable plans with different resources refuses before transferring any bytes, flags or charges; explicit cloning selects a new resource instead.
Compound journal and rebase-plan owners support move construction but delete assignment, so partial allocator migration cannot publish through a wrapper.
Session integration must destroy or clear these allocated records before the existing transaction byte-counter teardown, so a charge is returned once.
No new process-wide quota or counter is introduced.

A statement prepares its row, delta and map node privately and publishes through equal-resource node transfer or nonthrowing swaps after admission.
Failure preserves the old preimage, private delta, promotion markers and byte counts.
A prepared edit cannot publish after another edit changes its journal generation.

## Commit rebasing and promotion

The caller owns every participating current chunk in canonical commit-lock order before constructing a rebase plan.
Validation compares every observed or written row with its original canonical preimage before returning any plan, and equal values permit ABA.
Identity or schema changes return the existing table-change transaction conflict, and row or promoted-revision mismatches return its chunk-change conflict.
Accepted replacement rows and variable changes are prepared against the current chunk rather than the snapshot chunk, preserving every unrelated block and the current chunk columns.
The plan admits row shapes, REQUIRED initialization and combined logical variable-byte growth without modifying current storage.
It owns its prepared sparse data through the same charged resource and records exact per-chunk changed variable keys, values, logical entry counts and encoded-byte totals.
It records the current variable-data capacity exposed by `ChunkVars::Encode`, but does not invent the hidden offset-vector capacity or a complete physical staging peak.
An unavailable exact capacity or materialization bound is represented explicitly and cannot authorize physical mutation admission.
The planner never calls `ChunkVars::Merge` or `ApplyVarUpdate`, clones a fixed payload or presence array, allocates a producing revision, appends WAL or publishes a group.

Physical application is separate required coordinator work before runtime activation of these deltas.
That owner must reserve and charge every variable-section materialization and retained undo buffer before allocation, including whole-section copies made by multi-value `ApplyVarUpdate`.
It must retain those charges until the corresponding prepared or undo memory is released, rather than exempting it as existing storage.
It also captures and reserves any current-chunk before state required by registered snapshots before the first in-place edit.
All fixed-byte, presence and variable undo must then be prepared, nonallocating and nonthrowing, and failure must restore every earlier applied row before releasing any commit lock.
No physical application API is exposed by this foundation, so a valid sparse plan cannot stand in for that missing admission and rollback owner.

An explicit chunk or area operation promotes its affected chunks to the snapshot's whole producing-revision dependency.
Promotion prepares a charged marker and incorporates earlier row deltas into the caller's admitted whole-chunk private state before removing sparse entries.
A failed promotion leaves the sparse journal unchanged, and later block statements use the promoted private state rather than creating another sparse entry.
Promoted chunks require exact whole-revision validation even when a later state has equal row values.

## Failure checks

Tests cover disjoint rows, changed rows, restored-value ABA, repeated writes, read-only observations, deletes, NULL versus empty values, raw NaN and signed-zero bits, and metadata preservation.
They cover late conflicts across multiple chunks without storage mutation, promotion failure and whole-revision conflicts.
Allocation controls and exact quota boundaries exercise both private and shared counters, map nodes and failed prepared edits.
Large fixed-payload fixtures bound row capture and sparse private allocation independently of the chunk size, including history selection after a larger concurrent row.
Wide current variable sections with tiny row deltas retain unchanged storage and expose unavailable physical staging admission instead of bypassing the byte budget.
The existing history expiration checks remain in force; new row fixtures directly cover evicted, ended and foreign snapshot refusal without publishing private state, and both coordinate dimensions use the same row primitives.
Each targeted guard has an isolated omission control that must fail its corresponding regression.
