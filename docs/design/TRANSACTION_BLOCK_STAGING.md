# Prepared physical block staging

This design specifies prepared physical block staging for [#136](https://github.com/chunkdb/chunkdb/issues/136).
The initial implementation isolates allocation-aware variable storage; physical stage and history integration follow separately.
It follows [block observations and deltas](TRANSACTION_BLOCK_DELTAS.md).
It does not allocate revisions, append WAL, publish groups or activate session block deltas.

## Existing storage constraint

`ChunkVars` owns default-allocator vectors for encoded data and entry offsets.
`ApplyVarUpdate` materializes another complete section for multiple changes and retains the original section as undo.
Its single-change path reserves current storage and allocates an owned previous value before mutation.
`ChunkState` and the current history nodes also use default-allocator containers.
None of these allocator requests reaches `TxnBlockMemoryResource`.
An observed vector capacity after allocation is too late to admit its allocation peak.
Calling `reserve(n)` after charging `n` is insufficient because the vector may request a larger capacity.
A sparse physical stage must therefore use allocator-aware storage for growing variable sections and retained history.
A refusal-only stage would not implement full rebasing.

## Proposed representation and public decision

Use a scoped allocation-aware vector allocator for the two private `ChunkVars` buffers.
The allocator receives an explicit optional allocation account; ordinary `ChunkVars` has no transaction account.
Every charged allocation admits the actual request made by `allocator_traits`, including capacity, allocator bookkeeping and alignment, before forwarding upstream.
Vector growth needs no guessed capacity multiplier.
The encoded VARS layout, keys and iteration semantics remain unchanged.
The ordinary `Assign`, `Remove`, `reserve`, `Merge`, `ApplyVarUpdate` and `UndoVarUpdate` algorithms retain their existing paths.
There is no request-local global flag, allocation interception or thread-local budget switch.
The extra accounting branch is on allocation and deallocation, rather than on each ordinary row read or fixed write.

`Encode()` currently exposes `const std::vector<std::uint8_t>&` publicly and is noexcept.
Adding an internal encoded byte view does not make an allocation-aware buffer convertible to that exact reference type.
A lazy ordinary-vector compatibility copy would allocate in a noexcept const accessor and race between concurrent const readers.
An eager compatibility copy would require another growing default-allocator vector whose actual requests still cannot be admitted through the transaction account.
An accessor that works only for ordinary buffers would silently change the public contract after a grouped write.
None of those compatibility strategies is proposed.

Preserve the accessor's reference, noexcept and owning-copy semantics as far as possible by returning `const ChunkVars::EncodedStorage&`, where `EncodedStorage` is the vector with the scoped storage allocator.
Add a separate internal encoded byte view for consumers that only need a span, and expose data-capacity and offset-capacity byte accessors for admission.
In particular, `auto bytes = vars.Encode()` must remain an owned copy; replacing `Encode()` with a span would silently change that existing expression into a borrowed view.
An ordinary copy selects an uncharged allocator, while physical staging uses an explicit charged clone operation.
Calls using `auto`, `data`, `size`, `capacity`, iteration and equality between encoded results retain their behavior.
Source explicitly binding `const std::vector<std::uint8_t>&` or implicitly copying to `std::vector<std::uint8_t>` requires a change to the alias, a span or an iterator-range copy.
Functions accepting the old exact vector type must accept a span or use an explicit copy.
This is a C++ source and ABI change; exact old return-type compatibility cannot coexist with this representation and the stated pre-allocation accounting requirement.
The core PR must report that consequence explicitly.
`ChunkState` payload and presence vectors, public typed block APIs, wire formats and disk formats remain unchanged.
No compatibility cache or unconditional encoded-data duplicate is introduced.

An alternative is an owned immutable data-and-offset slab that live chunks and history can share.
It has an equivalent public accessor decision and makes lifetime transfer simple, but adds ownership and copy-on-write machinery to ordinary variable mutations.
The allocator-aware mutable representation is preferred because it preserves their current in-place and geometric-growth behavior.

## Allocation and counter ownership

An allocation account owns stable private accounting state and a shared lifetime handle to the existing global transaction counter.
The engine and session must not remain the sole owners of counters referenced by a physical stage.
The existing borrowed `TxnBlockMemoryResource` remains valid for journal-only allocations whose destruction precedes transaction teardown.
Physical buffers use an owning allocation domain because their lifetimes may extend beyond the preparing stack frame.
The domain's accounting target changes explicitly at publication, so retained live storage and history do not retain a closed transaction's counter.
The counter implementation remains the checked private admission and global compare-and-exchange used by the sparse resource.
Account/control storage is itself admitted before construction, with failure cleanup that returns its reservation.

Every variable-buffer allocation records its exact upstream extent, alignment, upstream resource and optional originating staged policy in an aligned prefix.
An ordinary allocator's policy remains null before and after allocation.
Default allocator copies made before allocation can therefore genuinely deallocate later ordinary buffers without any lazy allocator-identity change.
Explicit staged allocators carry a stable policy that knows its exact outstanding requests and current accounting target.
The owning subsystem serializes allocation, deallocation and handoff for each policy.
Prepared and live mutable storage uses its original chunk locks; exclusive final ownership also permits destruction without a chunk lock.
Retained history uses the innermost history mutex and its owning reservation, and deallocation never acquires the original chunk lock.
Account and upstream operations are allocation-only and non-reentrant; they never enter storage or acquire another storage lock.
The vector allocator propagates on swap and move assignment, so the allocator and its owned buffer always travel together.
These operations are noexcept and never rely on swapping unequal nonpropagating allocators.
Deallocation reads the original prefix, frees that exact upstream allocation and then returns its charge to the originating policy without throwing.
Allocator equality compares the genuine upstream deallocation domain and never changes merely because an allocator allocated a buffer.
Prefix recovery also makes equal allocator copies with different accounting policies genuinely cross-deallocatable.
Request recording is necessary even for ordinary storage because a later transaction may retain that exact buffer as undo or history.
The stage uses those records rather than inferring allocator requests from standard-vector capacity.
A staged intrusive policy control object has a known allocation extent and avoids an unknown shared-pointer control-block size.
Its creation admits the control allocation before requesting it upstream, and prefix and allocator references keep it alive through the last deallocation.
There is no policy or domain allocation for ordinary variable buffers, empty variable sections or ordinary fixed-only writes.
Ordinary variable buffers pay the recorded prefix and alignment-padding bytes and their allocation/deallocation bookkeeping.
That overhead is a concrete representation tradeoff requiring the existing paired hot-path measurements; it is not assumed to fit the budget.
Copy construction selects the ordinary allocator unless an explicit charged clone is requested.
No allocator may retain a pointer to a destroyed borrowed session resource.

History map nodes additionally need an aggregate retained-owner handoff because insertion does not install the node handle's allocator in the destination map.
Use one history-owned node deallocation domain for both ordinary and charged reservations.
Its rebound allocator adds an aligned node prefix holding the originating accounting owner and the actual upstream extent.
The prefix and alignment padding are included in admission before the upstream allocation.
The destination map can genuinely free any node from that domain by reading its prefix, regardless of which reservation account admitted it.
Allocator equality compares the shared deallocation domain, rather than pretending different upstream allocators are equal.
The node's prefix also owns the aggregate history charge released after the node itself is freed.
Node bookkeeping is incurred only when the existing history path allocates a kept node.

Prepared replacement buffers, sparse undo and retained old sections coexist under the transaction account until their ownership changes.
The original live variable section is also admitted by its recorded actual data, offset and domain-control requests before it becomes retained undo or history.
Data and offset capacities remain useful diagnostics but do not substitute for these allocator records.
That reservation does not allocate or copy the section and is not an exemption for preexisting memory.
A rejection before mutation returns this reservation and leaves its live owner unchanged.

At irreversible publication, a replacement variable section is already installed in its original locked `RegularChunk`.
The loaded chunk and existing regular-storage/cache ownership become its permanent owner before the allocation domain releases its exact transaction-private and transaction-global charges.
The current cache limit counts resident chunks and has no exact byte-quota counter; this design does not invent one or claim a nonexistent cache-byte reservation.
This handoff ends temporary transaction admission for primary storage, rather than exempting retained undo as preexisting memory.
The domain becomes ordinary-storage owned and drops its transaction account reference without allocating.
Later ordinary growth still follows the existing storage policy and retains the real deallocation domain.

Retained history transfers to the table history account under the innermost history mutex.
The history node records the exact retained allocation extent, including fixed/presence requests, variable data and offsets, metadata copies, node overhead and retained control storage.
`Publish` uses that extent for this reservation in the existing history-byte eviction policy, unregistering old snapshots and pruning before it retains a state that would exceed the limit.
The destination history accounting increment precedes the transaction accounting decrement, and both operations are checked, nonallocating and nonthrowing.
At transfer, individual allocation domains relinquish their transaction reservation to one aggregate retained-owner charge and keep their real upstream deallocation information.
The aggregate charge covers the immutable reservation and is released once, rather than again from each buffer and from `EraseLocked`.
The node prefix owns the final charge token; deallocation first destroys its retained buffers, copies that token to the deallocator's stack, frees the upstream node allocation and then returns the aggregate history charge.
The charged-node erasure path must not also execute the legacy `kept_bytes_ -= state.bytes` subtraction.
Legacy nodes keep their existing subtraction path.
If no snapshot still needs the state, it remains charged to the stage until destruction instead of being transferred or released early.
An allocation owner has exactly one final accounting target; histories never continue charging a closed transaction.
Pruning destroys the retained buffers and node and subtracts the same recorded history extent once.
Existing ordinary history reservations retain their established cost calculation; this subtask must not present that approximation as an exact physical byte measurement.
Exact charges of the new reservations and existing legacy charges coexist explicitly in `kept_bytes_` and the same eviction policy.
The table history object is kept alive by the prepared stage until its reservation is published or destroyed.
After publication, the final charge token references only a separate shared `HistoryAccountingState` containing the existing kept-byte counter and accounting metadata.
Moving that counter into a stable owner does not create another table quota or independently count the same retained state twice.
That state does not own history nodes or point back to `TxnHistory`, so the index cannot form an ownership cycle through its buffers.
Pruning or history destruction owns the nodes and performs the final deallocation, while the counter state remains alive through that deallocation.
Transferred domains and charge tokens hold no transaction, session or engine counter pointer.
Session teardown releases only legacy bulk charges and journal allocations still owned by it, and never subtracts transferred physical charges a second time.

## Prepared stage ownership

An internal `PreparedTxnBlockStage` is move-constructible and cannot be copied or assigned.
Preparation accepts the admitted sparse rebase plan, explicit allocation account and borrowed mutable current-chunk views.
The caller holds all participating chunk locks in canonical order for preparation, application and any rollback.
Views bind the exact payload and presence extents, variable owner, metadata owner and current producing state.
Preparation validates every binding before allocating any physical edit.
The stage cannot outlive those owners or be applied after releasing their locks.

Preparation builds charged descriptors, replacement variable sections and fixed/presence undo before changing storage.
It merges every changed chunk's variable section in one ordered pass into allocator-aware storage.
Both encoded data and offset allocations use the explicit account and the actual allocator requests.
The source section remains owned and unchanged throughout this preparation.
The stage does not invoke ordinary `ApplyVarUpdate`, whose internal staging is outside this explicit owner.
Logical entry and byte totals are checked against the sparse plan and the existing combined block/chunk variable limits.

Fixed undo retains only the original bytes touched by changed rows and validity bits, with overlapping byte positions coalesced.
Presence undo likewise retains each touched byte once.
Preparation computes replacement bytes from the admitted canonical rows without copying the whole fixed payload.
It prepares the new written flag, present-block count and chunk-column default owner when initialization is required.
Producing revision, commit time, feed, coordinate-index publication and WAL accounting remain coordinator responsibilities.
No apply routine invokes callbacks, validation, loading, allocation or quota admission.

`Apply()` is noexcept and swaps prepared variable storage, writes the prepared fixed/presence bytes and swaps prepared metadata ownership.
It records the applied prefix without allocation.
`Undo()` is noexcept and restores that prefix in reverse order using the retained variable owners and byte undo.
The stage destructor undoes an applied unfinished stage while its documented locks and owners remain held.
Explicit successful completion relinquishes rollback obligations only after the future coordinator establishes publication.
State transitions prevent reapplying, undoing twice or silently discarding an applied stage.

## History and late snapshot registration

Whole-state history cannot use an uncharged copy of public `ChunkState` as its physical staging representation.
Add an internal retained-state owner with allocation-aware fixed bytes, presence bytes, variable storage and serialized chunk metadata, plus a nonowning state view.
Existing public chunk reads materialize their existing public `ChunkState` results from that view.
Existing history consumers must read the view rather than assume every retained state is a default-vector `ChunkState`.
The retained metadata copy uses the explicit allocator instead of extending the lifetime of a standard-vector metadata object and an unmeasured shared-pointer control block.
Public reads materialize their ordinary immutable metadata result from that view when they need one; row comparisons do not materialize metadata.
The history index node itself is prepared through an actual request-accounting allocator before mutation.
Publishing the prepared node remains nonallocating under the history mutex.
The history mutex remains innermost and never covers chunk loading or another storage-lock acquisition.

Preparation has no producing revision and therefore cannot decide that a zero open-snapshot count makes history unnecessary.
A snapshot can register after physical preparation and before the future revision reservation.
The safe standalone stage prepares a full charged current before-state and history node for each changed chunk unconditionally.
It copies fixed and presence bytes through charged allocators.
For variable storage, preparation materializes the replacement, and apply moves the original complete variable owner into the already prepared history node through a noexcept swap.
That original section is covered by its admitted retained-capacity charge before the first edit.
Undo can swap it back from the unpublished history node without allocation.
When a chunk has no variable changes but history is required, an identical charged variable clone provides the new live owner, so the history node can still retain the complete original section.
The history metadata copy is charged by its real request and has no borrowed standard-vector capacity or hidden control-block estimate.
The block stage preserves the live immutable metadata owner except for the existing first-write default initialization rule.

The eventual runtime coordinator instead performs sparse validation, reserves the producing revision and then reads `OpenCount` in the existing sequentially consistent order before any in-place edit.
If that count is nonzero, it prepares the complete charged before-state and node while all original chunk locks are still held.
If it is zero, snapshots that registered before the revision reservation would have been visible, and snapshots registering afterward have a snapshot revision at or above the reserved tag.
Their completed-publication fence and the held chunk locks prevent reading half an applied group.
Only that real revision/open-count protocol may authorize the no-history state; there is no caller-supplied boolean that bypasses history preparation.
The coordinator exposes an internal admission token tied to the reserved tag and original locked owners, rather than a generic optional callback or skip flag.
Snapshot-free fixed-row commits then need no whole fixed or variable before-state copies, while variable growth still receives complete charged staging.
If that admission fails, no physical edit or group publication has occurred.
The stage cannot report readiness merely because a snapshot count sampled before revision allocation was zero.

The future coordinator supplies the reserved producing tag after preparation and rechecks the established snapshot registration ordering before application.
History reservation and account ownership remain available through every failure that can require the before-state.
Successful history publication transfers the retained owner and its charge without allocation.
Expiration or eviction destroys that retained owner and returns its charge exactly once.
The existing logical history-limit policy remains distinct from physical staging admission and must not be used as an invented exact allocation bound.

## Required checks before activation

An allocation observer must record actual data, offset, node and retained-state requests and verify that both private and shared reservations already cover each upstream allocation.
Exact quota boundaries, arithmetic overflow and upstream failures must preserve live bytes, metadata, index state and counters.
Multiple variable changes must exercise complete materialization, old-section retention and reverse undo across several chunks.
Tests must grow beyond both original data and offset capacities.
An allocation failpoint enabled after preparation must prove that apply and undo make no allocation requests.
Late snapshot registration must occur between physical preparation and revision reservation and still recover the old state.
Another fixture must register after revision reservation and exercise the established completed-publication fence.
Retained-history tests must destroy the session and engine-facing owner before pruning history to expose dangling counters or duplicate releases.
Ordinary variable-write tests and the required single-write budgets must verify the representation change before runtime activation.
Each guard needs an isolated omission control whose corresponding regression fails.

## Implementation boundaries

The representation is allocator-aware mutable `ChunkVars`, a const-reference allocator-aware `EncodedStorage` accessor, propagating explicit staged policies and exact allocation prefixes.
The exact old `Encode()` type cannot be kept safely, so the C++ API consequence is reported to core PR review without a misleading compatibility cache.
New primary storage leaves transaction accounting only after irreversible publication establishes its existing regular-storage owner.
Retained history transfers continuously to the existing table history-byte policy and releases its transaction counter references.
Unconditional full history preparation is limited to the standalone foundation; runtime activation requires the revision-aware admission protocol described above.
The allocator milestone does not expose physical application or authorize a runtime grouped writer.
