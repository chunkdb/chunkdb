# Commit groups

This contract covers [#136](https://github.com/chunkdb/chunkdb/issues/136) and builds on the [chunk model](CHUNK_MODEL.md).
The user-visible guarantees are in [Durability](../DURABILITY_CONTRACT.md).
The pure checked encodings are specified in [Commit group records](COMMIT_GROUP_FORMAT.md).

## Ownership and identities

The `commit_groups` incompat feature owns the directory journal and grouped table artifacts.
New directory and table defaults enable it; existing roots use the checked ENABLE FEATURE publication before grouped operations are admitted.
The existing activation decision covers both the directory and every table owner, so an older writer cannot accept only part of the new ownership.
A data directory owns one group journal and one checked counter for externally visible transaction IDs.
Standalone stores use the same coordinator with their store root as the directory scope.
The coordinator is created lazily and unconditional single-chunk writes without IDs or tags perform no journal or directory coordination.

Each group has an independent random internal identity, an exact member count and a member ordinal on each WAL frame.
The GROUP WAL TLV body is exactly 32 bytes: 16 raw identity bytes, a little-endian unsigned 64-bit member count and a little-endian unsigned 64-bit ordinal.
The identity is nonzero, the count is nonzero and the ordinal is strictly below that count.
Its enclosing frame header checksum protects the complete TLV, and decoding its shape alone does not authorize replay without the admitted directory decision.
A visible `txn` is a nonzero unsigned directory counter allocated only for cross-table groups and groups carrying an idempotency ID.
Its durable reservation ceiling precedes exposing a counter range, so restarting cannot reuse an exposed ID.
Frame and record identities include the data-directory identity, table epoch, dimensions, coordinates, producing revision and producing time.
Paths are derived from admitted identities rather than accepted from a journal as filesystem instructions.

The group record names each member's WAL boundary, exact frame extent and SHA-256 digest.
The pre-group WAL end and frame offset are separate fields, because a newly created WAL can place its first frame after a header while its original rollback end was zero.
Length and digest bind the entire frame, including its checked header and group identity.
The journal has a checked directory-bound header and length-delimited, checksummed records.
An independently published journal inventory binds the journal identity, generation, certified byte cut, last record identity and SHA-256 digest of the complete certified prefix.
Syncing a decision or durability receipt precedes atomically publishing and syncing its new inventory cut; acknowledgement follows that publication.
Bytes at or below that cut are never reclassified as an interrupted append, including a damaged last length field or a truncated final certified record.
Certifying journal bytes does not certify the members of an earlier relaxed group without that group's durability receipt.
Compaction writes and syncs a distinct journal generation, publishes its checked inventory reference, and only then retires the old generation.
The staged generation's file and parent directory are both durable before any inventory reference can name it.
Counts are admitted against the complete record extent with checked arithmetic before allocation.
Recovery codecs do not impose the current session's chunk-count limits on an earlier valid record.

## Admission and publication

Prepare the complete write set, decoded values, conditions, history reservations, coordinate reservations, feed rows and group record before the decision.
No member becomes visible while validation or allocation can still reject another member.
Take table admission in stable table-identity order and exclusive chunk locks in table-identity then coordinate order.
Evaluate conditions and rebase deltas while those locks protect the current states.
Single-block constant encoding remains before its chunk lock.

Append all member frames before the group record.
Relaxed groups hold their chunk locks until the record is appended and then publish all prepared states without allocation.
Synced groups first sync every member and its required directory entries, then append and sync the group decision before publishing or acknowledging.
Transactions use the synced rule even when every table is relaxed.
The directory publication gate protects the short cross-table visibility step, not frame encoding or filesystem sync.
The result publishes per-table revisions and a shared visible transaction ID when one was allocated.
Value-equal targets retain their producing identities and bind observed members rather than inventing mutation frames.
A durable decision first makes any observed producing state it relies on durable.
An empty ID outcome can retain its result without allocating a table revision or emitting a feed mutation.

Cancellation or a deadline can reject the group before its decision.
After that decision, the owner completes the checked outcome instead of returning a successful cancellation that implies no write.
Rollback or decision uncertainty fences the affected tables until group-aware recovery establishes the outcome.

## Durability dependencies

A published relaxed group retains ownership of its member WAL extents until the complete group is durable.
Checkpointing, table-mode changes, barriers and later synced mutations first satisfy any group dependency they would otherwise cross.
Making a group durable syncs every member before recording and syncing its durability receipt.
A later synced group record does not implicitly certify unrelated earlier relaxed groups.
Durability receipts identify exactly the group decisions they certify.

Dependency syncing does not acquire another chunk lock while holding a member's chunk lock.
Member ownership prevents checkpoint removal or truncation below the recorded extent until that dependency is satisfied.
DDL and shutdown drain such dependencies before retiring stores or releasing directory ownership.
Failures preserve the existing durability contract and never fall back to a weaker mode.

## Recovery and checkpoints

Read all group decisions and required member contexts into a private recovery plan before any member repair.
Validate complete member framing, exact digests, original replay bases, retained identities, schema histories and applicable rollback decisions before changing files.
Each changed member binds its checked earlier durable image, retained-empty identity or never-written anchor independently of its volatile pre-group WAL end.
An image anchor binds exact image length and SHA-256; every anchor includes its producing revision, time and schema where a producing state exists.
A separately certified durable WAL prefix, when available, binds its byte end and SHA-256 and cannot extend beyond the recorded pre-group end.
Absence of that certificate does not turn the volatile pre-group end into a durability claim.
The group owns that original anchor until complete durability or checked checkpoint coverage removes the rollback dependency.
Recovery validates the surviving pre-group prefix over that anchor and preserves all complete available frames before the group.
A crash-shaped loss of an earlier relaxed suffix can move the rollback cut back to its last complete frame, but never below a certified durable prefix or through interior damage.
It does not make every relaxed group sync its entire pre-group volatile prefix merely to manufacture a rollback point.
A table constructor cannot independently trim an unresolved group member.
An absent decision rolls every surviving member back at its first group frame.
A fully checked relaxed decision without a certified durability receipt whose members are missing or crash-shaped incomplete also rolls back the whole group.
Its surviving members and later dependent relaxed suffixes are discarded together after every rollback base passes admission.
If such a suffix contains a later group's member, the recovery plan includes that complete dependent group and computes the transitive closure before changing any member.
Rollback never guesses or silently replaces an invalid preceding prefix.
A checked synced decision or durability receipt with a missing required member is damage rather than an undecided crash tail.
An incomplete final append wholly above the certified cut is unpublished.
A complete record with invalid checksum, identity or structure, interior corruption, a shortened certified prefix and conflicting decisions refuse recovery without repair.

A checkpoint makes the decisions of all groups it includes durable before publishing its image.
Checked image and tombstone provenance identifies a covered group member after its WAL frame is retired.
Only durable published coverage can replace a frame during group admission.
Live journal records can retire after all members have published coverage.
Their immutable group seals remain while current checkpoint provenance, retained feed history or bounded idempotency records reference them.
A seal records member epochs and coordinates, visible transaction identity and tag without storing metadata per block.
A newly referenced seal and its parent directory are durable before publishing a reference or retiring its journal source.
Unreferenced seals are collected after their checked reference cut is durable.
Recovery admits the complete reference cut before deleting a journal record or seal.

## Snapshots and table lifecycle

The first transaction statement registers one completed-revision cut for each table in its directory snapshot vector.
Later access to a table uses that original cut rather than taking a new snapshot after a cross-table group.
Tables created after the cut cannot silently join that transaction.
Only touched table identities are required to remain valid at commit.

Snapshot registration briefly drains existing table mutation admission in stable table order and then captures the vector under the directory publication gate.
This admission pause keeps the same store owner and leaves feed senders attached; it does not use the DDL path that stops or replaces a store.
It registers history before producers resume, so a completed cut never relies on a discarded before-state.
Metadata admission excludes concurrent multi-table lease acquisition while that ordered drain runs.
No directory publication gate is held while waiting for table leases to drain.
Ordinary single-table writes use their existing table admission and do not acquire the directory gate.
Snapshot registration cost is measured separately from the unused single-write path.

DROP, SWAP, RENAME and restore admission wait for open groups touching their table identities.
They settle durability dependencies and publish lifecycle changes before releasing admission.
An old transaction's later statement or commit detects the changed identity instead of applying to a replacement table.
A multi-table backup pins a single directory group boundary and includes its admitted decisions and referenced seals.
Subset restore checks the backup's current and retained group membership against the selected epochs.
Without `--allow-split` it refuses an incomplete membership set; with that flag it reports the affected visible transaction IDs.

## Block deltas

Block reads and writes retain canonical row preimages and row deltas rather than private whole-chunk copies.
Comparison includes presence, validity, fixed bits and variable values; unchanged floating-point bits remain equal, including NaN payloads.
At commit, a changed observed row conflicts and an equal row permits ABA.
Apply accepted deltas to the current chunk, preserving unrelated rows and chunk-column changes.
Explicit chunk and area operations promote their affected chunks to whole-chunk version validation.
A private chunk read incorporates earlier block deltas and keeps the private form's zero revision and time.
REQUIRED initialization and variable-byte admission still apply to the complete resulting state.
Read preimages, deltas and promoted copies count toward the existing per-transaction and total byte bounds.
Admission publishes prepared state and byte reservations atomically on statement success.

## Limits and request metadata

HELLO reports the configured area chunk and response-byte bounds and transaction read and write chunk bounds.
The server flags are `--max-area-chunks`, `--max-area-response-bytes`, `--txn-max-written-chunks` and `--txn-max-read-chunks`.
Request admission and response decoding use the configured or negotiated bounds rather than fixed chunk-count constants.
The defaults are 4096 requested area chunks, 64 MiB of area response, 1024 written transaction chunks and 8192 read transaction chunks.
Unpaged GET AREA retains its existing array reply and refuses a request beyond its configured bounds.
An explicit LIMIT selects a page reply, and AFTER resumes that page in coordinate order.
Pages bound both visited requested cells and encoded reply bytes, and one chunk that cannot fit produces an error rather than an empty advancing loop.
Paging does not imply an unchanged snapshot across independent requests.

An ID is at most 64 bytes and is scoped to the authenticated user.
Its fingerprint binds the command text, length-framed parameter bytes and effective tag, so a different payload cannot reuse a known ID.
The fingerprint hashes a fixed domain followed by ordered statement records containing little-endian unsigned 64-bit text length, exact text bytes, parameter count and each parameter's length and bytes.
Its final record distinguishes an absent tag from a present empty tag and binds the complete effective tag.
Statement and final-record kinds keep a transaction's statement boundaries distinct from concatenated text or parameter bytes.
For BEGIN ID, the fingerprint covers BEGIN options and the complete ordered transactional statement transcript, including parameter bytes and statement boundaries.
BEGIN alone neither certifies a matching body nor returns an earlier transaction as a completed duplicate.
A duplicate BEGIN opens a private validation session; its statements cannot publish writes outside that session.
Read replies are evaluated against that session's current snapshot and are not retained or replayed as idempotency results.
Transcript recording continues through a private statement failure, so ignored follow-up statements cannot evade comparison.
COMMIT compares the complete transcript before any new commit admission, including when private validation has already failed.
For a known matching outcome it discards private state and returns the retained repeated result; a different transcript returns `INVALID_ARGUMENT reason=id_reused` without mutation.
An in-flight duplicate waits at COMMIT without table leases or chunk locks until its owner has a checked outcome.
If that owner is known not to have committed, the waiting session can obtain the next key reservation and undergo normal snapshot validation.
An uncertain owner outcome requires recovery and cannot transfer write ownership to the waiter.
Applications can use SHOW COMMIT to retrieve an earlier result without rerunning a read-dependent transaction body.
A duplicate in flight waits for that owner; canceling the waiter does not cancel the original group.
A completed duplicate returns the retained transaction and table versions without replaying RETURNING rows.
SHOW COMMIT reports retained versions and whether the complete group is durable.
Expiration or oldest-first byte eviction can remove that record, so absence is a bounded-retention result.
The default retention window is 600000 ms and its byte bound is 64 MiB.
A new record larger than its bound is refused before changing data.
The retained-ID inventory is separate from group seals and binds a checked journal replay cursor and each live key's exact group incarnation.
Only that inventory and admitted newer decisions can reconstruct the ID lookup; an older feed-referenced seal cannot resurrect an evicted binding.
Expiration or byte eviction publishes a durable retirement cut before the same key can obtain a new write reservation.
Publishing that cut first makes its journal cursor durable, without claiming that unrelated relaxed member frames are durable.
ID decision publication and retention-cut capture serialize their short metadata step, so a decision cannot fall before the cursor while its binding is omitted accidentally.
An in-flight reservation remains separately byte-charged and cannot be mistaken for an already completed binding during eviction.
Recovery first resolves group outcomes, restores only admitted live inventory bindings, and then applies newer key incarnations in journal order.
If a newer relaxed incarnation is absent or rolled back after a crash, lookup remains empty rather than falling back to the retired older incarnation.
The inventory and pending reservations obey the byte bound even when old seals remain needed by feed readers.
Inventory replacement keeps the old checked generation until the new generation and its publication are durable.
Every separately named new inventory object and its parent directory are synced before publishing a reference to that generation.

A tag is at most 256 bytes and an explicit tag overrides the HELLO default.
BEGIN captures the session default, and explicit tags on BEGIN or successful transactional writes must agree byte-for-byte for the entire group.
The first explicit tag can replace the captured default; a conflicting explicit tag refuses that statement without changing private data or the chosen group tag.
The checked group decision owns the tag and catch-up obtains it from the retained group seal.
Admission, rollback, uncertain outcomes, crash recovery and split restore receive independent failure tests.

## Failure and acceptance checks

The ordinary-write error matrix independently exercises version-clock reservation failure before SET BLOCK, DELETE BLOCK and SET CHUNK publish a mutation.
Failed deletes and whole-chunk writes are reopened to verify the previous values, existence, producing identity and feed frontier.
`chunkdb_verify` admits the directory journal and certified inventory, resolves every referenced complete group and validates all member or checkpoint proofs without changing files.
The verifier refuses missing synced members, conflicting decisions, invalid certified cuts and unadmitted grouped frames rather than validating each table in isolation.
Snapshot-vector bookkeeping has its own cost scenario, and the ordinary single-chunk hot-path budgets remain merge gates.
