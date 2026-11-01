# Commit group recovery ownership

This owner contract completes the recovery, checkpoint and restore rules of [#136](https://github.com/chunkdb/chunkdb/issues/136).
It uses the checked [group records](COMMIT_GROUP_FORMAT.md) and does not change the meaning of a producing revision.
A revision greater than a group member's revision is never evidence that the member survived.
The `commit_groups` feature owns every additional object described here.

## Checked coverage objects

The directory owns `chunkdb.groups.coverage`, a checked publication root, and immutable objects below `chunkdb.groups.objects`.
Object names are derived from their complete raw SHA-256, and references additionally bind the exact byte length and object kind.
Every object binds the directory identity and has a checked header, exact extent, reserved-zero fields and complete-record checksum.
The root binds its nonwrapping generation, the coverage-tree root and the retained-group reference-tree root.
A pending root additionally binds the exact previous root and a complete replacement plan; a final root has no pending plan.
An unknown tag, changed referenced byte, missing required object or generation overflow refuses admission before repair.
New object files and every newly created parent name are durable before a root can reference them.

The coverage key is table epoch, dimensions and signed XYZ, with zero Z in two dimensions.
A leaf binds the exact currently selected durable artifact: a checked image with producing revision, time, schema, raw length and SHA-256, or a retained-empty identity with the same producing fields and the exact checked retained-record bytes.
A never-written leaf has no artifact and all producing fields zero.
A forgotten-empty leaf additionally binds the admitted deletion floor and the historical retained-empty proof it superseded, but exposes no current producing identity.
That leaf can certify retained group history without making its forgotten tombstone visible to reads or scans.
A retained-empty proof is an owned immutable copy of the admitted record and its inventory context, so later CKIX replacement cannot invalidate the proof.
The artifact itself must pass its native context, schema and content checks; a matching digest alone does not admit an image or tombstone.
A leaf contains a canonical set of covered member references, each containing group identity, exact decision SHA-256, ordinal and the original member's producing revision, time and schema.
The referenced checked decision or seal must agree with every field, table epoch, dimensions and coordinate.
A separate optional producer reference identifies the changed group member that produced the leaf's current identity; observed members never become producer references.
A later ordinary producing mutation clears that producer reference, while preserving covered references that still have another owner.
No metadata is stored per block.

Coverage objects explicitly certify that their bound artifact contains the effects of their listed members after complete replay and schema admission.
The checkpoint owner can create that certificate only from a fully validated source state and admitted member proofs.
Startup checks the complete published certificate, its exact selected artifact and its referenced decisions; it does not infer coverage from ordering of revisions.
A current WAL suffix is replayed over that selected artifact, and its group frames still require independent group admission.

The two trees use immutable bounded pages and checked copy-on-write paths.
Their keys have a fixed maximum length, each page is at most 64 KiB, and a path is bounded by that key length rather than directory population.
Leaf reference sets use their own bounded copy-on-write trees, with exact count, total bytes, page order and a complete-set digest in the leaf.
A mutation pins the existing reference-set root instead of enumerating or copying all covered members, and adding or removing one reference changes only its bounded tree path.
A reference set has a format-wide 256 MiB ceiling and is streamed during admission; current transaction count limits are not decoder limits.
Group preparation reserves reference-storage bytes before mutation and refuses a group that cannot retain its required proofs.
One checkpoint writes its changed leaf pages and ancestor paths, rather than rewriting every table's coverage.
Object traversal detects duplicate keys, cycles, missing pages, invalid child ranges and counts inconsistent with measured extents before publishing owned state.
The exact page codec and its sparse oversized-file tests precede owner implementation.

## Relaxed anchors and dependency settlement

A changed member's rollback anchor is the decision's earlier durable image, retained-empty identity or never-written state.
The live group owner pins that exact image or retained record and its original WAL generation before appending any grouped member.
A pin prevents checkpoint replacement, retained-record compaction, truncation or unlink from removing a required anchor or member extent.
It does not claim that the original volatile WAL end is durable.
Existing durable images and retained records need no new pre-group WAL sync, and never-written anchors create no synthetic identity.

The owner keeps seekable shared native handles and checked extents, not whole-WAL byte vectors.
Recovery streams at most one bounded frame at a time over the checked durable anchor.
It verifies any certified pre-group prefix against its exact raw SHA-256 before admitting crash-shaped loss above that prefix.
A missing or incomplete relaxed suffix can move the rollback cut only to the last admitted complete frame, never below its certificate or through interior damage.
The complete transitive dependent-group rollback set and all resulting cuts are validated before any file is shortened.
An agreeing repeated cut is idempotent; incompatible cuts refuse without changing any member.

Value-equal observed members with a nonzero producing identity need a checked observation capsule because their decision fields contain no new frame extent.
That capsule binds the decision SHA-256, ordinal and an exact image, retained-empty or complete WAL-prefix proof for the observed producing identity.
It is streamed and context-checked like coverage, and a synced decision first makes its observation capsules and selected producing states durable.
A relaxed decision without an available complete observation proof cannot be promoted to durable or partially recovered; whole-group rollback applies when the proof was lost as an unpublished relaxed append.
An observed zero identity needs no file, has zero time and schema, and cannot acquire a producer reference.
The observation-capsule layout is an additional checked codec prerequisite; the decision encoding remains unchanged.

A group owner stores its pinned native handles and dependency edges independently of resident chunk mutexes.
Settling a group copies and pins its complete member handles under the short group-owner mutex, releases that mutex, and syncs them in table-epoch and coordinate order.
It takes no member chunk mutex, table admission or directory publication gate during those syncs.
Only after all required bytes and parent names are durable does it sync the exact decision's durability receipt and publish the certified journal cut.
Concurrent settlers share one completion state; waiting for that state cannot require a mutex held by a settler.
Checkpointing or a later synced write can wait for that settlement while owning its current chunk, because settlement never enters that or any other chunk lock.
Failure fences dependent owners and retains their pins instead of silently weakening durability.
An unconditional ordinary write to a chunk with no live group dependency or retained coverage retains its existing path and performs no directory lookup, allocation or sync.

## Checkpoint transition and supersession

Before publishing a checkpoint, settle every group whose effects or observed state the checkpoint will cover.
Collect the complete checked state and its existing covered set under the normal checkpoint ownership and chunk lock.
Prepare the exact new image or retained-empty proof and coverage leaf without changing the selected artifact.
Preserve the old selected artifact under an immutable durable name if canonical replacement would otherwise remove its only link.
Sync the staged new artifact, staged leaf pages and all newly named objects before publishing a pending coverage root.
The pending replacement plan binds each coordinate, old selected artifact, staged new artifact, new leaf and both root identities with exact lengths and raw digests.
It authorizes only installation of that complete prepared new state, never truncation back to an arbitrary higher revision.

After the pending root is durable, install each canonical artifact, sync the affected directories and publish the final root selecting the new leaves.
Only after that final root is durable may the old image, covered WAL frames, staging links and obsolete retained-record owners be reclaimed.
Recovery admits the complete pending plan, every unchanged leaf and every old or new candidate before doing any installation.
For each replacement, it accepts an exact already installed new artifact or redoes installation from the durable staged copy.
A missing stage is acceptable only when the exact new artifact is already installed and durable; an arbitrary same-version file is not a substitute.
The complete plan is redone after the irreversible pending publication, and failures retain the plan and fence its tables.
No per-table constructor, verifier repair or normal crash-tail trim runs before this directory-wide admission.

A later ordinary WAL mutation does not need a coverage-root update merely to append its own frame.
Its replay state carries the earlier covered set and clears current producer ownership when its producing identity changes.
When it later replaces an image or collects an empty chunk, it uses the same checked transition and transfers still-owned references to the new artifact.
An old reference can therefore remain valid after a member's original WAL frame and original image both disappear.
A coverage update never adds an omitted member solely because a later image has a larger revision.
Schema translation preserves each original member identity while the selected artifact uses its actual checked schema history.

Read-only open and offline verification admit root, trees, selected artifacts and group records into one private observation before serving any group member.
A read-only view uses the existing equal-even directory publication bracket and rejects a changed generation before publishing the view.
Already pinned immutable objects remain valid across a later root replacement.
A read-only owner encountering a durable pending replacement refuses service until the writable owner completes its plan; it never repairs or chooses a partially installed side.
The read-only owner reports an unchanged invalid observation as damage; it does not retry corruption as a generation race.
Offline verification follows the same references without installation, cleanup or journal-tail repair.

## Reference cuts and lifecycle

The retained-group tree is the authoritative ownership cut for decisions and seals.
For each exact group incarnation it binds its decision digest, complete member set, durability state and references from current producer leaves, covered members, rollback anchors, feed history, retained IDs and active recovery transitions.
An in-process reader pins its admitted generation; retirement marks an incarnation unavailable for new pins before testing that its existing pins have drained.
Durable feed-slot and ID roots are captured under their publication ownership and remain pinned until the new reference cut is durable.
A feed-referenced seal never reconstructs an evicted ID binding.

A live journal decision can retire only after all changed and observed members have durable coverage or observation proofs and a durable exact seal preserves every remaining owner.
Current producer references keep that group and its full membership available even if another member has since been overwritten.
After all current producer, feed, ID, rollback and transition owners are gone, its covered references can be removed from leaves in a checked batch.
The final batch publishes a reference root that no longer names the incarnation before deleting its seal, capsules or last journal source.
A bounded streamed mark plan prepares those changed paths; deleting a seal never discovers or rewrites the directory opportunistically.
An interrupted cleanup resumes from the exact admitted cut and cannot fall back to an older ID generation or revive a retired group.

Table rename changes current catalog names but leaves epoch ownership and historical result labels intact.
DROP and SWAP first drain open groups and settle dependencies, then publish their epoch transition and the corresponding reference-tree update as one checked lifecycle decision.
A dropped epoch can remain a history-only member while a current producer in another table, a feed slot or an ID still retains the group.
Its last required coverage, schema history and seal stay in immutable history ownership instead of becoming a path to a replacement table.
Explicit tombstone compaction first settles dependencies and certifies the current empty identity, then publishes its deletion floor and forgotten-empty leaf in the same recoverable decision.
It clears current producer ownership for that forgotten identity while retaining any history-only member proof still owned by another producer, feed slot or ID.
A history-only proof never recreates the forgotten identity in the active coordinate index.
Schema and tombstone-history retirement keep every schema or retained proof needed by a coverage leaf, rollback anchor or feed base until the same checked reference cut releases it.
An epoch replacement never satisfies an old member merely because names or coordinates match.

## Backup and restore

A directory backup pins one completed group boundary, its certified journal cut, coverage root, reference root, retained-ID generation and counter reservation ceiling.
It captures all referenced immutable objects and exact selected artifact prefixes before releasing their collection pins.
An actively written hard-linked WAL or index is copied only to the admitted captured extent, with bounded streaming and cancellation checkpoints.
Every selected table is validated against that same group cut before the backup's complete marker is published.
A subset backup carries complete checked membership and proof metadata for every group referenced by its selected current state or retained history, including omitted source epochs.
It also captures the checked proof objects and exact referenced artifact bytes needed to admit omitted members, held as history-only witnesses rather than selected table state.
It cannot replace absent source data with a fabricated successful member proof.

Restore first admits the entire source backup and group closure without modifying destination files.
The destination stages complete resulting chunk states and retained history under its new directory identity and table epochs.
A whole-root restore can preserve visible transaction values only into an empty new counter scope, with its reservation ceiling durably set above every restored exposed value.
Restore into an existing directory allocates a fresh nonoverlapping destination counter range and records a source-directory and transaction to destination-transaction mapping before publication.
Counters never decrease and a failed restore cannot reuse a reserved exposed range.
Immutable table-name result labels and binary tags remain the original historical labels and bytes.

Restore writes a checked provenance capsule binding the source backup inventory digest, exact original decision or seal digest, source member ordinals, destination directory and epoch mapping, destination transaction mapping and exact rewritten decision digest.
Destination members are ordered canonically by their new epochs and coordinates.
Every retained GROUP frame is rewritten with its destination group identity, member count and new ordinal, then has its checksums and exact frame digest recomputed.
The fixed-size GROUP body preserves frame extents and offsets, but no old frame digest is reused for rewritten bytes.
WAL and image headers, archive-base descriptors, rollback-anchor hashes, coverage leaves, receipts, seals and journal-prefix certificates are rebuilt for the admitted destination contexts.
Source original capsules remain available through the provenance object; they are never treated as local replay authority.
The restored complete checkpoint state makes the destination covered group durable before any local certified outcome is exposed.
Retained feed archives are rewritten and validated with their exact before-state and complete schema histories rather than silently discarded.
The restore publication decision binds every staged table and group root before any destination manifest becomes visible.

Without `--allow-split`, restore refuses a selection that excludes any member of a group retained by current state, feed history or ID history.
With that flag, a checked split capsule additionally binds the complete source membership, selected and omitted ordinal sets, full source admission proof and the rewritten selected destination decision.
It authorizes only that recorded selection and reports the affected source visible transaction IDs and any destination mapping.
Selected frames are rewritten to the selected count and canonical ordinals; omitted epochs never become ordinary unresolved local members.
Split history retains original source identity and tag as provenance and explicitly reports that the restored selection is incomplete.
It does not recreate an ID lookup entry or imply a complete repeated outcome for the original request.

A whole-root restore copies live retained-ID bindings only when their authentication scope is restored with the same principal ownership and the complete group is selected.
The restored inventory binds the rewritten local decision, mapped transaction and original fingerprint rather than an old directory cursor.
A subset restore into an existing authentication scope drops imported ID lookup entries and reports that omission; matching principal text alone is not identity proof.
Retained request bytes inside an imported seal never bypass this rule.
An expired, evicted or split binding remains absent even if its source seal is retained for feed history.

## Required failure checks

Checkpoint tests crash before and after staged-name sync, pending root, each artifact replacement, final root and reclamation, including a member superseded by an ordinary write before restart.
Valid same-length replacement images, retained records and seals with different contents must fail their raw bindings.
Relaxed tests lose only an uncertified preceding suffix, lose a certified prefix, damage an interior frame and roll back transitive groups before asserting that every original file is unchanged on refusal.
Observed members, never-written anchors and header-only WALs must not create revisions or retain invented metadata.
Dependency tests hold one member lock while another settles the group and prove settlement takes no second chunk lock.
Reference tests keep a seal through one current producer, then through a feed slot and an ID, and collect it only after all checked owners retire.
Restore tests cover negative XYZ, reordered new epochs, root and subset counters, selected and omitted members, split reports, principal ownership and absent-versus-empty tags and IDs.
Sparse oversized objects, cyclic trees and count/extent mismatches fail before large allocations, and no test substitutes a full WAL buffer for bounded admission.
