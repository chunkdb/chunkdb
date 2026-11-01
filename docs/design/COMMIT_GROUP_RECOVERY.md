# Commit group recovery ownership

This owner contract completes the recovery, checkpoint and restore rules of [#136](https://github.com/chunkdb/chunkdb/issues/136).
It uses the checked [group records](COMMIT_GROUP_FORMAT.md) and does not change the meaning of a producing revision.
A revision greater than a group member's revision is never evidence that the member survived.
The `commit_groups` feature owns every additional object described here.

## Checked coverage objects

The directory owns `chunkdb.groups.owner`, a checked publication root, and immutable objects below `chunkdb.groups.objects`.
Object names are derived from their complete raw SHA-256, and references additionally bind the exact byte length and object kind.
Every object binds the directory identity and has a checked header, exact extent, reserved-zero fields and complete-record checksum.
The root binds its nonwrapping generation, the coverage-tree root, retained-group reference-tree root, selected certified journal inventory and selected retained-ID inventory by identity, generation, exact length and raw SHA-256.
The reference-tree checkpoint also binds its own complete journal replay cursor and raw-prefix proof, independently of the possibly later certified cut.
It also binds the counter owner and minimum required reservation ceiling; the independently admitted current ceiling cannot be lower.
A checked INACTIVE root instead has zero selected-object references and certifies that grouped frames have never been authorized; ACTIVE roots contain the complete selected contexts and never revert to INACTIVE.
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
For a volatile WAL mutation this clears only the live replay reference; the durable leaf retains its earlier producer owner until the superseding state has complete durable coverage.
Retirement cannot use a merely later relaxed revision to discard that owner, because losing its suffix could make the earlier group producer current again.
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
The owner-state codec additionally requires a nonzero matching directory and owner identity, a nonzero generation, no selected objects or pending plan in INACTIVE, and complete selected contexts in ACTIVE.
Its transition validator refuses owner changes, generation reuse, ACTIVE-to-INACTIVE replacement and an incompatible pending predecessor before publication.

## First use and complete discovery

The checked directory or standalone manifest has a stable nonzero group-owner identity whenever the `commit_groups` feature is enabled.
Fresh creation stages and syncs the checked INACTIVE root and its parent names before publishing that manifest; ENABLE FEATURE does the same under its existing checked activation decision before exposing the feature.
Standalone construction completes this publication before exposing the store, and catalog construction or feature activation completes it before exposing any table lease.
A committed feature-enabled manifest without its matching checked owner is damage, not permission for late catalog initialization.
The runtime coordinator remains lazy, and an INACTIVE root lets startup omit grouped-WAL discovery because no GROUP frame has ever been authorized.

First grouped use stages and syncs empty journal, coverage, reference and ID objects and their parent names, then atomically publishes and syncs an ACTIVE root before appending any GROUP frame.
This transition uses only the independent owner-transition serialization and pinned file I/O; it never acquires catalog operations, feature metadata admission or another table lease.
It is therefore admissible from an existing direct C++ conditional operation already owning a table lease, and from a standalone store.
An ACTIVE owner never reverts, including after its last group retires, and a missing selected object is damage.
A failed activation publishes no grouped frame; an uncertain ACTIVE publication fences the coordinator until checked admission resolves it.
This is a once-per-directory boundary and adds no sync or journal lookup to unused ordinary writes.
Grouped preparation, first activation, recovery and every ACTIVE-owner mutation require admitted exclusive writer ownership of the directory and participating stores.
An incompatible `allow_multiple_processes` context refuses before group publication or owner mutation; this protocol does not add interprocess group coordination or weaken the existing shared-writer restriction.
Read-only owners can admit a matching stable context but cannot activate, repair or collect it.

Each feature-owned table manifest binds its directory identity, stable owner identity and standalone-or-catalog role independently of whether a GROUP frame remains in its WAL.
A standalone binding names its own root; a catalog binding requires the matching checked directory owner context injected before store admission.
Opening a catalog-owned table in isolation, including read-only open, refuses without that context and never bootstraps a local standalone owner.
Creation and ENABLE FEATURE publish this immutable scope with their checked manifest decision; backup preserves it as source context, restore rewrites it for the destination, and offline verification checks it against its admitted directory owner.
No scope contains a filesystem path supplied by persisted group metadata.

When the owner is ACTIVE, startup enumerates every managed table's WAL namespace, including canonical orphan coordinates not yet represented by the live coordinate index.
It also enumerates the history-only epochs admitted by the selected owner roots and refuses an unclaimed owned epoch rather than repairing it independently.
It streams every complete available WAL frame to collect group identities, claimed counts, actual ordinals, table contexts and first group offsets before a table constructor can trim anything.
Canonical placement, dimensional arity, checked headers, frame limits, checksums and schema contexts are admitted during this pass.
A declared member count is only a consistency claim; allocation and plan cardinality derive from measured valid records, not that field.
Private checked pages and a bounded cache hold the recovery catalogue, so the scan does not require a whole-WAL buffer or an unbounded in-memory member map.
A plan-storage failure refuses recovery before changing original files.

A group with no checked decision is discovered from its surviving frames even when some logical members never reached disk.
Its first frame on every discovered coordinate is a rollback boundary, and the complete preceding prefix is admitted over that coordinate's checked durable image or retained-empty state.
A valid header with no preceding producing frame is an unwritten base; retaining that header after rollback does not invent a producing identity.
All found members, overlapping later groups and their transitive rollback cuts are gathered and validated before any truncation, orphan cleanup or archive-alias normalization.
A crash after different-table member appends but before decision append therefore cannot leave one table repaired while another still exposes the group.

## Relaxed publication and durable metadata cuts

Selected owner, journal, reference and ID roots describe durable metadata checkpoints, not every live relaxed decision.
After initial ACTIVE admission, a relaxed group reserves its in-memory dependency, producer, feed and ID ownership before appending frames, flushes the required user-space buffers and appends its checked decision without a mandatory root sync.
Its logical states and prepared live references are published together under the short directory visibility gate while all validated member locks remain owned.
The coordinator's admitted complete decision suffix and pinned native generations own these references until a later durable cut captures or retires them.
Neither registering that live group nor retaining its volatile ID outcome forces a new owner or reference-root transition.

Startup first admits selected durable roots, then streams every not-yet-retired checked decision and globally discovered WAL member into its private plan before reconstructing live references.
Decisions after the reference-tree replay cursor remain implicit live owners even when a later certificate already covers their journal bytes.
An uncertified complete decision contributes only after its whole group passes admission; an absent or crash-shaped incomplete relaxed outcome instead removes its entire dependent live ownership through the global rollback plan.
A newer lost or rolled-back ID incarnation leaves the retired earlier incarnation absent.
Durable metadata cuts may include a relaxed decision without a member-durability receipt, so a retained reference alone is never a durable-member proof.
Checkpoint coverage, member-durability promotion, journal-generation switching, ID retirement, lifecycle publication and destructive reclamation require their checked durable owner transition.
Their publication certifies only the explicitly bound metadata or member proofs and does not incidentally promote unrelated relaxed groups.

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
Before any group decision is appended, every changed frame and required observed prefix is appended and flushed out of its user-space staging batch and WalAppendStream buffer into the exact pinned native generation.
This buffer flush is required even for relaxed publication and is distinct from a durability sync.
The owner verifies the complete native extent before exposing its frame or prefix digest; syncing a second handle cannot substitute for flushing the first handle's buffers.
Settlement pins have real sync rights, such as a duplicated writable native writer handle on Windows or a writable file descriptor on POSIX, with the existing sharing and no-follow admission.
A read-only SharedFileInput handle cannot serve as that Windows settlement handle, and settlement never reopens a canonical pathname that might now name another generation.
Settling a group copies and pins its complete member handles and earlier dependency edges under the short group-owner mutex, then releases that mutex.
It settles the transitive earlier groups before its own receipt, in admitted decision order, and syncs member handles in table-epoch and coordinate order.
Dependency edges always point to earlier admitted decisions; a repeated identity or cycle is damage rather than a wait graph to execute.
It takes no member chunk mutex, table admission or directory publication gate during those syncs.
Only after all required bytes and parent names are durable does it sync the exact decision's durability receipt and publish the certified journal cut.
Concurrent settlers share one completion state; waiting for that state cannot require a mutex held by a settler.
Checkpointing or a later synced write can wait for that settlement while owning its current chunk, because settlement never enters that or any other chunk lock.
Failure fences dependent owners and retains their pins instead of silently weakening durability.
An unconditional ordinary write to a chunk with no live group dependency or retained coverage retains its existing path and performs no directory lookup, allocation or sync.

## Journal and ID generation transition

The owner root is authoritative for the matching journal and retained-ID contexts; conventional filenames are not an independent selection mechanism.
A journal compaction first closes group and ID publication admission before chunk locks or table leases are acquired, drains already registered publishers and captures one complete old journal cut.
The closed gate refuses new group registrations, not the completion of an already registered publisher or a durability receipt for an earlier group.
Those continuations use independent owner-transition serialization without re-entering closed group admission, acquiring another table lease or waiting for another member chunk lock.
A publisher retains its original registration through its decision and final logical publication instead of dropping it and requesting fresh admission.
While a compactor drains or stages reversibly it holds no owner-transition permit; a receipt can advance the current journal and root, requiring the compactor to re-admit and rebase its capture before pending publication.
Once a compactor owns the irreversible transition permit it waits for no chunk or group settlement, and later receipts select the admitted final journal generation after that continuation completes.
Ordinary ungrouped writes keep their existing admission and continue independently.
The compactor rebuilds the live ID set from the admitted old inventory, its exact newer decision suffix and durable retirement cuts, while keeping in-flight reservations distinct from completed bindings.
It writes a new journal generation and an ID inventory whose cursor is the end of its fully processed new generation, preserving live incarnations through exact decision or seal bindings.
It cannot copy an old cursor numerically into a different generation or replay an old retired incarnation to fill an absent key.

All new journal bytes, certified inventory, rewritten ID inventory and names are durable before publishing a pending owner root.
The pending plan binds the complete old and new selected journal, ID and reference-tree replay contexts together, including their prefix digests and last complete record identities.
While pending, new group publication and ID mutation remain excluded and readers cannot choose a mixture of old and new roots.
Recovery admits both complete contexts and the exact transition before completing installation of the new owner root.
It never passes an old ID context to the new journal's cursor validator or treats an old pinned file as permission to fall back.
Only after the final matching root is durable can publication resume and the checked reference cut release obsolete generations.
Existing readers can finish through already pinned old immutable roots, but cannot publish new ownership through them.
The same selected-root rule applies to ID eviction and ordinary certified-cut advancement; updates within one journal generation retain a cursor only when it remains a checked complete prefix of the selected cut.

## Checkpoint transition and supersession

One owner-transition continuation serializes all pending root updates through their final publication, including checkpoints, group-reference updates, journal and ID switches, lifecycle changes and restore.
The owner-transition permit is separate from the short directory visibility gate and can remain owned during file sync without holding that visibility gate.
Every updater acquires or drains its table admission, pins its coordinate checkpoint ownership and finishes all required chunk-lock acquisition before requesting the permit.
Group dependency settlement and other work requiring a separate owner transition also finish before requesting the permit.
An updater may retain already owned chunk locks while waiting for the permit, but no permit holder waits for a table lease, new chunk mutex, catalog operation, group settlement or another owner transition.
This preserves the group's validated write set through its decision without introducing a permit-to-chunk acquisition edge.
Prepared ownership pins keep its exact selected artifacts and generations available while it waits.
Reversible staging captures an expected root generation; after receiving the permit it must re-admit against the current root and rebuild or rebase changed paths if another updater advanced it.
No updater may publish a pending plan derived from an obsolete root or replace another updater's pending plan.
After pending publication, the same continuation alone completes every installation and final root; failure leaves that exact pending plan fenced for recovery instead of allowing another update to overwrite it.

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
A read-only view admits the coordinator's directory-wide group publication generation together with the participating table generations and rejects a changed or odd observation before publishing the view.
The directory bracket is an additional group-owner responsibility; separate table brackets alone do not establish a cross-table observation.
Ungrouped writes do not acquire the directory publication gate.
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
Reclamation never passes that reference checkpoint's admitted replay cursor or omits an implicit owner from a newer complete decision.
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

A directory backup first pauses new table and group mutation admission, drains already admitted writers and obtains its maintenance ownership without holding the directory visibility gate.
It acquires any required table or chunk ownership and pins the existing immutable users-registry snapshot before entering the short directory capture section.
It never holds the visibility gate while waiting for a lease, chunk lock, admission drain, native sync or file copy.
The capture pins one completed visible journal boundary, its distinct certified journal cut, coverage root, reference root and replay cursor, retained-ID generation and cursor, and counter reservation ceiling.
The visible boundary includes every admitted complete relaxed decision needed by the captured table states, including decisions above the certified cut.
Copying only the certified prefix is invalid when selected table prefixes contain a later logically published group.
Publication resumes after those immutable objects, exact table prefixes and closure witnesses are pinned; copying proceeds outside the visibility gate.
It captures all referenced immutable objects and exact selected artifact prefixes before releasing their collection pins.
An actively written hard-linked WAL or index is copied only to the admitted captured extent, with bounded streaming and cancellation checkpoints.
Every selected table is validated against that same group cut before the backup's complete marker is published.
A subset backup carries complete checked membership and proof metadata for every group referenced by its selected current state or retained history, including omitted source epochs.
It also captures the checked proof objects and exact referenced artifact bytes needed to admit omitted members, held as history-only witnesses rather than selected table state.
It cannot replace absent source data with a fabricated successful member proof.
The backup carries the exact visible decision prefix and validates complete group closure before its complete marker, without misrepresenting journal-byte certification as source member durability.

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
The rewritten split decision has no request binding; its original ID and fingerprint remain only in the source provenance capsule.
When the selected destination result set has one table and no restored ID, its rewritten decision has transaction zero as required by the decision codec; the source transaction remains in split provenance and the reported mapping.
Split history retains original source identity and tag as provenance and explicitly reports that the restored selection is incomplete.
It does not recreate an ID lookup entry or imply a complete repeated outcome for the original request.

A whole-root restore copies named-principal ID bindings only into an empty destination authentication scope while restoring the complete checked source `chunkdb.users` bytes unchanged, including its existing secret, credential verifiers and grants.
The source and staged destination registry lengths and raw SHA-256 must agree, every copied named principal must exist in that registry, and no user merge, rename or credential substitution is admitted.
This uses the existing users format and introduces no new user identity or security format.
The distinguished anonymous scope can be copied only as that exact non-user scope during whole-root restore; matching arbitrary principal text is never the proof.
The complete group must be selected in either case, and a missing ownership proof omits the binding with an explicit report.
The restored inventory binds the rewritten local decision, mapped transaction and original fingerprint rather than an old directory cursor.
A subset restore into an existing authentication scope drops imported ID lookup entries and reports that omission; matching principal text alone is not identity proof.
Retained request bytes inside an imported seal never bypass this rule.
An expired, evicted or split binding remains absent even if its source seal is retained for feed history.

## Required failure checks

Bootstrap tests crash around inactive-root name sync and CREATE or ENABLE FEATURE manifest publication, then verify that no table lease can observe a missing claimed owner.
Scope tests checkpoint every grouped WAL away, then refuse isolated catalog-table open and a mismatching injected owner before any data read or repair.
First-use tests hold a direct conditional table lease while catalog exclusive admission waits, activate through the independent owner root, and prove that no reverse catalog-lock acquisition occurs.
Activation tests crash before and after ACTIVE publication and its directory sync, assert that no GROUP frame precedes confirmed activation, and retain lazy startup for an unused INACTIVE root.
Concurrent checkpoint and ID-generation updates stage from the same root, then prove that only one pending continuation owns installation and the other re-admits rather than losing either leaf.
Buffer tests retain real user-space buffered bytes, then require their flush before relaxed decision publication and their exact writable native generation during settlement after canonical replacement.
Relaxed tests assert no per-group owner-root sync, reconstruct a complete suffix above its selected cut, and roll back a lost suffix without reviving an older retired ID.
Discovery tests crash after different-table member appends and a newly orphaned coordinate but before any decision, then prove that all discovery and validation precedes every repair.
Journal tests crash after selecting the new generation and before the ID cursor alias is installed, then require one matching context without reviving an expired or rolled-back incarnation.
Checkpoint tests crash before and after staged-name sync, pending root, each artifact replacement, final root and reclamation, including a member superseded by an ordinary write before restart.
Valid same-length replacement images, retained records and seals with different contents must fail their raw bindings.
Relaxed tests lose only an uncertified preceding suffix, lose a certified prefix, damage an interior frame and roll back transitive groups before asserting that every original file is unchanged on refusal.
Observed members, never-written anchors and header-only WALs must not create revisions or retain invented metadata.
Dependency tests hold one member lock while another settles the group and prove settlement takes no second chunk lock.
A three-party barrier test registers a publisher waiting for a checkpoint-owned chunk, makes that checkpoint wait for an earlier group's receipt, then closes compaction admission and proves that the receipt and original publisher still finish before the captured cut is re-admitted.
Reference tests keep a seal through one current producer, then through a feed slot and an ID, and collect it only after all checked owners retire.
Restore tests cover negative XYZ, reordered new epochs, root and subset counters, selected and omitted members, split reports, principal ownership and absent-versus-empty tags and IDs.
Sparse oversized objects, cyclic trees and count/extent mismatches fail before large allocations, and no test substitutes a full WAL buffer for bounded admission.
