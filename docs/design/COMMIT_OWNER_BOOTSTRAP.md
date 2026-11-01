# Initial commit owner admission

This publication contract implements the bootstrap prerequisite of [#136](https://github.com/chunkdb/chunkdb/issues/136) and [recovery ownership](COMMIT_GROUP_RECOVERY.md).
It installs only a generation-one INACTIVE owner and binds its immutable manifest scope before exposing stores or table leases.
ACTIVE and PENDING publication, complete reference-tree authorization and grouped recovery remain separate prerequisites; this component refuses those owner states instead of serving through syntax-only admission.

## Manifest ownership

The existing table and directory manifest envelopes retain their tags because the `commit_groups` incompatibility bit owns the added fields.
A table adds option TLV 12 and a directory adds option TLV 2, each containing the exact checked 40-byte [owner scope](COMMIT_GROUP_OWNER_FORMAT.md).
Each option occurs once and is mandatory exactly when bit 64 is present; a known scope without its feature, duplicate scope, wrong length, zero identity or invalid role refuses.
Unknown compatible features cannot exempt these known-field checks.
The parsed scope is owned separately from mutable options, so option, schema, lineage and version-floor rewrites preserve it without duplicating its TLV.
Codecs never generate identities or infer scope from paths, default feature masks or another artifact.
An earlier feature-enabled development manifest without scope refuses; its missing binding is not repaired by inventing authority.
Manifests without the bit and without scope retain their ordinary ungrouped admission.

A catalog directory scope is `{data_dir_id, owner_id, Catalog}`.
Every catalog table repeats that exact scope while retaining its separate table epoch.
A standalone table uses `{store_id, owner_id, Standalone}` and has no directory manifest.
The directory, owner and table identities are nonzero, and catalog table epochs are unique in the admitted inventory.
Table names are inventory labels; neither the scope nor an owner record supplies a filesystem path.

An opaque checked context owns the admitted directory scope and pinned canonical root observation.
Only complete directory admission constructs it; a public scope value or a caller callback cannot construct catalog authority.
TableCatalog injects that context through StoreConfig before geometry admission, recovery, narrowing cleanup or lease publication.
A single-use store-admission ticket pins its admitted table-directory generation and epoch; an identically scoped copy outside that managed generation receives no authority.
Existing-table pins are opened relative to the admitted native directory and fixed tables namespace, and staged CREATE/MIGRATE pins follow their checked publication into that namespace.
Construction consumes the ticket into an RAII admission owner before geometry selection; failure releases the pin, and a successful constructor releases it before returning while retaining only the shared directory owner context.
A consumed ticket cannot open another store, so a closed native pin is never reused as authority through a saved inode number or copied StoreConfig.
The root context is shared by the catalog tables and table pins do not become one persistent directory descriptor per table; Windows admission pins allow delete sharing so admitted DDL renames remain possible.
These retained facts grant no later grouped-write or ACTIVE-publication permission; those operations require a live exclusive owner lease and their own participant pins.
Admission does not claim to make existing ordinary pathname-based I/O safe against arbitrary namespace changes after the constructor returns, and a leaf pin does not imply ancestor-rename exclusion.
A catalog-owned table opened directly, including read-only open, refuses without the matching checked context.
It never bootstraps a standalone owner as fallback, even after every grouped WAL has retired.
All existing table scopes and epoch uniqueness are checked before any table's pending narrowing or storage recovery can mutate data.

## Fresh-root decision

The fixed `chunkdb.bootstrap.pending` decision owns fresh directory or standalone creation.
Its complete target manifest chooses the directory or store identity and owner identity once; recovery reuses those bytes and never regenerates either identity.
The decision has the following little-endian envelope, followed by its complete target manifest and a final CRC32 over all preceding bytes.

| Offset | Bytes | Value |
| --- | --- | --- |
| 0 | 4 | `CKGB` |
| 4 | 2 | tag 1 |
| 6 | 1 | directory creation 1 or standalone creation 2 |
| 7 | 1 | zero |
| 8 | 8 | complete decision extent |
| 16 | 8 | target manifest extent |
| 24 | 32 | SHA-256 of every raw target-manifest byte |
| 56 | 4 | zero |
| 60 | 4 | CRC32 of bytes 0 through 59 |
| 64 | variable | checked CKDM or CKMF target manifest |
| end | 4 | complete-record CRC32 |

Measured extent, the fixed header and the applicable existing manifest ceiling are admitted before allocating the bounded body.
The total extent is exactly 68 plus the manifest extent and cannot exceed the table-manifest ceiling plus 68.
The target must claim bit 64 and the matching role and identity, and its complete options, geometry and schemas pass normal manifest admission.
The decision owns fixed target names only; it is not a general file-redo or arbitrary-root publication format.

The creator owns an exclusive ProcessLock bound to the actual native root before committing the decision.
A constructor without a long-lived lock can take one short exclusive bootstrap lock, reread under it and release it only after initialization; conflicting acquisition refuses without retry.
An explicitly borrowed bootstrap lock is checked by native root and fixed lock-child identity rather than trusting `acquire_process_lock=false`.
Catalog table construction instead uses its injected owner context and never creates an owner under the table directory.
The short creation lock does not authorize grouped operations or ACTIVE-owner mutation in an incompatible shared-writer context.

Fresh admission requires no existing manifest or owned data except checked interrupted-bootstrap state and the existing narrowly admitted empty coordinate inventory.
An existing unpublished empty inventory fixes the standalone epoch and must agree with the planned geometry and exact empty-inventory encoding.
An owner root without a matching creation decision and manifest is damage, not an alternative source of fresh identities.
Unrelated foreign entries retain the existing ignore rules and remain untouched.

Creation publishes in this order while no store or table lease can escape:

1. Validate the complete plan and current owned namespace, then durably publish the decision file and its parent name.
2. Publish any required checked empty coordinate inventory, then install and sync the derived 512-byte INACTIVE root and its parent name.
3. Publish and sync the exact target manifest bound by the decision.
4. Remove the decision and sync its parent before returning an initialized owner context.

The root publisher accepts only an admitted persisted creation decision and constructs INACTIVE internally; it accepts neither an arbitrary CommitOwnerRoot nor a caller-declared permission flag.
It uses the native no-follow, exclusive publication and exact-generation durability rules of [artifact I/O](COMMIT_OWNER_ARTIFACT_IO.md), without creating an unused object directory.
A matching already installed INACTIVE root is re-admitted and file-and-directory synced; another scope, generation or state refuses before manifest publication.
If the target manifest already exists, its exact bytes must match the decision and its matching durable root and inventory must already exist.
A committed manifest with a missing or damaged claimed root is damage even when a creation decision remains; redo never disguises that loss as initialization.
Before the manifest exists, a valid pending decision can complete its missing intended artifacts after all preflight checks.
Read-only opening never completes a decision and refuses pending bootstrap.
Uncertain decision or manifest publication prevents constructor success; restart admits the same persisted decision before cleanup or replay.
Pre-decision failures retain only unselected private work, and no ambiguous existing name is removed by a pathname check.

The early geometry probe can read a pending standalone target to validate constructor geometry and schema before acquiring the writer lock.
That probe grants no serving or repair authority; writable initialization re-admits the decision and namespace under its exclusive lease.
Both manifest-less constructor paths recognize checked pending bootstrap before ordinary unconverted-directory refusal.

## Checked feature activation

Catalog `ENABLE FEATURE 'commit_groups'` uses the existing exclusive feature gate, complete table inventory and drained table admission.
It waits for leases before taking operations or resource-publication locks, and performs no late initialization from an already held table lease.
The feature journal becomes CKFA tag 2: its existing 32-byte header is followed by a fixed 40-byte scope and the existing target-name/epoch records and final CRC32.
For bit 64 the scope is the chosen nonzero catalog scope matching the journal directory identity; every other feature requires those 40 bytes to be zero.
The minimum extent is 76 bytes and the existing measured byte and inventory bounds remain enforced.
CKFA tag 1 retains its historical layout and recovery for known features other than bit 64, with no journal-owned scope and preservation of any independently admitted manifest scope.
Tag 1 claiming bit 64 refuses; only tag 2 can authorize its recorded owner binding.
No new standalone activation command is introduced: an existing unowned standalone store remains usable for ordinary operations and refuses grouped operations until an explicit checked activation interface admits it.

Before its decision, activation admits every current participant and reserves its completion bookkeeping.
Without a pending decision, the old inventory must be consistently unowned or consistently bound; a partial unexplained claim is damage rather than a no-op or repair opportunity.
The chosen owner ID is stored in the durable CKFA decision before any canonical owner file or feature bit is installed.
Recovery admits every named epoch, the exact inventory, and every old or intended-new scope before changing any participant.
It allows only the old unowned state or the exact recorded new scope, never a different owner, unrelated standalone root or conflicting bootstrap/migration decision.

Completion first installs and durably syncs the derived INACTIVE root, then publishes the directory scope and feature bit, then each table's identical scope and bit.
If any table already carries the intended claim, the directory claim and canonical root must also be present and consistent.
A directory already carrying the claim cannot recover a missing canonical root by recreating it.
Each rewrite unions only the requested feature and binding into freshly validated manifests, preserving current schemas, options, lineage and unrelated feature masks.
All table writes and their parent names are durable before the pending decision is removed and its parent synced.
Only then are the prepared catalog/store contexts installed and all table admissions resumed.
An uncertain or failed post-decision completion fences directory metadata and all participants through the existing feature-recovery health path until writer recovery finishes.
Read-only startup validates a pending decision but refuses repair, and offline verification reports a checked pending activation without selecting a mixed context.
An already enabled no-op still validates the owner and every table binding and performs no initialization.

## Other publication and transfer paths

CREATE and MIGRATE CREATE carry the admitted catalog scope in their privately staged manifests before the existing table-directory publication.
Migration redo validates every before/after manifest scope against its directory owner before any file or directory action; generated identities come from its checked journal images.
Mutable manifest replacements and feature activations for other bits preserve scope exactly.
Fresh default constructors keep bit 64 but must use real bootstrap ownership; raw manifest fixture helpers must supply explicit scope and actual owner artifacts when testing an initialized store.
Fixtures intentionally testing a disabled feature can omit both bit and scope, and pure codec fixtures do not acquire serving authority from serialization.

An INACTIVE-only catalog backup pins the exact canonical owner file and includes it in the checked backup inventory beside its unchanged manifest scope.
It reads the canonical root before the first table capture and again after the last capture; a changed, missing, ACTIVE or PENDING selection refuses before completion.
The checks use new native opens of the fixed canonical leaf, not a second read of the initially pinned inode, and hold no owner gate while waiting for table or chunk admission.
The never-reverting owner-state rule makes an unchanged generation-one INACTIVE observation sufficient for this restricted cut.
Unselected private objects are omitted because an INACTIVE root references none; this rule cannot handle or silently strip an ACTIVE owner.
Backup validation admits the root and every table scope against the directory before interpreting its chunks, and preserves existing exact inventory checks.

Restore of a fully admitted INACTIVE source chooses one new directory identity, one new owner identity and the existing new table epochs within its guarded private copy.
A closed restore-plan admission binds the complete source backup and intended rewritten scope; its root publisher still constructs only INACTIVE and exposes no general root replacement.
The source canonical file is replaced only inside that guarded private copy, and the new root is file-and-directory durable before destination manifest claims are installed.
Every table receives the same destination catalog scope, then the complete rewritten inventory and contents are validated and synced before destination publication and guard removal.
The guarded incomplete copy is not a redo source: an interrupted copy is refused, and no retry regenerates identities from its partial manifests.
The restricted `RewriteInactiveRestore(private_copy, admittedBackupRecord)` operation re-admits the exact complete copied source inventory, fixed CKRI guard and private operation name under one native directory pin.
It admits the source generation-one INACTIVE root and every directory/table scope, derives the destination directory and owner identities internally once, and returns a CKDM preserving unrelated feature flags and options only after its private canonical root is durable.
The operation accepts no caller root, scope, callback or writer-permission flag and creates no ProcessLock artifacts in the copied inventory.
The returned manifest's one chosen scope governs every later table and migration rewrite; a second invocation after root replacement fails source-context admission rather than creating another identity.
Guard, copy, root-generation and extent substitution refuse without changing the external source backup or visible destination, and a crash after the private-root replacement remains guarded and cannot be opened or resumed.
Restore cleanup retains the native no-follow safeguards of [#150](https://github.com/chunkdb/chunkdb/pull/191); this component depends on that change rather than recreating generic temporary-tree cleanup.
A typed restore-ownership failure records that the pinned private directory no longer admits its current name, including failure to establish that native identity; the outer restore handler preserves that original error and retains the uncertain name instead of passing it to pathname cleanup.
The exact copied source-root generation is admitted and file-synced through the pinned directory before reopening writable directory access; reopening and full native-identity failures retain the same uncertain-ownership outcome.
That outcome owns bounded diagnostic bytes and has nonallocating, noexcept construction, copy and `what`, so allocation failure while establishing or formatting native identity cannot restore pathname cleanup authority.
Other restore failures keep the existing native cleanup policy, and this boundary does not claim protection against arbitrary same-user namespace changes throughout ordinary restore I/O.
ACTIVE/PENDING restore, grouped history, IDs, tags, split decisions and owner-reference rewriting require the separate complete recovery/restore owner and are explicitly refused by this INACTIVE-only component.
Offline verification checks directory/root/table context before chunk replay, rejects isolated catalog-table verification without directory context and never initializes a missing owner.

## Failure checks

Crash children cover every decision, inventory, inactive-root, manifest and decision-removal publication boundary in standalone and catalog creation and every table position during feature activation.
Restart retains the exact recorded IDs, admits only complete claimed ownership and exposes no table lease during incomplete activation.
Wrong later participants, duplicate epochs, changed scopes, missing roots and conflicting decisions refuse before any earlier manifest, root, index or chunk is changed.
Tests cover both coordinate dimensions, read-only isolation, explicit borrowed/bootstrap locks, fresh shared-writer initialization, default masks, CREATE, MIGRATE, option/schema/lineage rewrites and no-op activation.
An interrupted tag-1 non-group activation recovers on writer restart and remains inspectable without repair by the verifier; read-only startup retains its existing pending-decision refusal.
INACTIVE backup/restore tests retain data, producing identities and epochs while rewriting the complete scope; missing root inventory entries and an activation between the two capture observations refuse.
Sparse and checksum-valid oversized decisions fail before body allocation, and native reparse/generation substitution preserves unrelated bytes.
Isolated controls remove owner-root-before-manifest sync, recorded-ID reuse, context injection, all-participant preflight and upper identity checks and must fail the corresponding exact-byte or admission assertions.
