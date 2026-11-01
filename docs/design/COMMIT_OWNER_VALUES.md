# Checked commit owner values

These prerequisite formats implement typed ownership facts for [#136](https://github.com/chunkdb/chunkdb/issues/136) and the [selected owner closure](COMMIT_GROUP_RECOVERY.md).
They grant no native admission, recovery, serving, counter allocation or publication authority.
The native closure must admit every referenced source and cross-check owner references in both directions before the reference cut can govern reclamation.
ACTIVE and PENDING runtime admission remain refused until their complete recovery paths exist.

## Compatibility and common bounds

The existing 512-byte root, page layout, key encodings and 204-byte membership capsule remain unchanged.
The explicit additions are object kind 11 for owner edges, tree kind 4 for owner edges and tree kind 5 for nonzero observations.
Unknown discriminants refuse without a legacy interpretation.
Coverage, group, observation and edge values use the existing checked 64-byte CKGO header and final CRC32.
Each payload has a checked tag-specific extent, exact scope and positive creating generation no newer than its selecting owner generation.
Every unused union field and reserved byte is zero.
Each value is at most 64 KiB; native records referenced inside values retain their independent native bounds.

A set commitment contains a page reference, measured key count, measured owned bytes and complete-set SHA-256.
Its tree kind determines its canonical fixed-width key and typed leaf value.
The set digest hashes a domain-separated tree-kind prefix, every ordered key and exact value reference, then the measured count.
Empty sets still select a checked empty page and the canonical empty-set digest.
Member, observation and owner-edge sets each have a format-wide 256 MiB owned-metadata ceiling, independently of current session limits.
Owned bytes count the tree pages and directly referenced capsules, not native bodies referenced inside capsules.
Counts, extents and sums cannot wrap, and a declared count never determines an allocation before physical bounds pass.

## Fixed native source locators

A journal-record locator contains its checked directory/journal/generation context, record offset, complete record length and complete raw SHA-256.
A seal locator contains its complete raw length and SHA-256 and has no journal context or record offset.
The discriminant determines the exact permitted fields and native record kind.
Decisions, seals and receipts retain their existing format-wide bounds and are streamed during native admission.
An immutable image or WAL-prefix locator similarly binds its exact native kind, complete owned extent and raw digest; the containing value supplies its original table epoch, dimensions and coordinate.
Names are derived by the native owner from these typed identities; no locator contains a filesystem path.
The raw SHA includes embedded checksums and does not replace native header, schema, content or source-generation admission.

## Coverage values

A coverage value contains its complete 41-byte coverage key, selected state kind, current producing revision/time/schema, typed artifact or retained proof, covered-member set commitment and optional producer membership fact.
Image and retained-empty states have nonzero current revision and schema and exact selected artifact identity.
Never-written state has no artifact, producer or producing identity; only zero observed memberships can bind such a selected state.
A producer is always a changed member, has the same coverage key and exactly matches the current producing identity.
An observed member cannot become a producer merely because its value is equal.
Covered historical membership retains its original producing identity rather than substituting the selected artifact's later revision.

A retained proof owns the exact 52-byte or 60-byte CKIX deleted record, dimensional CKIX header, checked CKII header and selected-entry context, original inventory length/SHA and committed record position.
Inactive dimensional padding is zero and all copied native checksums, epochs, coordinates, whole-record boundaries and producing fields agree.
Certificate creation first admits the complete original inventory/log context and actual selected deleted state.
Later owned-copy validation checks this self-contained checked certificate under its selecting owner and original schema context; it does not require an unavailable replaced CKII file.
The original inventory digest identifies the admitted creation context rather than claiming that a copied entry alone is a new complete-inventory admission.

Forgotten-empty state exposes no current producing identity or producer.
It owns the historical retained-empty proof and a checked deletion-floor context covering that historical revision.
The native closure additionally admits the table's nonregressing deletion floor; a later revision or floor alone never manufactures the historical proof.
Forgotten coverage cannot recreate a visible tombstone or coordinate entry.

## Retained group values and observations

A group value contains its exact group identity, original decision SHA, fixed tagged decision-or-seal locator and typed durability evidence.
It binds complete canonical member, owner-edge and required-observation set commitments.
The member set uses the existing 24-byte group/ordinal keys and unchanged membership capsules.
The observations tree uses those same key bytes with observation values, without repurposing membership payloads.
Each nonzero observed member requires exactly one observation capsule, zero observed members require none, and changed members require none.

An observation capsule contains its exact original observed membership fact and a tagged image, retained-empty or complete owned WAL-prefix proof.
Its revision and schema are nonzero and its group, decision SHA, ordinal, count, coverage key and producing fields bind the original decision.
A WAL-prefix proof additionally owns its exact durable image/retained-empty/never-written base and complete prefix digest/boundary.
Native admission checks the resulting producing identity after complete bounded prefix replay; a checksum-valid prefix alone does not establish that identity.
Observed zero identities have no capsule or invented file.

Durability evidence distinguishes an unsettled relaxed decision, an originally synced decision, a specific durability receipt and complete durable member coverage.
An originally synced claim requires that mode in the exact original decision.
A receipt binds its own exact source and original decision SHA.
Complete-coverage durability requires every changed and nonzero observed proof, not a numeric journal cut or a caller-provided success flag.
Pure consistency validation does not certify that any referenced byte is durable.

## Owner-edge keys and values

An owner-edge key has exactly 49 bytes: owner class u8 followed by a 48-byte canonical typed source key.
Classes are distinct even when they retain the same group and coordinate.

| Class | Canonical source key after the class byte | Required checked source |
| --- | --- | --- |
| Current producer, 1 | Coverage key 41, seven zero bytes | Exact coverage value and changed target ordinal |
| Covered member, 2 | Coverage key 41, seven zero bytes | Exact coverage value, its committed covered set and target ordinal |
| Rollback anchor, 3 | Dependent group identity 16, source ordinal u64, 24 zero bytes | Exact dependent decision/member incarnation and bound earlier anchor |
| Feed history, 4 | Selected feed-root SHA 32, source edge ordinal u64, eight zero bytes | Exact checked CKSL epoch/slot context and complete retained archive/group witness |
| Retained ID, 5 | Length-delimited principal/ID key SHA 32, 16 zero bytes | Exact selected CKID generation, entry extent and raw binding |
| Recovery transition, 6 | Plan/provenance SHA 32, participant ordinal u64, eight zero bytes | Exact checked replacement-plan or restore-provenance context |

Each edge binds its complete target group identity and decision SHA, derived key and class-specific typed source incarnation.
Source locators and full target bindings are checked separately from the key hash.
A duplicate key or a colliding key with a different typed source is damage, never an overwrite or merged owner.
Native closure must prove every selected edge has its actual source and every actual source owner appears in the selected commitment.
Feed admission depends on complete checked slot records, retained archive framing and group witnesses, not a slot name or watermark alone.
Transition/provenance admission depends on their complete checked payloads and original/destination mappings; unsupported native sources refuse.
No source reference may form a recursive raw-hash dependency on its selecting root or containing value.
Pure edge parsing does not choose a pending side or certify a transition.

## Directory transaction counter

The counter is a separate fixed 96-byte CKTC/tag-1 native record, not a table VersionClock record or CKGO page.
Its generation is independent of owner-root generations because counter reservation can precede later root publication.

| Offset | Bytes | Field |
| --- | --- | --- |
| 0 | 4 | CKTC |
| 4 | 2 | Tag 1 |
| 6 | 1 | Owner role |
| 7 | 1 | Zero |
| 8 | 8 | Exact complete extent, 96 |
| 16 | 16 | Directory identity |
| 32 | 16 | Stable group-owner identity |
| 48 | 16 | Counter-owner identity |
| 64 | 8 | Positive counter-record generation |
| 72 | 8 | Exclusive reserved ceiling |
| 80 | 12 | Zero |
| 92 | 4 | CRC32 of bytes 0 through 91 |

All identities are nonzero, the role agrees with scope, and the exclusive ceiling starts at one.
Only positive values strictly below an admitted durable ceiling can have been exposed from its reserved range.
A successor preserves exact scope and counter owner, advances generation without wrapping and strictly increases the ceiling.
UINT64_MAX is the last representable exclusive ceiling, so exposing that value itself is impossible and further reservation refuses.
Restart discards unused reserved values instead of reusing a prior process's range.
The selecting owner binds this exact counter owner and a minimum ceiling; native admission also checks every retained exposed value against the actual reservation.
A merely present unselected counter record grants no range or publication authority.
This slice defines checked bytes and consistency validators only; creation, durable reservation and allocation remain native-owner responsibilities.

## Remaining native admission and tests

The native closure still needs pinned complete source reads, streamed journal/seal/ID admission, complete tree traversal and both-direction owner joins.
Its original table/schema inventory, full WAL discovery, rollback planning, pending replacement and restore provenance remain separate prerequisites.
No constructor ticket, writer permit or recovery capability can be constructed from these codec results.

Tests cover golden layouts, both dimensions and negative coordinates, every truncation cut, appended bytes, reserved fields, unknown kinds, checksums and wrong scope/generation/key bindings.
They distinguish observed from changed producers, zero from nonzero observations, retained from forgotten state, selected from merely present counters and every owner class.
Valid different same-length source records exercise raw digest and exact incarnation checks independently of checksum refusal.
Sparse oversized headers and allocation guards verify admission before complete-body allocation.
Every targeted omitted production safeguard must compile and fail its regression assertion, followed by an independently rebuilt restored positive.
