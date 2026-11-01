# Checked commit group records

This layout implements the pure record admission required by [#136](https://github.com/chunkdb/chunkdb/issues/136) and the [commit group contract](COMMIT_GROUPS.md).
It publishes no files and grants no recovery, durability or feature activation by itself.
The owner admits the `commit_groups` feature and complete directory, table and WAL contexts before acting on these records.
All integers are little-endian, signed coordinates use their two's-complement bits, and identities contain 16 bytes.
Every reserved byte is zero and every codec requires the exact complete extent.
Unsupported tags and record kinds are refused without a legacy interpretation.

## Record envelope

Decisions, receipts and seals use this 64-byte header followed by their payload and a four-byte CRC32 of the complete header and payload.
The header CRC32 covers bytes 0 through 59.
The outer SHA-256 bindings always cover every raw byte, including embedded checksums.

| Offset | Bytes | Field |
| --- | --- | --- |
| 0 | 4 | `CKGR` |
| 4 | 2 | Tag 1 |
| 6 | 1 | Kind: decision 1, durability receipt 2, seal 3 |
| 7 | 1 | Zero |
| 8 | 8 | Complete record bytes, including both checksums |
| 16 | 16 | Nonzero data-directory identity |
| 32 | 16 | Nonzero group identity |
| 48 | 8 | Payload bytes, exactly complete bytes minus 68 |
| 56 | 4 | Zero |
| 60 | 4 | Header CRC32 |

Header inspection validates its checksum, identity, kind and checked extent before the caller allocates a record buffer.
Complete parsing additionally checks the final checksum and every field before allocating owned member, result or string storage.
It never classifies a malformed record as an interrupted append; only the journal owner applies the certified-cut recovery rule.

## Decision payload

The fixed decision prefix is 88 bytes.
Its fields are mode u8, presence flags u8, six zero bytes, visible transaction u64, creation time in milliseconds u64, member count u64, result count u64, principal length u32, ID length u32, tag length u32, zero u32 and the 32-byte request fingerprint.
Mode is relaxed 0 or synced 1; the owner maps both synced table modes and every transaction to synced.
Presence flag bit 0 denotes an ID and bit 1 denotes a tag, so present-empty byte strings differ from absence.
An absent ID has zero principal and ID lengths and a zero fingerprint; a present ID is at most 64 bytes and a present tag is at most 256 bytes.
Principal bytes are the owner's stable authentication scope, including its distinguished anonymous scope when authentication is disabled.
The variable prefix stores principal bytes, ID bytes and tag bytes in that order, without terminators or padding.
The member array follows, then the result array, with no trailing bytes.
Counts are u64 and are admitted against the checked complete extent rather than current session chunk-count settings.

Each member is 248 bytes in the following order.

| Bytes | Field |
| --- | --- |
| 16 | Nonzero table epoch |
| 1 | Dimensions 2 or 3 |
| 1 | Changed 1 or observed 2 |
| 6 | Zero |
| 24 | Signed X, Y and Z |
| 8 | Producing revision |
| 8 | Producing time in milliseconds |
| 8 | Producing schema revision |
| 8 | Original pre-group WAL bytes |
| 8 | Exact frame offset |
| 8 | Exact frame bytes |
| 32 | SHA-256 of the exact complete frame |
| 8 | Member ordinal, equal to its zero-based array index |
| 72 | Durable rollback-base descriptor |
| 40 | Certified pre-group WAL end and raw-prefix SHA-256 |

Members are unique and ordered by table epoch bytes, then signed X, Y and Z, and one epoch has one dimension count.
Two-dimensional members require Z zero.
A changed member has nonzero producing and schema revisions and nonzero frame bytes; frame offset plus length cannot overflow.
A zero original WAL end permits a newly created WAL whose first frame starts at the current dimensional WAL header size, 48 plus 8 times dimensions.
A nonzero original WAL end is at least that header size and equals the frame offset.
This end is not certified durable; the owner admits an earlier durable anchor and may lose a checked volatile suffix according to the parent recovery contract.
An observed member has zero original WAL end, offset, length and digest because it contributes no new WAL frame.
An observed revision zero also requires zero producing time and schema revision; otherwise its schema revision is nonzero.
Only changed members have a `WalGroupRef`, whose member count includes both changed and observed members and whose ordinal selects this array.
The caller verifies the admitted frame's table, coordinates, producing identity, schema and exact group reference before publication or recovery.

The rollback-base descriptor contains kind u8, seven zero bytes, producing revision u64, producing time u64, producing schema revision u64, raw image bytes u64 and raw image SHA-256.
Kind is never-written 0, image 1 or retained-empty 2, and the containing member binds its table epoch, dimensions and coordinates.
Never-written requires every remaining field zero.
Image and retained-empty require nonzero producing and schema revisions, with the base producing revision below the changed member's producing revision.
Image has nonzero raw length and an exact digest; retained-empty has zero image length and digest.
Observed members carry the unused all-zero never-written descriptor.
The owner validates the actual checked image or retained record before using this descriptor as a rollback anchor and refuses loss below any certified durable prefix.
The optional WAL certificate stores end u64 and digest bytes; end zero with a zero digest means absent.
A present certificate starts at least at the dimensional WAL header size, does not exceed the original WAL end and binds every raw byte up to that end.
Observed members require an absent certificate.
Absence makes no durability claim about volatile pre-group WAL bytes above the admitted rollback anchor.

Each result starts with table epoch bytes, result revision u64, immutable table-name length u32 and zero u32, followed by the name bytes.
Names are nonempty result labels from commit-time admission and are never filesystem instructions or resolved through a replacement catalog entry.
Results are unique and ordered by table epoch bytes, and every member has a result with a revision at least its producing revision.
Additional result-only tables and an empty member set can represent no-op outcomes without fabricated frame references.
A cross-table result set or a present ID requires a nonzero visible transaction; all other decisions require transaction zero.

## Durability receipt and seal

A receipt payload is exactly the 32-byte SHA-256 digest of its original checked decision record.
Its envelope has the same directory and group identities as that decision.
This binds one exact decision and does not certify unrelated earlier relaxed decisions sharing a journal prefix.

A seal payload is original decision length u64, its SHA-256 digest, and the exact original checked decision bytes.
The nested decision must pass complete admission and agree with the outer directory and group identities.
This immutable capsule preserves original member identities, result labels, transaction, request binding and tag after the live journal record retires.
Its retained request bytes do not authorize reconstruction of the ID lookup; only the retained-ID inventory and admitted newer decisions do that.

## Journal header and certified inventory

A journal header is 64 bytes: `CKGJ`, tag u16 1, zero u16, directory identity, journal identity, journal generation u64, twelve zero bytes and CRC32 of its first 60 bytes.
Both identities and the generation are nonzero.
Record bytes begin at offset 64 and retain their original encoding across journal generations.

The certified inventory is 128 bytes: `CKGC`, tag u16 1, zero u16, directory identity, journal identity, journal generation u64, certified byte cut u64, last record group identity, last record kind u8, seven zero bytes, complete certified-prefix SHA-256, inventory generation u64, zero u32 and CRC32 of its first 124 bytes.
Its identities and generations are nonzero and its cut is at least 64.
Cut 64 requires a zero last group identity and kind zero; a larger cut requires a nonzero last group and an admitted record kind.
Admission compares the measured prefix, raw digest and exact last record boundary with this inventory and refuses a shorter file or any changed certified byte.
Bytes beyond the cut confer no durability and do not alter its digest.
Journal-byte certification and a particular group's member-durability receipt remain distinct.
The pure API accepts already measured prefix facts so a filesystem caller need not materialize the complete journal.

## Retained-ID inventory

The retained-ID inventory has a 160-byte checked header, variable entries and a final four-byte CRC32 of the complete header and entries.
Its header fields are `CKID`, tag u16 1, zero u16, complete bytes u64, directory identity, journal identity, journal generation u64, inventory generation u64, replay cursor u64, entry count u64, last record group identity, last record kind u8, seven zero bytes, replay-prefix SHA-256, payload bytes u64, twelve zero bytes and header CRC32 of its first 156 bytes.
Complete bytes equal 164 plus payload bytes, and cursor, identity and last-record rules match the certified inventory.
The cursor can name a shorter admitted journal prefix but must be a complete record boundary within a certified prefix.
The owner pins its named journal generation while admitting this cursor and any newer decisions.

Each entry starts with complete entry bytes u64, group identity, decision SHA-256, request fingerprint, creation time u64, principal length u32, ID length u32 and zero u64, then the principal and ID bytes.
Its fixed prefix is 112 bytes, its ID is at most 64 bytes, and present-empty IDs remain valid keys.
Entries are unique and ordered by unsigned principal bytes and then unsigned ID bytes.
The owner additionally binds each entry to the exact admitted decision or seal, matching directory, group, principal, ID, fingerprint and creation time.
Only admitted live entries and newer decision incarnations reconstruct lookup state; neither an expired seal nor a rolled-back newer incarnation restores an older binding.
The owner enforces its configured retention and byte budgets before publishing this snapshot, separately charging in-flight reservations.

## Validation boundaries

Pure tests cover exact golden bytes, both dimensions, changed and observed members, empty ID and tag presence, result-only outcomes, all truncation cuts, overflowed counts, reserved bytes, checksums, context mismatches and noncanonical ordering.
Bindings are tested with distinct valid records of equal extent so checksum recomputation cannot hide a context or SHA failure.
Journal and ID-inventory tests distinguish certified byte preservation from member durability and reject a cursor between records.
Filesystem publication, complete group recovery, seal reference cuts, retained-ID retirement and cancellation remain coordinator responsibilities under the parent contract.
