# Commit group coordinator

This note covers the first slice of [#136](https://github.com/chunkdb/chunkdb/issues/136) on top of [artifact I/O](COMMIT_OWNER_ARTIFACT_IO.md) and the [recovery owner](COMMIT_GROUP_RECOVERY.md).
Commit groups stay inactive: creation and `ENABLE FEATURE` install only the generation-one INACTIVE root through the [bootstrap](COMMIT_OWNER_BOOTSTRAP.md), no server or tool path constructs the coordinator, and no path writes a canonical ACTIVE or PENDING root.
Write, replay and checkpoint paths do not call it.

## Ownership

The coordinator owns one owner root: a data directory, or the root of a standalone store.
A read-only coordinator only reads; a writer borrows the exclusive ProcessLock of that root, which must outlive it, and refuses a shared-writer context.
It has no internal mutex: its caller serializes it, and the owner-transition permit arrives with the first ACTIVE mutation.
A failed journal write or sync fences only that journal file object; a later object must re-admit the generation by a scan from disk before it appends, and a coordinator-level fence is still to come.

## Canonical owner

`ReadOwner` returns the canonical root as facts and refuses an INACTIVE root whose generation is not one, because only bootstrap creates INACTIVE and no successor transition yields it.
`InstallInactiveOwner` installs or re-syncs the fixed generation-one INACTIVE root without replacement.
Creation and `ENABLE FEATURE` reach the same fixed-bytes installer of artifact I/O through the [bootstrap](COMMIT_OWNER_BOOTSTRAP.md), before their manifest claims the feature; calling it on an existing directory outside that decision is forbidden, and the coordinator's own installer has no production caller.

## Activation candidate

`StageActivation` requires the exact generation-one INACTIVE canonical root and first re-syncs it and the root directory, because a visible root may come from an installer that failed before its syncs.
It then creates a journal generation with a fresh random nonzero journal identity and generation one, holding only its 64-byte header.
It stages an empty coverage page and an empty groups page of generation two, a generation-one journal inventory and a generation-one empty ID inventory that both certify the 64-byte header cut.
Last it stages a generation-two ACTIVE root object that names them, with that cut as its reference replay and a zero counter owner, because no native counter format exists yet.
Every selected object and its parent names are durable before the root object names them.
The canonical root never changes; the candidate is only a durable private object.
After a crash nothing is resumed: leftovers stay unselected and uncollected, and a new attempt uses a fresh journal identity.

## Handover check

`ValidateActivation` requires the canonical root to be exactly the generation-one INACTIVE root and the candidate's root object to read back as the candidate at generation two.
It checks the INACTIVE-to-ACTIVE successor rule, admits the selection and requires an empty journal and an empty ID inventory at the header cut.
It proves admission, not durability; durability comes from the syncs of staging.

## Selection admission

`AdmitSelection` admits an ACTIVE root without changing files and refuses a PENDING root, because replacement plans are not admitted by this build.
It reads both tree roots and refuses any nonempty tree, because the child, value and plan codecs are missing.
It reads the journal inventory, pins the journal generation it names, reads the ID inventory against that context and refuses any retained ID entry, because entries are not bound to decisions yet.
It scans the pinned generation once against the inventory with the ID cursor and the reference cursor, then checks the selections and the zero counter owner.
Journal records are admitted only as records: member frames, rollback bases and synced-member presence are not checked in this build.

## Verification

`chunkdb_verify --data-dir <path>` checks the canonical INACTIVE root of a catalog or standalone store whose manifest claims `commit_groups` before checking chunk artifacts.
It reports `chunkdb.groups.objects` and `chunkdb.groups.journal`, and any owner name in a catalog table, as the warning `group_owner_unclaimed`, because no INACTIVE owner selects them.
The internal `VerifyCommitGroupOwner` checks an owner for a supplied scope without changing files; nothing reaches it from the command line until an ACTIVE owner can exist.
A missing or invalid catalog root is the error `data_dir_manifest_invalid`.
A standalone store reports `group_owner_missing` for a missing root and `group_owner_invalid` for an unreadable, foreign or non-generation-one INACTIVE root.
An INACTIVE root gets no further checks, because staged objects and journal generations under it are not admitted.
A PENDING root is the warning `group_owner_pending`.
For an ACTIVE root, any refusal of the selection is the error `group_journal_invalid` at the journal leaf once the inventory names it, else at the object directory.
A tail above the cut is the warning `group_journal_tail`, and the info `group_journal` gives the generation, the certified bytes and the uncertified record count.
