# Revision and mutation framing in 2.0

The [storage reference](../STORAGE_FORMAT.md) specifies the bytes; this page explains the invariants behind revision tokens and WAL frames.

## Persisted revisions

Images record the revision of the state they contain, and every WAL frame records its resulting revision.
Reload and restart adopt those revisions rather than allocating replacement versions.
The checked version clock durably reserves ranges before exposing tokens; DROP raises the directory version floor so recreating a table cannot reuse its tokens.
A valid initialized marker with missing or invalid clock bookkeeping refuses opening.
Simultaneous loss of marker and clock cannot establish whether earlier tokens were exposed.

## One mutation per frame

FRM2 carries the revision, commit time, typed metadata, record count and body size.
The header CRC includes offsets/length metadata and the frame CRC includes all record bytes.
Replay validates the whole frame before applying records; a multi-record SET CHUNK cannot recover only a prefix.
The image uses checked ordered sections and CHKIMAGE; the WAL uses CHKWALOG.
These internal version numbers are independent of the product's 2.0 version.

## Decisions spanning files

Conditional writes persist CKRB before appending and commit with CKRC.
Transactions persist all changed chunks' boundaries in CKTB and commit with CKTC.
Recovery rolls back only uncommitted boundaries and retains committed WAL frames.
The snapshot-generation bracket prevents a read-only process from combining observations across a transition.
See [durability](../DURABILITY_CONTRACT.md) for errors whose outcome is unknown.
