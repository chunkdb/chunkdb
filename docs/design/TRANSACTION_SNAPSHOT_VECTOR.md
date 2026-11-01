# Transaction snapshot vectors

This contract specifies session ownership for [#136](https://github.com/chunkdb/chunkdb/issues/136).
The publication and recovery rules remain in [Commit groups](COMMIT_GROUPS.md).

BEGIN creates a transaction without taking table snapshots.
Its first executable statement, including PING or DESCRIBE, captures the directory snapshot vector before execution.
COMMIT and ROLLBACK do not create a vector when no statement has run.
A separate captured flag distinguishes a captured empty directory from a transaction that has not started a statement.
The first statement does not defer a table's cut until its first data access.
Capture failure publishes no partial vector, and cancellation and the transaction duration are checked after the final snapshot registration.

Each vector entry owns its original table object, immutable table name and epoch, and registered snapshot.
It also owns the read set, promoted whole-chunk copies and sparse block observations and deltas for that table.
Entries remain ordered by epoch for commit admission; a name lookup resolves only entries in the original vector.
No mutable ambient current-table field determines the owner of a read set or private write.
An absent name or a replacement epoch produces a table-changed conflict rather than admitting a newly created table.
Only entries actually touched by a statement need to retain their table identity at commit.
Changing an untouched table does not invalidate an otherwise valid transaction.

Snapshot capture owns no table lease on entry.
It excludes metadata and multi-table lease acquisition before pausing ordinary table admission in epoch order.
All existing mutations drain before snapshots are registered, and history registration completes before admission resumes.
The pause keeps each store, maintenance owner and attached feed alive.
The directory visibility gate is never held while waiting for table leases or chunk locks.
The vector owns snapshot history registrations rather than long-lived table leases, so an open idle transaction does not prevent DDL.

Every table statement resolves its original vector entry before obtaining its command lease.
The acquired store must own that exact snapshot, including after an ALTER that retains the table epoch but replaces the store.
Rights are checked for the statement and again for each written table at commit.
Commit takes exclusive feature metadata admission before checking identities and acquiring all touched table leases in epoch order.
Catalog DDL owns shared feature admission, so existing DDL completes before the group obtains its first lease and another DDL cannot interleave between its lease acquisitions.
The group releases feature admission before loading chunks, taking chunk locks or performing group I/O.
It never waits for table admission while holding the catalog operations mutex, because an already admitted exclusive table operation can need that mutex to finish.
The admission gate provides a condition-based deadline and cancellation wait, rather than polling a mutex or adding a sleep.
The existing single-table lease path remains unchanged; group admission uses a separate deadline-aware lease overload while a table is temporarily busy.
An empty cohort, missing original owner, duplicate epoch or expired admission is refused before returning any lease.
An executing group already owns its complete lease cohort and cannot re-enter table-cohort admission or snapshot capture.
It validates every touched identity before preparing a group and takes chunk locks in epoch and coordinate order.
An identity or read conflict cannot commit a subset of tables.

The configured read and written chunk bounds count distinct pairs of table epoch and coordinate across the whole transaction.
Equal coordinates in different tables count separately, and a lowered per-table counter cannot bypass the directory bound.
Sparse block observations and private writes share the existing per-transaction and global byte reservations.
A statement prepares its new bookkeeping and byte reservation privately and publishes them together after all admission checks pass.
Failure leaves all entries and byte counters unchanged; abort, rollback and connection close release the complete vector exactly once.

Regression fixtures interleave the first PING with writes to a table first read later, table creation after an empty cut, replacement of touched and untouched tables, and equal coordinates in different tables.
They also cover failed final registration, aggregate count and byte refusal, rights revocation, and complete snapshot release on every transaction exit.
