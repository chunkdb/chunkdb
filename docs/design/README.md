# Design notes

- [Backup](BACKUP_DESIGN.md): pinned cuts, metadata capture and guarded publication.
- [Change feed](CHANGE_FEED_DESIGN.md): producer ordering, buffers and durable archives.
- [Columns](COLUMNS_DESIGN.md): packed state, positional block addressing and schema translation.
- [Concurrency](CONCURRENCY.md): lock admission, ownership and fail-closed paths.
- [Connections](CONNECTIONS.md): socket ownership, sessions, cancellation and admission limits.
- [Expressions](EXPRESSIONS_DESIGN.md): conditions and computed assignments, typing and evaluation.
- [Extensible format](EXTENSIBLE_FORMAT_DESIGN.md): feature flags and typed extensions.
- [Secondary indexes](INDEXES.md): keys, runs, write maintenance, recovery and GET BLOCKS.
- [Messages](MESSAGES.md): application messages in a commit, their WAL TLV and change feed representation.
- [Replication apply](REPLICATION_APPLY.md): follower tables, APPLY, APPLY GROUP and APPLY CATALOG.
- [Revisions and frames](FORMAT_V2_DESIGN.md): persistent equality tokens and atomic mutations.
- [Request I/O](REQUEST_IO.md): polling, reads, dispatch and replies on the request path, and how changes to them are measured.
- [Table restore](RESTORE_TABLE.md): online RESTORE TABLE, SWAP TABLES, RENAME TO and feed resync.
- [Swap guard](SWAP_GUARD.md): SWAP TABLES by guarded renames where no atomic exchange exists.
- [Tables](TABLES_DESIGN.md): catalog leases and directory publication.
- [Transactions](TRANSACTIONS_DESIGN.md): snapshot history and durable decisions.
- [Users](USERS_DESIGN.md): SCRAM, rights and metadata recovery.
- [WAL archive](WAL_ARCHIVE.md): sealed segments, seal records, retention, point-in-time restore and BACKUP STREAM.

- [Persistence design](PERSISTENCE.md) records checkpoint, rollback, publication, and platform sync details.
