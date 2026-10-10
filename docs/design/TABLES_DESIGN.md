# Table catalog in 2.0

The [CQL reference](../CQL.md) defines commands and the [storage reference](../STORAGE_FORMAT.md) defines directory publication.

## Catalog and identity

One data-directory manifest identifies the directory; each named table has its own manifest, store identity, geometry, schema, clocks and artifacts.
Table names and directory entries are validated before opening.
The catalog leases a table while a statement uses it, preventing concurrent drop or reopen.
Cache and WAL-stream limits are shared across tables; persistence and revision order remain per table.

## Publication and recovery

CREATE TABLE builds a complete staged directory and publishes it with exclusive rename and parent-directory syncs.
DROP TABLE raises the version floor, drains statements and moves the table into the dropped directory before deletion.
Writer startup resolves pending migrations before interrupted table cleanup and table opening.
It creates default only when the initialized catalog has no table.
Read-only processes open the existing catalog without cleanup and must reopen to discover new tables.

## Schema admission

Ordinary ALTER/DROP drain exclusive table access before acquiring catalog serialization, sharing that admission order with migrations.
Named migration preparation releases catalog serialization while draining table work, then rechecks health and identity after admission.
The metadata gate serializes named migrations against backup metadata capture; per-table exclusive admission serializes ordinary DDL against pinning that table.
A failed durable decision fences new catalog work until writer recovery.
User metadata locks are acquired only after table admission and released before unrelated waits.
See [backup design](BACKUP_DESIGN.md) and [durability](../DURABILITY_CONTRACT.md).
