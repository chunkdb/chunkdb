# Slot partitions and consumer groups

A slot has one consumer at a time (#156).
This note lets several consumers share one slot: the slot is split into a fixed number of partitions by chunk key, and each partition has its own acknowledged position.

## Rule

Every change of a chunk goes to the watch that holds the chunk's partition, in revision order.
Work is handed out by partition, never change by change: a partition is held by at most one watch at a time.
A partition's position is the revision through which its consumer acknowledged every change of the partition's chunks.

## Partitions

`CREATE SLOT 's' ON t ... PARTITIONS n` fixes n, 1 to 256, at creation; without PARTITIONS a slot has one partition.
The count never changes: to partition an existing consumer differently, create another slot.
A chunk's partition is `SplitMix64` of its coordinates modulo n (exact steps in STORAGE_FORMAT.md), the same in every build and every process.
One change can touch chunks of several partitions; each watch receives the part of the change for its partitions, with the change's revision, time, user, tag and txn.
A change that keeps no block, chunk change or chunk form for a watch is not sent to it, as for an AREA filter.
Schema, progress and resync pushes go to every watch of the slot.

## Watches

`WATCH t SLOT 's' PARTITION p[, p ...]` holds those partitions; a watch without PARTITION holds every partition (with box 2, the slot's group share below, which is every partition while it is the only member).
A watch whose partitions overlap another watch's gets SLOT_BUSY; TAKEOVER replaces the one watch holding them, after publishing its accepted ACK, and refuses when they are held by several watches.
The watch starts at the lowest position of its partitions; for each partition it skips the changes at or below that partition's position.
`ACK r` raises every partition of the watch to at least r: the watch was sent every change of its partitions through r.
SHOW SLOTS `acked` is the lowest partition position, which history retention and `lag_revisions` use; `partitions` lists each partition's `acked` and `attached`.
A restore or SWAP TABLES marks every partition to resync; a watch starts with resync when any of its partitions has the mark, and an ACK past a partition's position clears that partition's mark.

## Groups

This section is #156 box 2 and is not built yet: until then a watch without PARTITION holds every partition, and a second one gets SLOT_BUSY.
The partitions that no PARTITION watch holds are the group's; watches without PARTITION on the slot are its members.
A member that joins takes partitions from the members holding the most, and the partitions of a member that leaves go to the members holding the fewest, so each member holds `floor(n/m)` or `ceil(n/m)` of the group's partitions and as few as possible move.
A member whose partitions change writes its accepted ACKs, receives `[rebalance, epoch, revision]`, and its connection returns to statements; its next WATCH replies with its new `partitions`.
A joining WATCH waits until the members that give it partitions have written their ACKs and ended, as a TAKEOVER waits.
A member leaves when its watch ends: UNWATCH, a closed connection (TCP keepalive finds a dead peer), or its `ACK TIMEOUT`, which ends a member that holds changes it does not acknowledge in time.
The group's assignment lives in memory: after a restart the members join again.

## Format

`chunkdb.slots` keeps format tag 4; header flag `0x2` marks the partition format, and then every slot record ends with its partition count `u16`, followed, for a count of 2 or more, by each partition's position `u64` and flags `u8` (`0x1` resync).
The slot's written revision is the lowest partition position and its resync flag is set when any partition has resync; a record that disagrees is damage.
The flag stays set once written, like the filter format flag.
The table feature `slot_partitions` (incompat bit 8, `0x100`) owns the partition format: CREATE SLOT with PARTITIONS 2 or more requires it, and a table whose records carry the flag without the feature is refused.

## Upgrade

Existing slots have one partition and keep their records byte for byte; nothing converts them.
New directories enable `slot_partitions`; an existing directory runs `ENABLE FEATURE 'slot_partitions'` (after `feature_activation`) before its first partitioned slot.
A build without the feature refuses a table that has it, so it never reads a partitioned record as an ordinary one; rollback restores a backup made before the bit was enabled.
MIGRATE does not take PARTITIONS: a migration's slot request has no partition count, and the statement is refused instead of creating an unpartitioned slot.

## Crash points

All partitions of all slots are in one record file, replaced atomically with file and directory sync: a crash leaves the previous or the next record, never some partitions of one flush.
An ACK that a crash loses was never written: its changes repeat after restart, as for an unpartitioned slot.
CREATE SLOT publishes the slot with every partition at the creation revision in one replace; a crash leaves the slot absent or complete.
A failure after the replace is visible returns `OUTCOME_UNKNOWN stage=slot_records` and fences the table, as before.
Retention removes history only below the lowest partition position, after the record holding it is durable; a crash between the two keeps more history.
A partition without a consumer therefore holds the history of the whole slot, and past the retained byte limit the whole slot is lost, as one slow consumer of an unpartitioned slot loses it.
Backups keep every partition position at or below the cut; a restore sets every partition to the cut with resync.

## Limits

The records hold 1024 slots in 1 MiB and a partition takes 9 bytes, so about 450 slots of 256 partitions fill them; CREATE SLOT checks the whole file and refuses a slot that does not fit with OUT_OF_RANGE.
The catalog feed's slot record carries filters and owner, not the partition count.
