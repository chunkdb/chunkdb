# WAL durability pins

This component supports exact-generation settlement in [#136](https://github.com/chunkdb/chunkdb/issues/136) and the [group recovery owner](COMMIT_GROUP_RECOVERY.md).
It adds `WalAppendStream::FlushAndPin()` and a movable, noncopyable `WalDurabilityPin` without changing stream fields, ordinary opens, ordinary writes, ordinary flushes or stream counters.
The caller owns exclusive access to the stream during pin creation and remains responsible for staged mutation vectors, WAL framing and parent-directory durability.
It must already admit the original handle's owning WAL context and namespace, and reserve and account for required pins before mutation.

The factory first refuses a closed or failed stream without clearing its existing error.
It flushes the original POSIX `FILE` buffer before acquiring a native generation, and reports that stream's first flush error if the flush fails.
Windows writes already reach the native handle directly, so this step does not introduce a durability sync.
On POSIX, the factory checks the original native object and uses `F_DUPFD_CLOEXEC` to retain its writable file description without changing the shared position.
On Windows, it uses `ReOpenFile` on the original native object with `GENERIC_WRITE | FILE_READ_ATTRIBUTES`, read/write/delete sharing and `FILE_FLAG_OPEN_REPARSE_POINT`, then refuses a non-disk, directory or reparse object.
This obtains documented sync rights without changing ordinary append-only handle rights or resolving a canonical pathname.
The pin admits the exact native regular object, but does not retroactively reject a leaf alias followed by the original Windows open.
The POSIX pin likewise requires a regular file after the original buffer has been flushed.
No pin contains a pathname or performs a pathname reopen, seek, read or write.

The pin owns only its newly acquired native descriptor or handle and requires no heap allocation.
Construction transfers ownership without throwing after native acquisition, and move assignment releases its previous native owner before adopting the new one.
Acquisition failures release only new resources and preserve the original stream's existing error and ownership.
A duplicate or reopen resource failure does not poison a healthy writer, while a failed original buffer flush remains a stream failure.
Destroying a pin releases its native owner without syncing or modifying the stream.

`Sync()` invokes `fsync` or `FlushFileBuffers` on the owned generation and propagates the native failure.
On macOS it preserves the existing file durability contract by first requesting `F_FULLFSYNC`, falling back to `fsync` only for the same unsupported-capability errors accepted by ordinary file synchronization.
An empty or moved-from pin refuses synchronization.
The pin remains valid after the original stream closes, its name is renamed or replaced, or a directory occupies its former pathname.
It does not certify bytes still in another user-space buffer, future writes, a frame boundary or any parent name.

Tests flush small buffered writes through the factory, append after synchronization without changing writer position, and synchronize after rename, stream destruction and directory replacement at the original path.
They cover move ownership, closed and previously failed streams, scoped descriptor exhaustion, and Linux `/dev/full` buffer-flush failure where available.
The existing macOS full-file-sync failure hook verifies that a real sync failure is reported without clearing or closing the stream.
A pathname-based synchronization mutation must fail the renamed-generation test, and omitting the original buffer flush must fail the visibility or `/dev/full` test.
Windows tests must execute the real sync-capable native path rather than infer success from handle duplication.
