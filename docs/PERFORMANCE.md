# Performance

Recorded measurements describe their hardware, build, workload, and settings.
Use them to understand that workload rather than predict another application's throughput.

## Recorded results

[Measurement records](releases/PERFORMANCE_HISTORY.md) contain absolute throughput, latency, host details, and reproduction commands.
The records distinguish direct storage calls from protocol requests and synced writes from relaxed writes.

## Your workload

Cache size, chunk geometry, column widths, variable values, concurrency, and durability mode affect resource use.
Test realistic coordinate distributions and data sizes when choosing settings.
A sparse workload can exercise more filesystem entries and cold reads than a dense one.

See [Server flags](SERVER_FLAGS.md), [Durability](DURABILITY_CONTRACT.md), and [Limitations](KNOWN_LIMITATIONS.md).
