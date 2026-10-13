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

## Protocol workload options

`chunkdb_server_bench` keeps its ordinary request mixes when no additional
options are selected. Its grid scenarios fill their regions before measurement.
Throughput timing includes worker creation, connection and HELLO; request latency
starts after the request is sent and ends after its validated reply.

The following options describe enabled protocol work independently of the
ordinary scenarios:

- `--with version|schema|both` adds the selected metadata to GET BLOCK.
  GET AREA accepts schema metadata; GET CHUNK already contains its metadata and
  keeps its ordinary request. `metadata_envelopes` counts the replies actually
  wrapped, rather than treating every GET as a metadata read.
- `--watch default --watch-mode ordinary|progress|filtered|chunks` attaches
  a new ephemeral watch after each scenario's untimed fill. Ordinary watches
  read changes; progress watches exclude the workload's nonnegative positions
  with AREA -1 -1 TO -1 -1; filtered watches select chunk 0,0, column bits and
  NO BEFORE; chunks watches receive whole chunk forms. Other table names remain
  available with ordinary mode. Start replies accept the legacy OK position or
  a map, with validated epoch and revision.
- `--tests takeover --clients 1 --pipeline 1` exercises idle slot ownership
  replacement. Each cycle connects, negotiates HELLO, accepts WATCH TAKEOVER,
  and validates the previous owner's closure. A KILLED terminal is validated
  when delivered; the server may close without that best-effort terminal.
  Slot creation, the initial owner, and final UNWATCH/DROP are outside the
  cycle timer. There are no mutations or ACKs in this case; it measures idle
  ownership, not delivery, ACK handoff or timeout work.

JSON adds `enabled_config` only when metadata or a watch is selected. These
options are report-only. Existing request mixes and throughput/latency fields
remain available; additional counters record actual response bytes and fully
parsed change, progress and chunks events. Watch bytes include its UNWATCH
completion, and `watch_duration_s` includes attach-to-drain time. The ordinary
hot timer excludes watch setup and final draining. Progress mode waits outside
that timer for an event covering the completed post-workload frontier, with a
bounded I/O deadline; quiet ordinary watches can remain silent until UNWATCH.

Takeover reports actual HELLO/WATCH requests (and AUTH when logging in) per
second and ownership
cycles per second, with zero handled blocks. Its latency percentiles describe
whole cycles. Setup and cleanup commands are excluded from those counts.
Compare enabled configurations using the same producer statements and report
absolute requests/s, blocks/s and p50/p99 with the configuration and run count.
