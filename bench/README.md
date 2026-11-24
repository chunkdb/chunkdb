# Protocol benchmark

`chunkdb_server_bench --help` lists workload and connection options; the additions below produce report-only data.
Default workloads and JSON fields stay unchanged when these additions are unused.

- `--with none|version|schema|both` defaults to `none`; GET BLOCK gets selected metadata, GET AREA gets schema metadata with `schema` or `both`, and GET CHUNK stays unchanged.
- `--watch <table>[:N][:slots]` starts N subscriptions (default 1) after each untimed grid fill and consumes pushes through UNWATCH completion; with `slots`, each owns a durable slot with the watch mode's filters, acknowledges what it reads, counts its initial schema push in `watch_other`, and drops the slot afterwards.
- `--watch-mode ordinary|progress|filtered|chunks` defaults to `ordinary`; other modes require `--watch default`.
- `ordinary` delivers mutations, `progress` excludes workload positions with AREA -1 -1 TO -1 -1, `filtered` selects chunk 0,0 and column bits with NO BEFORE, and `chunks` delivers whole chunk forms.
- `--watch-delivery strict|measure` defaults to `strict`, where a resync or a drain longer than `--io-timeout-ms` fails the run; `measure` requires `--watch` and is meant for measuring many subscribers.
- `--slot-create-timeout-ms N` bounds the CREATE SLOT reply in `measure` mode (default 600000); the first slot on a table checkpoints every chunk with WAL data before it replies, outside the timed window.
- `--tests takeover --clients 1 --pipeline 1` measures idle connect→HELLO→WATCH TAKEOVER→old-owner closure cycles and requires no watch or read metadata.
- Takeover performs no mutations or ACKs; initial slot creation/ownership and final UNWATCH/DROP are excluded from its timing and request counts.
- `--failed-logins N` requires `--user` and starts N clients once every measured client has logged in; each logs in as that user with a wrong password, one login after another, until the measured clients finish, and makes at least one attempt.
- The server delays each failure and ban reply (see [protocol](../docs/PROTOCOL.md)), so the measured latencies show whether those held replies slow logged-in clients; the failures ban the bench's address, and the next scenario starts after the ban ends.
- Results add `failed_logins`, the failure and ban replies received, and the report adds `failed_login_clients`; takeover does not take the option.
- `--idle-connections N` opens N more connections before each scenario, logs each in like a measured client, and keeps them silent until the scenario ends; spawn mode then raises the server's idle connection timeout to its maximum, and an external server's `--idle-connection-timeout-ms` must outlast the run.
- The bench holds both ends of each idle connection against a spawned server, so its descriptor limit must exceed twice N plus the measured clients; the report adds `idle_connections`, and takeover does not take the option.

Ordinary throughput includes worker creation, connection and HELLO, but excludes fill, watch setup and final draining.
Ordinary latency starts after sending a request and ends after validating its reply; takeover latency covers a whole ownership cycle.
`watch_duration_s` starts after the accepted WATCH start and ends after UNWATCH completion, excluding setup; `watch_bytes` excludes negotiation/start replies and includes UNWATCH completion.

Report-only results add `response_bytes`, `metadata_envelopes`, `watch_bytes`, `watch_changes`, `watch_progress`, `watch_chunks`, `watch_other` and `watch_duration_s`.
`metadata_envelopes` counts wrapped reads, while watch event counters count fully consumed push frames; watch counters and `watch_bytes` add up all watches, and `watch_duration_s` is the longest watch's.
Metadata or watches also add `enabled_config` with `report_only`, `with`, `watch` and `watch_mode`.
In `measure` mode, `enabled_config` adds `watch_delivery`, and each result adds `watchers`, one entry per watch with `index`, `slot`, `events`, `changes`, `chunks`, `progress`, `other`, `resyncs`, `bytes`, `lag_revisions_at_end`, `catch_up_s` and `slot_create_ms`.
An ordinary watch that falls behind its share of the feed buffer gets a resync and skips the changes before it; `resyncs` counts those, so `changes` can be lower than the writes.
A drain in `measure` mode waits while pushes arrive and fails after `--io-timeout-ms` without one; a slot watch receives changes only up to the durable frontier, which the server advances every `--slot-sync-ms`, so its catch-up includes that wait.
`lag_revisions_at_end` is the write frontier minus the watch position when the last write replied, and `catch_up_s` is the time from then until the watch reached the frontier; revisions can have gaps, so lag counts revisions, not changes.
Takeover adds `report_only`, `ownership_cycles`, `ownership_cycles_s`, `latency_unit="ownership_cycle"` and `acks_sent=0`; `throughput_req_s` counts HELLO/WATCH requests plus AUTH when logging in, and `throughput_blocks_s` is zero.
Compare matching workload settings and retain absolute requests/s, blocks/s and p50/p99 with the run count and configuration.

`--system-stats` adds `system` to each result except takeover, counters of the measured window from just before its first connection to just after its last reply, and `server_close_s` to a spawned run, the time from stopping the server until its store closed.
`process_*` comes from getrusage (not on Windows); on Linux, `threads_*` sums the threads alive at both ends of the window (off-CPU time includes idle waits; `threads_blkio_s` needs `kernel.task_delayacct=1`), and `machine_cpu_*`, `psi_*`, `vm_*`, `mem_*` and `disk_<device>_*` (devices with activity in the window) cover the whole machine.
`engine_*` are the spawned store's checkpoints, evictions, WAL batch flushes and barriers, version clock ceiling writes with their time, and the time writers waited for a ceiling, each waiting writer counted; `requests_over_100ms` counts slow requests and `requests_over_100ms_s` sums their latencies.
`sampler_max_gap_ms` is the longest gap between wake-ups of a thread that sleeps 10 ms, so a stall of the whole process or machine shows there, while a stall inside the engine does not.

See [artifact formats](artifacts/README.md) for stored results.

## Shared maintenance

`chunkdb_maintenance_bench --data-dir NEW_PATH` runs separate 1, 100 and 1,000 table cases with background maintenance enabled.
`--tables N` selects one case, `--requests N` sets its write count, and `--maintenance-workers N` sets the shared pool size.
`--flushes N` sets the number of timed barriers after one write to every table and of timed barriers with nothing to sync.
`--background-maintenance off` measures the default server configuration, where only barriers use the pool.
The defaults are 200,000 writes, 20 flushes and two workers.
Run measurements on the benchmark server through its `measure.sh` wrapper, one `--tables` case per process so memory figures are not shared between cases.
The executable creates only a new data-directory path and leaves its data for inspection.

Each JSON row records the request count, write duration, writes per second and the duration in milliseconds of the first catalog WAL barrier after the write window.
`write_checkpoints` counts the checkpoints finished in the write window, and `write_inline_checkpoints` those not run on a maintenance worker.
It also records p50, p99 and maximum milliseconds of the dirty and idle barriers.
Linux rows also report process threads and resident bytes after the write window, and threads and peak resident bytes after the barriers; other platforms report `null` for those counters.
Table creation, initial population and the initial barrier are outside the measured windows.
The same driver can compile against baseline public headers; `maintenance_workers: 0` identifies the earlier per-table scheduling model.
