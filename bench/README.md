# Protocol benchmark

`chunkdb_server_bench --help` lists workload and connection options; the additions below produce report-only data.
Default workloads and JSON fields stay unchanged when these additions are unused.

- `--with none|version|schema|both` defaults to `none`; GET BLOCK gets selected metadata, GET AREA gets schema metadata with `schema` or `both`, and GET CHUNK stays unchanged.
- `--watch <table>[:N][:slots]` starts N subscriptions (default 1) after each untimed grid fill and consumes pushes through UNWATCH completion; with `slots`, each owns a durable slot with the watch mode's filters, acknowledges what it reads, counts its initial schema push in `watch_other`, and drops the slot afterwards.
- `--watch-mode ordinary|progress|filtered|chunks` defaults to `ordinary`; other modes require `--watch default`.
- `ordinary` delivers mutations, `progress` excludes workload positions with AREA -1 -1 TO -1 -1, `filtered` selects chunk 0,0 and column bits with NO BEFORE, and `chunks` delivers whole chunk forms.
- `--watch-delivery strict|measure` defaults to `strict`, where a resync or a drain longer than `--io-timeout-ms` fails the run; `measure` requires `--watch` and is meant for measuring many subscribers.
- `--slot-create-timeout-ms N` bounds the CREATE SLOT reply in `measure` mode (default 600000); the first slot on a table checkpoints every chunk with WAL data before it replies, outside the timed window.
- `--report-cpu` adds to each result `cpu_s`, the CPU seconds of the bench process from the first client start to the last client reply, with a spawned server inside it; `client_cpu_s`, those of the measured client threads; `watch_cpu_s`, those of the watch readers from WATCH to UNWATCH; and `context_switches` of the process in the timed window, 0 on Windows.
- `cpu_s` minus `client_cpu_s` and `watch_cpu_s` is about what a spawned server used, so runs with 0, 1, 10 and 100 watches show whether a slower write comes from the server or from the readers sharing the host.
- `--tests takeover --clients 1 --pipeline 1` measures idle connect→HELLO→WATCH TAKEOVER→old-owner closure cycles and requires no watch or read metadata.
- Takeover performs no mutations or ACKs; initial slot creation/ownership and final UNWATCH/DROP are excluded from its timing and request counts.
- `--failed-logins N` requires `--user` and starts N clients once every measured client has logged in; each logs in as that user with a wrong password, one login after another, until the measured clients finish, and makes at least one attempt.
- The server delays each failure and ban reply (see [protocol](../docs/PROTOCOL.md)), so the measured latencies show whether those held replies slow logged-in clients; the failures ban the bench's address, and the next scenario starts after the ban ends.
- Results add `failed_logins`, the failure and ban replies received, and the report adds `failed_login_clients`; takeover does not take the option.

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
