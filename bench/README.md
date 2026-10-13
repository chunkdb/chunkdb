# Protocol benchmark

`chunkdb_server_bench --help` lists workload and connection options; the additions below produce report-only data.
Default workloads and JSON fields stay unchanged when these additions are unused.

- `--with none|version|schema|both` defaults to `none`; GET BLOCK gets selected metadata, GET AREA gets schema metadata with `schema` or `both`, and GET CHUNK stays unchanged.
- `--watch <table>` starts an ephemeral subscription after each untimed grid fill and consumes pushes through UNWATCH completion.
- `--watch-mode ordinary|progress|filtered|chunks` defaults to `ordinary`; other modes require `--watch default`.
- `ordinary` delivers mutations, `progress` excludes workload positions with AREA -1 -1 TO -1 -1, `filtered` selects chunk 0,0 and column bits with NO BEFORE, and `chunks` delivers whole chunk forms.
- `--tests takeover --clients 1 --pipeline 1` measures idle connect→HELLO→WATCH TAKEOVER→old-owner closure cycles and requires no watch or read metadata.
- Takeover performs no mutations or ACKs; initial slot creation/ownership and final UNWATCH/DROP are excluded from its timing and request counts.

Ordinary throughput includes worker creation, connection and HELLO, but excludes fill, watch setup and final draining.
Ordinary latency starts after sending a request and ends after validating its reply; takeover latency covers a whole ownership cycle.
`watch_duration_s` starts after the accepted WATCH start and ends after UNWATCH completion, excluding setup; `watch_bytes` excludes negotiation/start replies and includes UNWATCH completion.

Report-only results add `response_bytes`, `metadata_envelopes`, `watch_bytes`, `watch_changes`, `watch_progress`, `watch_chunks`, `watch_other` and `watch_duration_s`.
`metadata_envelopes` counts wrapped reads, while watch event counters count fully consumed push frames.
Metadata or watches also add `enabled_config` with `report_only`, `with`, `watch` and `watch_mode`.
Takeover adds `report_only`, `ownership_cycles`, `ownership_cycles_s`, `latency_unit="ownership_cycle"` and `acks_sent=0`; `throughput_req_s` counts HELLO/WATCH requests plus AUTH when logging in, and `throughput_blocks_s` is zero.
Compare matching workload settings and retain absolute requests/s, blocks/s and p50/p99 with the run count and configuration.

See [artifact formats](artifacts/README.md) for stored results.
