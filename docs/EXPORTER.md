# Export metrics to Prometheus

The server serves no HTTP.
`chunkdb_exporter` logs in to a server, reads [SHOW METRICS](reference/SERVER.md) on each scrape and serves the text at `GET /metrics` for Prometheus.
It ships beside `chunkdb_server` in the release archives and the Docker image.

## Set up

Create a user with the MONITOR right, which reads metrics and status and nothing else:

```sh
chunk-cli --uri "$CHUNKDB_URI" --new-password-file monitor.password "CREATE USER monitor PASSWORD"
chmod 600 monitor.password
chunk-cli --uri "$CHUNKDB_URI" "GRANT MONITOR ON * TO monitor"
```

Start the exporter with that user's password in a file:

```sh
chunkdb_exporter --uri chunks://monitor@db.example.com:4242/ --password-file monitor.password
curl -s http://127.0.0.1:14242/metrics | tail -n 1
```

```output
chunkdb_up 1
```

Point Prometheus at it:

```yaml
scrape_configs:
  - job_name: chunkdb
    static_configs:
      - targets: ["127.0.0.1:14242"]
```

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `--uri` | required | `chunk://user@host:port/`, or `chunks://` for TLS. A password in the URI is refused. |
| `--password-file` | required with a user | A file whose first line is the user's password, readable by its owner only (mode 0600); it is read again at each login. |
| `--listen` | `127.0.0.1:14242` | `host:port` of the HTTP listener, with an IPv6 host in brackets such as `[::1]:14242`; port 0 picks a free port, which the `port` field of the INFO line `serving metrics` names. |
| `--allow-remote` | off | Allow `--listen` on an address that is not loopback. |
| `--tls-ca-file` | system certificates | Certificates that verify a `chunks://` server; the certificate must name the URI's host. |
| `--no-channel-binding` | off | Do not bind the login to its TLS connection; needed only when something between the exporter and the server ends TLS. |
| `--timeout-ms` | `5000` | 1–600000 ms for one scrape: connecting, logging in and reading SHOW METRICS. |
| `--log-level` | `info` | `debug`, `info`, `warn` or `error`. |

SIGTERM or SIGINT stops the exporter.

## Scrapes

The exporter keeps one connection between scrapes.
When a kept connection fails, as after the server's `--idle-connection-timeout-ms` or a restart, the same scrape opens a new one.
After a failed connect or login the exporter does not try again for 1 second, doubled after each failure up to 30 seconds, and answers scrapes meanwhile with `chunkdb_up 0`.
Each response appends `chunkdb_up`: 1 with the server's metrics, or 0 alone with status `503` when the server cannot be reached, refuses the login or refuses SHOW METRICS.
The exporter logs the reason when scrapes start failing and when they work again.
Scrapes that arrive while one runs share its result.
SHOW METRICS leaves out the samples of a table that is not serving at that moment, except `chunkdb_table_fenced`: one that an exclusive statement such as ALTER TABLE holds or waits for, one whose change feed cleanup failed until RECOVER TABLE, or one being dropped.
Resolving the URI's host does not observe `--timeout-ms`: a scrape that waits on DNS answers `503` with `chunkdb_up 0` when the timeout passes, and stopping the exporter waits for that lookup.

`HEAD /metrics` returns the headers of `GET /metrics`.
Other paths get `404`, other methods `405` with `Allow: GET, HEAD`, and a malformed request `400`.
A request line and headers must arrive within the timeout and fit in 8192 bytes, or the reply is `408` or `431`.
The listener holds at most 16 connections; a slow client delays no other one.

## Metrics

Metric names, types and label names are fixed for 2.x: a 2.x release may add a metric or a label value, and renames, removes or retypes none.
[SHOW METRICS](reference/SERVER.md#returns) says what the table metrics count.

- By `class` (`auth`, `point_read`, `point_write`, `chunk_read`, `chunk_write`, `scan`, `range`, `conditional`, `barrier`, `admin`, `other`): the counter `chunkdb_commands_total`, also by `outcome` (`ok`, `error`), and the histogram `chunkdb_command_duration_seconds`.
- The counter `chunkdb_errors_total` by `code`: `invalid_argument`, `out_of_range`, `auth_required`, `auth_failed`, `unknown_command`, `version_mismatch`, `internal`, `other`.
- Server counters: `chunkdb_auth_failures_total`, `chunkdb_connections_rejected_total`, `chunkdb_malformed_requests_total`, `chunkdb_large_chunk_refusals_total`.
- Server gauges: `chunkdb_active_connections`, `chunkdb_pending_connections`, `chunkdb_loaded_chunks`, `chunkdb_large_chunk_containers`, `chunkdb_large_chunk_container_limit`.
- Table gauges by `table`: `chunkdb_open_wal_streams`, `chunkdb_background_queue_depth`, `chunkdb_txn_history_bytes`, `chunkdb_table_fenced`, `chunkdb_slot_frontier_frozen`.
- Table counters by `table`: `chunkdb_checkpoints_total`, `chunkdb_background_checkpoints_total`, `chunkdb_background_checkpoint_failures_total`, `chunkdb_background_queue_full_inline_total`, `chunkdb_compressed_checkpoint_images_total`, `chunkdb_wal_batch_flushes_total`, `chunkdb_wal_barriers_total`, `chunkdb_wal_barrier_full_syncs_total`, `chunkdb_evictions_total`, `chunkdb_eviction_recency_skips_total`, `chunkdb_eviction_snapshot_builds_total`, `chunkdb_eviction_probes_total`, `chunkdb_eviction_no_progress_cycles_total`, `chunkdb_eviction_forced_wal_flushes_with_data_total`, `chunkdb_eviction_forced_wal_flushes_empty_batch_total`, `chunkdb_empty_chunk_gcs_total`, `chunkdb_unique_loaded_chunks_total`, `chunkdb_txn_history_copies_total`, `chunkdb_txn_history_copied_bytes_total`.
- From the exporter: the gauge `chunkdb_up`.

## Security

The HTTP endpoint has no authentication and no TLS: anyone who reaches it reads the metrics, table names included.
It listens on loopback by default; with `--allow-remote`, bind it to a private address or put a TLS proxy in front of it.
On Linux and macOS the exporter refuses a password file that its group or others can access.

## Docker

The container runs as the image's `chunkdb` user, which must own the password file with mode 0600; find its IDs with `docker run --rm --entrypoint id ghcr.io/chunkdb/chunkdb:2`.

```sh
docker run -d --name chunkdb-exporter --network host \
  --mount type=bind,src="$(pwd)/monitor.password",dst=/run/monitor.password,readonly \
  --entrypoint chunkdb_exporter ghcr.io/chunkdb/chunkdb:2 \
  --uri chunk://monitor@127.0.0.1:4242/ --password-file /run/monitor.password
```
