# Export metrics to Prometheus

The server serves no HTTP.
`chunkdb_exporter` logs in to a server, reads [SHOW METRICS](reference/SERVER.md) on each scrape and serves the text at `GET /metrics` for Prometheus.
It ships beside `chunkdb_server` in the release archives and the Docker image.

## Set up

Create a user with the MONITOR right, which reads metrics and status and nothing else:

```sh
chunk-cli --uri "$CHUNKDB_URI" --new-password-file monitor.password "CREATE USER monitor PASSWORD"
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
| `--password-file` | required with a user | A file whose first line is the user's password; it is read again at each login. |
| `--listen` | `127.0.0.1:14242` | `host:port` of the HTTP listener, with an IPv6 host in brackets such as `[::1]:14242`. |
| `--allow-remote` | off | Allow `--listen` on an address that is not loopback. |
| `--tls-ca-file` | system certificates | Certificates that verify a `chunks://` server; the certificate must name the URI's host. |
| `--no-channel-binding` | off | Do not bind the login to its TLS connection; needed only when something between the exporter and the server ends TLS. |
| `--timeout-ms` | `5000` | Limit for one scrape: connecting, logging in and reading SHOW METRICS. |
| `--log-level` | `info` | `debug`, `info`, `warn` or `error`. |

SIGTERM or SIGINT stops the exporter.

## Scrapes

The exporter keeps one connection and logs in again after it fails, so a server restart needs no action.
Each response appends `chunkdb_up`: 1 with the server's metrics, or 0 alone with status `503` when the server cannot be reached, refuses the login or refuses SHOW METRICS.
The exporter logs the reason when scrapes start failing and when they work again.
Scrapes that arrive while one runs share its result.
SHOW METRICS leaves out a table that an exclusive statement such as ALTER TABLE holds or waits for, and a fenced table.

`HEAD /metrics` returns the headers of `GET /metrics`.
Other paths get `404`, other methods `405` with `Allow: GET, HEAD`, and a malformed request `400`.
A request line and headers must arrive within the timeout and fit in 8192 bytes, or the reply is `408` or `431`.
The listener holds at most 16 connections; a slow client delays no other one.

## Security

The HTTP endpoint has no authentication and no TLS: anyone who reaches it reads the metrics, table names included.
It listens on loopback by default; with `--allow-remote`, bind it to a private address or put a TLS proxy in front of it.
Keep the password file readable only by the exporter's user.

## Docker

```sh
docker run -d --name chunkdb-exporter --network host \
  --mount type=bind,src="$(pwd)/monitor.password",dst=/run/monitor.password,readonly \
  --entrypoint chunkdb_exporter ghcr.io/chunkdb/chunkdb:2 \
  --uri chunk://monitor@127.0.0.1:4242/ --password-file /run/monitor.password
```
