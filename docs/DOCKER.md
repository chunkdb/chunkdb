# Docker

This document describes how to build and run `chunkdb_server` in Docker with persistent storage and reproducible test execution.

## Prerequisites

- macOS: Docker Desktop
- Windows: Docker Desktop (WSL2 backend recommended)
- Linux: Docker Engine + Docker Compose plugin

The default server port is `4242`.

## Build Image

Build the local runtime image:

```bash
docker build -t chunkdb:local .
```

Build with TLS support in the build step:

```bash
docker build --build-arg CHUNKDB_WITH_TLS=ON -t chunkdb:local-tls .
```

## Run Container

Run `chunkdb_server` directly with a named volume:

```bash
docker volume create chunkdb_data

export CHUNKDB_TOKEN=$(openssl rand -hex 32)

docker run -d --name chunkdb \
  -p 127.0.0.1:4242:4242 \
  --ulimit nofile=65536:65536 \
  -e CHUNKDB_TOKEN \
  -v chunkdb_data:/var/lib/chunkdb/data \
  chunkdb:local \
  --listen-uri chunk://0.0.0.0:4242/ \
  --data-dir /var/lib/chunkdb/data \
  --durability relaxed \
  --workers 4
```

The image ships **no default token**. With none supplied the server refuses to
start, so `CHUNKDB_TOKEN` (or a mounted `--token-file`) is required.

`-p 127.0.0.1:4242:4242` publishes on loopback only. Publishing on all interfaces
(`-p 4242:4242`) also bypasses many host firewall setups via Docker's own NAT
rules — do it only deliberately, and build with `CHUNKDB_WITH_TLS=ON` first so
the token is not sent in cleartext.

Token-in-URI examples are development-only: `CHUNKDB_TOKEN` and `--token-file` are
the supported forms. Note that token sources are resolved in the order
`--token-file` > `CHUNKDB_TOKEN` > `--token` > `--listen-uri`, so a token in the
environment silently outranks one passed as a flag. The server logs which source
it used (never the value) and warns when a lower-priority source is ignored.

Check logs:

```bash
docker logs -f chunkdb
```

The runtime image includes a Docker `HEALTHCHECK` that sends `PING` and expects
`+PONG`. `PING` is answered before the auth gate, so the probe needs no credential.

Stop/remove:

```bash
docker rm -f chunkdb
```

## Run with Docker Compose

`CHUNKDB_TOKEN` has no default and must be set — `docker compose up` fails fast
without it:

```bash
export CHUNKDB_TOKEN=$(openssl rand -hex 32)
docker compose up -d
```

The compose service publishes on `127.0.0.1:4242` only.

Inspect logs:

```bash
docker compose logs -f chunkdb
```

Verify `PING`/`INFO` from inside the container (`netcat` is included in runtime image):

```bash
docker compose exec -T chunkdb sh -lc 'printf "AUTH $CHUNKDB_TOKEN\r\nPING\r\nINFO\r\nQUIT\r\n" | nc 127.0.0.1 4242'
```

Run tests in container (test profile):

```bash
docker compose --profile test run --rm chunkdb-test
```

Stop and remove containers + volume:

```bash
docker compose down -v
```

## Configuration via Compose Variables

`docker-compose.yml` supports these variables:

- `CHUNKDB_TOKEN` (**required** — no default; compose fails fast if unset)
- `CHUNKDB_DURABILITY` (default: `relaxed`)
- `CHUNKDB_WORKERS` (default: `4`)
- `CHUNKDB_DATA_DIR` (default: `/var/lib/chunkdb/data`)
- `CHUNKDB_NOFILE_SOFT` (default: `65536`)
- `CHUNKDB_NOFILE_HARD` (default: `65536`)

Example:

```bash
CHUNKDB_TOKEN=mytoken CHUNKDB_DURABILITY=fsync-wal CHUNKDB_NOFILE_SOFT=65536 CHUNKDB_NOFILE_HARD=65536 docker compose up -d
```

## Multi-Arch Buildx (Optional)

Create and use a buildx builder:

```bash
docker buildx create --name chunkdb-builder --use
```

Build multi-arch image (`linux/amd64`, `linux/arm64`) and push to registry:

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t <registry>/<namespace>/chunkdb:<tag> \
  --push .
```

Notes:
- `--push` is required for multi-platform manifest publication.
- Local `docker run` without push usually uses single-arch image builds.

## Performance in Docker: What to Expect

Running `chunkdb` in Docker adds virtualization + container I/O overhead versus direct host execution. The exact penalty depends on host OS, Docker backend, and filesystem path mapping.

To measure this on your machine with the same benchmark command for host and Docker:

```bash
scripts/bench/host_vs_docker.sh
```

The script prints a scenario table with:

- `ops/s` (host vs Docker)
- `p95` and `p99` latency (host vs Docker)
- `overhead_ops_%` (`(host_ops - docker_ops) / host_ops * 100`)

Interpretation guidance:

- near-zero or negative overhead can happen on noisy runs; repeat before drawing conclusions;
- write-heavy sparse scenarios are usually more sensitive to container/filesystem overhead;
- compare runs only under the same durability mode and similar system load.
