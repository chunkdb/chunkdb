# Docker in 2.0

Start the published image:

```sh
docker run -d --name chunkdb -p 127.0.0.1:4242:4242 \
  -v chunkdb-data:/var/lib/chunkdb ghcr.io/chunkdb/chunkdb:2.0.0
docker logs chunkdb
```

The first start generates a password for `admin` and prints it with a password-change command.
Use that password to connect through [QUICK_START.md](QUICK_START.md).
Later starts load `data/chunkdb.users` and print no new password.
Changing bootstrap environment variables does not change an existing user's password; use `ALTER USER admin PASSWORD` through `chunk-cli`.
The image runs as the `chunkdb` user, includes TLS support, and listens on port 4242.
Plain connections use `chunk://`; TLS requires certificate/key mounts and a `chunks://` listen URI.

## Build from source

From a repository checkout:

```sh
docker build -t chunkdb:local .
```

Substitute `chunkdb:local` for the published image in the commands above and below.

## Choose the first password

Pass a password through the environment on the first start:

```sh
export CHUNKDB_ADMIN_PASSWORD='choose-a-long-private-password'
docker run -d --name chunkdb -p 127.0.0.1:4242:4242 \
  -e CHUNKDB_ADMIN_PASSWORD -v chunkdb-data:/var/lib/chunkdb ghcr.io/chunkdb/chunkdb:2.0.0
```

Or mount a file readable by the container's non-root user:

```sh
docker run -d --name chunkdb -p 127.0.0.1:4242:4242 \
  -v chunkdb-data:/var/lib/chunkdb \
  --mount type=bind,src="$(pwd)/admin.password",dst=/run/admin.password,readonly \
  -e CHUNKDB_ADMIN_PASSWORD_FILE=/run/admin.password ghcr.io/chunkdb/chunkdb:2.0.0
```

`CHUNKDB_ADMIN_USER` chooses the first administrator's name (default `admin`).
`--admin-user` and `--admin-password-file` server arguments are also supported; an explicit file takes precedence over the environment password.
Passwords are used only to bootstrap a directory with no users.

## Persistence and health

One volume at `/var/lib/chunkdb` holds `data/` and `backups/`.
`BACKUP TO 'snapshot'` writes `/var/lib/chunkdb/backups/snapshot`; see [BACKUP.md](BACKUP.md).
The image also includes `chunkdb_verify` and `chunkdb_restore`, runnable through `docker run --entrypoint`.
For a bind mount, give the image's `chunkdb` UID/GID write access to the mounted directory.
Find those IDs with `docker run --rm --entrypoint id ghcr.io/chunkdb/chunkdb:2.0.0`.
The health check sends `HELLO 3` without credentials and accepts a HELLO map or `AUTH_REQUIRED`.
Check it with `docker inspect --format '{{.State.Health.Status}}' chunkdb`.

```sh
docker stop chunkdb
docker start chunkdb
```

Removing the container retains the named volume.
Deleting the volume deletes the database and its backups.
Publish on `127.0.0.1` for local use; configure TLS and access controls before exposing another interface.
See [USERS.md](USERS.md) and [SERVER_FLAGS.md](SERVER_FLAGS.md).

## Compose

```sh
docker compose up -d --build
docker compose logs chunkdb
```

Compose uses one named volume and publishes only on loopback.
It accepts `CHUNKDB_ADMIN_USER`, `CHUNKDB_ADMIN_PASSWORD`, `CHUNKDB_DURABILITY` (default `relaxed`), `CHUNKDB_WORKERS` (default `4`), `CHUNKDB_DATA_DIR` (default `/var/lib/chunkdb/data`), and `CHUNKDB_NOFILE_SOFT` / `CHUNKDB_NOFILE_HARD` (default `65536`).
For a mounted password file or TLS certificates, add the mounts and server arguments to the service.
