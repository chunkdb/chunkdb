# Run chunkdb with Docker

You run a server with persistent data and backups in one named volume.
For the complete write/read/watch tutorial, follow [Quick start](QUICK_START.md).

## Start

```sh
docker run -d --name chunkdb -p 127.0.0.1:4242:4242 \
  -v chunkdb-data:/var/lib/chunkdb ghcr.io/chunkdb/chunkdb:2
docker logs chunkdb
```

On its first start, the image prints these lines among its logs:

```output
First administrator: admin
Generated password: <generated-password>
```

Install chunk-cli as shown in [Quick start](QUICK_START.md#2-connect-the-shell), then connect to this container:

```sh
export CHUNKDB_PASSWORD=$(docker logs chunkdb 2>/dev/null | sed -n 's/^Generated password: //p')
export CHUNKDB_URI='chunk://admin@127.0.0.1:4242/'
chunk-cli --uri "$CHUNKDB_URI" shell
```

```output
chunk>
```

A fresh server has no tables; create them with CREATE TABLE.
Later starts load saved users and generate no new password.
The generated password stays in `docker logs` for as long as the container exists.
Change it after your first login:

```sh
chunk-cli --uri "$CHUNKDB_URI" "ALTER USER admin PASSWORD"
```

`ALTER USER ... PASSWORD` is a chunk-cli form, not a server statement.
The CLI prompts for the new password twice, computes its verifier locally, and sends `ALTER USER admin VERIFIER $1`.

Changing bootstrap settings does not change existing passwords; see [Users and rights](USERS.md).

## Choose the first password

Set a password when creating a fresh data directory:

```sh
export CHUNKDB_ADMIN_PASSWORD='choose-a-long-private-password'
docker run -d --name chunkdb -p 127.0.0.1:4242:4242 \
  -e CHUNKDB_ADMIN_PASSWORD -v chunkdb-data:/var/lib/chunkdb ghcr.io/chunkdb/chunkdb:2
```

Or mount a password file readable by the container's user:

```sh
docker run -d --name chunkdb -p 127.0.0.1:4242:4242 \
  -v chunkdb-data:/var/lib/chunkdb \
  --mount type=bind,src="$(pwd)/admin.password",dst=/run/admin.password,readonly \
  -e CHUNKDB_ADMIN_PASSWORD_FILE=/run/admin.password ghcr.io/chunkdb/chunkdb:2
```

CHUNKDB_ADMIN_USER selects the administrator's name, default admin.
Server arguments --admin-user and --admin-password-file are also supported; an explicit file takes precedence over the environment password.
These settings apply only before users exist.

## Data and health

The image runs as the chunkdb user, includes TLS support, and listens on port 4242.
One volume at /var/lib/chunkdb contains data/ and backups/.
Directories created by the server use owner-only mode 0700; new files use mode 0600.
Startup tightens existing users-file permissions.

Named volumes remain accessible to the container's user.
For bind mounts, grant that same UID/GID access to the mounted directory.
Find its IDs with `docker run --rm --entrypoint id ghcr.io/chunkdb/chunkdb:2`.
BACKUP TO 'snapshot' writes /var/lib/chunkdb/backups/snapshot; see [Backup and restore](BACKUP.md).

Check health with:

```sh
docker inspect --format '{{.State.Health.Status}}' chunkdb
```

```output
healthy
```

Removing a container retains the named volume; deleting the volume deletes the database and its backups.
Keep the loopback binding for local use.
For TLS, mount certificate/key files and use a chunks:// listen URI; see [Server flags](SERVER_FLAGS.md).

## Compose and local builds

```sh
docker compose up -d --build
docker compose logs chunkdb
```

Compose publishes on loopback and uses one named volume.
It accepts CHUNKDB_ADMIN_USER, CHUNKDB_ADMIN_PASSWORD, CHUNKDB_DURABILITY (relaxed), CHUNKDB_WORKERS (4), and CHUNKDB_DATA_DIR (/var/lib/chunkdb/data).
CHUNKDB_NOFILE_SOFT and CHUNKDB_NOFILE_HARD default to 65536.
Add service mounts and server arguments for password files or TLS certificates.

To build your checkout, run `docker build -t chunkdb:local .` and substitute chunkdb:local for the published image.
For a forgotten password, follow [Reset a forgotten password](PASSWORD_RESET.md).
