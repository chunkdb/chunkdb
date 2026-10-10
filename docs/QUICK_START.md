# Quick start

You need Docker and Go 1.25.6 or later for this path.
Run these commands from a checkout of [chunkdb](https://github.com/chunkdb/chunkdb).

## Start the server

```sh
docker build -t chunkdb:local .
docker run -d --name chunkdb-quickstart -p 127.0.0.1:4242:4242 \
  -v chunkdb-quickstart-data:/var/lib/chunkdb chunkdb:local
docker logs chunkdb-quickstart
```

The first log contains `First administrator: admin`, a generated password, and a command to change it.
The server runs without root; one persistent volume holds `data/` and `backups/`.
Before connecting, wait for `docker inspect --format '{{.State.Health.Status}}' chunkdb-quickstart` to report `healthy`.
For an environment password, a mounted password file, TLS or Compose, see [DOCKER.md](DOCKER.md).

## Connect

Install the CLI from current source, then use the generated password:

```sh
go install github.com/chunkdb/chunk-cli/v2/cmd/chunk-cli@main
export PATH="${GOBIN:-$(go env GOPATH)/bin}:$PATH"
export CHUNKDB_PASSWORD=$(docker logs chunkdb-quickstart 2>/dev/null | sed -n 's/^Generated password: //p')
chunk-cli --uri chunk://admin@127.0.0.1:4242/ PING
```

The reply is `PONG`.
On a later container start, use your saved password; the server does not generate another one.
After trying the example, you can change it with `chunk-cli --uri chunk://admin@127.0.0.1:4242/ "ALTER USER admin PASSWORD"`; enter the new password twice and update `CHUNKDB_PASSWORD` before the next command.

## Write blocks and read an area

```sh
chunk-cli --uri chunk://admin@127.0.0.1:4242/ "CREATE TABLE world (kind u8, name text(16) NULL) CHUNK 2 x 2"
chunk-cli --uri chunk://admin@127.0.0.1:4242/ "SET BLOCK 0 0 IN world kind = 1, name = 'grass'"
chunk-cli --uri chunk://admin@127.0.0.1:4242/ "SET BLOCK 1 0 IN world kind = 2, name = 'wall'"
chunk-cli --uri chunk://admin@127.0.0.1:4242/ "GET BLOCK 0 0 FROM world"
chunk-cli --uri chunk://admin@127.0.0.1:4242/ --blocks "GET AREA 0 0 TO 0 0 FROM world"
```

CREATE prints `OK`; each write prints its chunk version.
GET BLOCK prints:

```text
kind = 1
name = 'grass'
```

The area contains chunk `0 0`, with two present blocks out of four, `grass` and `wall`.
Area coordinates count chunks; block coordinates count individual blocks.
See [CQL.md](CQL.md) for chunk forms, types and schema changes.
To repeat CREATE, first run `DROP TABLE world` through the same CLI; this deletes the example table and its data.

## Watch a change

In one terminal, keep the connection open:

```sh
chunk-cli --uri chunk://admin@127.0.0.1:4242/ watch world
```

After its `start <epoch>:<revision>` line, run in another terminal with the same login environment:

```sh
chunk-cli --uri chunk://admin@127.0.0.1:4242/ "SET BLOCK 0 0 IN world kind = 3, name = 'door'"
```

The watcher prints a `change revision ... time_ms ... user admin` header and the changed block's before/after values.
Press Ctrl-C to stop it; retained subscriptions are described in [CHANGE_FEED.md](CHANGE_FEED.md).

## Binary alternative

With a 2.0 archive for your platform from [Releases](https://github.com/chunkdb/chunkdb/releases), extract it and run its server from that directory (Windows uses `chunkdb_server.exe`):

```sh
export CHUNKDB_ADMIN_USER=admin
export CHUNKDB_ADMIN_PASSWORD='choose-a-long-private-password'
./chunkdb_server --listen-uri chunk://127.0.0.1:4242/ --data-dir ./data --backup-dir ./backups
```

Keep this process running and follow the same CLI commands with your chosen password.
The archive contains `chunkdb_verify` and `chunkdb_restore`; the matching SHA256 sidecar is a separate download beside the archive.
Compare the archive hash before extracting.

## A small world in each client

Go and TypeScript examples use named migrations and can be run again against their `world_go` and `world_js` tables.
The CLI example creates `world`; drop that example table before running it again.
[Go](https://github.com/chunkdb/chunkdb-go/blob/main/examples/world/main.go), [TypeScript](https://github.com/chunkdb/chunkdb-js/blob/main/examples/world.ts) and [CLI](https://github.com/chunkdb/chunk-cli/blob/main/examples/world.sh) fill an area, read it and watch an update.
Their READMEs show the single run command and login settings.
Continue with [users and rights](USERS.md), [transactions](TRANSACTIONS.md) or [backups](BACKUP.md).
