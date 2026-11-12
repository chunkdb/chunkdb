# Store and watch blocks

You start a server, store two grid cells, read them, and watch an update.
You need Docker and Go 1.25.6 or later.
## 1. Start the server
<!-- docker-quickstart:start -->
```sh
docker run -d --name chunkdb-quickstart -p 127.0.0.1:4242:4242 \
  -v chunkdb-quickstart-data:/var/lib/chunkdb ghcr.io/chunkdb/chunkdb:2
docker logs chunkdb-quickstart
```

```output
First administrator: admin
Generated password: <generated-password>
```

Wait until this command prints `healthy`:
<!-- docker-quickstart:health -->
```sh
docker inspect --format '{{.State.Health.Status}}' chunkdb-quickstart
```

```output
healthy
```

## 2. Connect the shell

```sh
go install github.com/chunkdb/chunk-cli/v2/cmd/chunk-cli@latest
export PATH="${GOBIN:-$(go env GOPATH)/bin}:$PATH"
```
<!-- docker-quickstart:login -->
```sh
export CHUNKDB_PASSWORD=$(docker logs chunkdb-quickstart 2>/dev/null | sed -n 's/^Generated password: //p')
export CHUNKDB_URI='chunk://admin@127.0.0.1:4242/'
chunk-cli --uri "$CHUNKDB_URI" shell
```

```output
chunk>
```

Enter statements at the shell’s `chunk>` prompt; write results such as `2`, `3`, and `4` are chunk versions.

## 3. Write and read blocks

Create a table with two columns and chunks of four blocks:
<!-- docker-quickstart:write -->
```cql docs-check session=main
CREATE TABLE world (kind u8, name text(16) NULL) CHUNK 2 x 2
SET BLOCK 0 0 IN world kind = 1, name = 'grass'
SET BLOCK 1 0 IN world kind = 2, name = 'wall'
```

```output
OK
1
2
```
<!-- docker-quickstart:read -->
```cql docs-check session=main
GET BLOCK 0 0 FROM world
```

```output
kind = 1
name = 'grass'
```

## 4. Watch an update

In another terminal, export the same password and URI, then start a watcher:
<!-- docker-quickstart:watch -->
```sh docs-check-watch
chunk-cli --uri "$CHUNKDB_URI" watch world
```

```output
start <epoch>:2
change revision 3 time_ms <time-ms> user admin
  block 0 0: {kind = 1, name = 'grass'} -> {kind = 3, name = 'door'}
```

After the watcher prints `start`, return to the shell and change a block:
<!-- docker-quickstart:watch-update -->
```cql docs-check session=main
SET BLOCK 0 0 IN world kind = 3, name = 'door'
```

```output
3
```

Press Ctrl-C to stop it.

## Next steps

[Blocks reference](reference/BLOCKS.md), [Change feed](CHANGE_FEED.md), and [Users and rights](USERS.md).
