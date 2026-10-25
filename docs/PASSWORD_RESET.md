# Reset a forgotten password

You replace a user's password while preserving their data and rights.
Stop the server before running the offline administration tool.

## Docker

These commands use the container and volume from [Quick start](QUICK_START.md).

<!-- docker-quickstart:reset -->
```sh
docker stop chunkdb-quickstart
printf 'choose-a-new-private-password\n' | docker run --rm -i --entrypoint chunkdb_admin \
  -v chunkdb-quickstart-data:/var/lib/chunkdb ghcr.io/chunkdb/chunkdb:2 \
  --data-dir /var/lib/chunkdb/data reset-password admin --password-file /dev/stdin
docker start chunkdb-quickstart
```

Wait for the container to become healthy, then test the new password:

<!-- docker-quickstart:reset-login -->
```sh
export CHUNKDB_PASSWORD='choose-a-new-private-password'
chunk-cli --uri "$CHUNKDB_URI" PING
```

```output
PONG
```

## Downloaded server

```sh
printf 'choose-a-new-private-password\n' > ./new.password
./chunkdb_admin --data-dir ./data reset-password admin --password-file ./new.password
```

Restart your server and connect with the new password.
The tool must have permission to write the data directory.
Delete your password file after use.
See [Users and rights](USERS.md) for changing a known password without stopping the server.
