# Users and rights

A chunkdb server has users with passwords and rights per table. Clients log in with the user and password, for example from the URI `chunk://bot:password@host:4242/`; the password never crosses the network ([PROTOCOL.md](PROTOCOL.md), SCRAM-SHA-256).

## The first administrator

The first start of a data directory creates its administrator: a user who manages users and has `ADMIN` on every table.

```bash
printf 'change-me\n' > ./admin.password
./build/chunkdb_server --data-dir ./data --admin-user admin --admin-password-file ./admin.password
```

`CHUNKDB_ADMIN_USER` and `CHUNKDB_ADMIN_PASSWORD` do the same. Later starts read the users the data directory holds (`chunkdb.users`) and ignore these settings. Without users and without them, the server refuses to start.

`--auth none` runs without users: every connection logs in with `HELLO 3` alone and has every right. It is meant for local development; the server warns when it listens beyond localhost.

## Users

```text
CREATE USER bot VERIFIER $1 [MANAGES USERS]
ALTER USER bot VERIFIER $1
ALTER USER bot [NO] MANAGES USERS
DROP USER bot
SHOW USERS                                -> *n of {name, manages_users, grants}
```

- Passwords are used as their UTF-8 bytes, without SASLprep normalization; a client and the server must agree on the same bytes.
- `VERIFIER` takes the SCRAM verifier `SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>` (base64 parts, at least 4096 iterations and a 16-byte salt), which a client computes from the password; the server never receives or stores the password. It is a parameter or a quoted value.
- A login with an unknown name gets a server-first message like a user's with 4096 iterations, so wrong names and wrong passwords look the same; a verifier with more iterations shows in that message that its user exists.
- A user may change their own password. Everything else on users needs `MANAGES USERS`; the last user who manages users cannot be dropped or lose that right.
- Changes take effect at once, also for connections already logged in.

## Rights

```text
GRANT READ | WRITE | ADMIN ON world | * TO bot
REVOKE READ | WRITE | ADMIN ON world | * FROM bot
```

- `ADMIN` includes `WRITE`, which includes `READ`. `REVOKE` takes away the named right and those above it: after `REVOKE WRITE` a user who had `ADMIN` keeps `READ`.
- `*` stands for every table, those created later included. A grant may name a table before it exists, so rights can be set up before the table is created; grants on a table go when it is dropped.

| Statement | Needs |
|---|---|
| `GET BLOCK`, `GET CHUNK`, `GET AREA`, `SCAN CHUNKS`, `DESCRIBE`, `WATCH` | `READ` on the table |
| `SET BLOCK`, `DELETE BLOCK`, `SET CHUNK` | `WRITE` on the table |
| `ALTER TABLE`, `DROP TABLE`, `CREATE SLOT`, `DROP SLOT` | `ADMIN` on the table |
| `CREATE TABLE`, `SHOW METRICS` | `ADMIN` on `*` |
| `FLUSH WAL` | `WRITE` on some table |
| `SHOW TABLES`, `SHOW SLOTS` | nothing; lists only tables the user has a right on |
| `ACK` | an open slot WATCH, which requires `READ` on its table |
| `PING` | nothing |
| user statements, `GRANT`, `REVOKE` | `MANAGES USERS` |
| `MIGRATE 'name' <statement>` | the inner statement's rights |
| `SHOW MIGRATIONS` | `MANAGES USERS` |

A statement without the right gets `-ERR PERMISSION_DENIED <right> on <table>`. A table the user has no right on at all reads as one that does not exist (`NO_TABLE`).

`SHOW MIGRATIONS` includes the applying user's name and the original statement text.
With `--auth none`, migration listing is permitted and the applying user is empty.
The inner statement's current rights are checked before returning `skipped` or a conflict.
A conflict names an already-used migration name to anyone allowed to run the submitted inner statement; `MANAGES USERS` is not required to learn that the name exists this way.
Dropping a table removes its specific grants. A per-table `ADMIN` user rerunning a list gets `NO_TABLE` on earlier steps for a table dropped later, including its DROP step. Run such repeatable lists with a deployment user holding `ADMIN` on `*`, whose wildcard grant survives a drop; keep application users' grants specific to their tables. A user with `MANAGES USERS` can grant this to an existing deployment user:

```text
GRANT ADMIN ON * TO deploy
```

## A lost password

With the server stopped:

```bash
printf 'new-password\n' > ./new.password
./build/chunkdb_admin --data-dir ./data reset-password admin --password-file ./new.password
```

It writes a new verifier for the user and keeps their rights.
