# Users and rights: design

Users with passwords and per-table rights replace the single token (#63). This document fixes the login exchange, the storage, the statements and the right each statement needs.

## Login

`HELLO` stays the first line: it agrees on the protocol, logs in, and answers the server's limits. Login is SCRAM-SHA-256 (RFC 5802, RFC 7677): the password never crosses the network, and the server proves it knows the verifier too.

```text
C: HELLO 3 USER <name> $1     $1 = client-first-message   n,,n=<name>,r=<client nonce>
S: +SCRAM <server-first-message>                            r=<nonce>,s=<salt>,i=<iterations>
C: AUTH $1                    $1 = client-final-message   c=biws,r=<nonce>,p=<proof>
S: %8 ... server_signature    the HELLO map, plus v=<server signature> for the client to check
```

- The SCRAM messages travel as parameter frames, so nothing is escaped. Channel binding is not used (`n,,`); TLS protects the connection.
- An unknown user gets the same exchange with a salt derived from the name and a server secret, and fails at `AUTH` like a wrong password, so replies do not tell which users exist.
- A wrong proof gets `-ERR AUTH_FAILED`; failures count toward `--max-auth-failures` per connection and the per-address bans, as today.
- `HELLO 3` without `USER` logs in only on a server started with `--auth none` (local development), as a user with every right; otherwise `-ERR AUTH_REQUIRED`.
- `AUTH <token>`, `--token`, `--token-file` and `CHUNKDB_TOKEN` go. Clients take the user and password from the URI (`chunk://user:password@host:4242/`) or their options.

## Storage

- `chunkdb.users` in the data directory holds, per user: the name, the SCRAM verifier (salt, iterations, `StoredKey`, `ServerKey`), whether the user manages users, and the grants. It is written like a table manifest: checksummed, replaced atomically, file and directory synced, so a crash leaves the old or the new file.
- The server secret for unknown-user salts lives in the same file.
- A user and grant change is one rewrite of the file under the catalog's operation lock; connections already logged in keep their user and see the new rights from their next statement.
- Passwords are never stored or logged; neither are verifiers.

## Statements

```text
CREATE USER bot VERIFIER $1 [MANAGES USERS]      -> +OK
ALTER USER bot VERIFIER $1                       -> +OK
ALTER USER bot [NO] MANAGES USERS                -> +OK
DROP USER bot                                    -> +OK
GRANT READ | WRITE | ADMIN ON world | * TO bot   -> +OK
REVOKE READ | WRITE | ADMIN ON world | * FROM bot -> +OK
SHOW USERS                                       -> *n of {name, manages_users, grants}
```

- `$1` is the verifier `SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>` (base64 parts, the PostgreSQL form), computed by the client from the password. A plain password is never accepted; iterations are at least 4096.
- Rights are ordered: `ADMIN` includes `WRITE`, which includes `READ`. A grant on `*` covers every table, those created later included; a grant on a name is removed when that table is dropped.
- A user may change their own verifier; everything else on users needs `MANAGES USERS`. The last user that manages users cannot be dropped or lose that right.

## Rights per statement

| Statement | Needs |
|---|---|
| `PING`, `HELLO` | nothing |
| `GET BLOCK`, `GET CHUNK`, `GET AREA`, `SCAN CHUNKS`, `DESCRIBE` | `READ` on the table |
| `SET BLOCK`, `DELETE BLOCK`, `SET CHUNK` | `WRITE` on the table |
| `ALTER TABLE`, `DROP TABLE` | `ADMIN` on the table |
| `CREATE TABLE` | `ADMIN` on `*` |
| `SHOW TABLES` | nothing; lists the tables the user has any right on |
| `FLUSH WAL` | `WRITE` on any table |
| `SHOW METRICS` | `ADMIN` on `*` |
| user statements | `MANAGES USERS` (own `ALTER USER ... VERIFIER` excepted) |

A refused statement gets `-ERR PERMISSION_DENIED <right> on <table>`; a table the user has no right on reads as `NO_TABLE`, so names do not leak. The check runs once per statement on rights the connection already holds; it adds no lock to the hot path.

## First administrator and recovery

- A data directory without `chunkdb.users` creates it at the first start from `CHUNKDB_ADMIN_USER` and `CHUNKDB_ADMIN_PASSWORD` (or `--admin-user` and `--admin-password-file`): that user manages users and has `ADMIN` on `*`. Without them, and without `--auth none`, the server refuses to start.
- `chunkdb_admin --data-dir <dir> reset-password <user> --password-file <file>` rewrites a user's verifier offline; it takes the writer lock, so the server must be stopped.

## Other

- A server listening beyond localhost without TLS logs a warning at start: SCRAM protects passwords, but data and statements travel in the clear.
- SHA-256, HMAC and PBKDF2 are implemented in the engine (with the RFC test vectors), so builds without OpenSSL still log in; nonces and salts come from the operating system's random source.
- Clients compute verifiers and run the exchange; JS, Go and the CLI follow the server.
