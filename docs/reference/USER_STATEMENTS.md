# CREATE USER, ALTER USER, DROP USER, GRANT, REVOKE, SHOW USERS

You create login identities and grant access to tables.
A right permits reading, writing, or administering a table.

## Examples

The CLI computes a password verifier locally from your password file:

```sh docs-check-shell
printf 'example-private-password\n' > "$EXAMPLE_DIR/bot.password"
chunk-cli --uri "$CHUNKDB_URI" --new-password-file "$EXAMPLE_DIR/bot.password" "CREATE USER bot PASSWORD"
chunk-cli --uri "$CHUNKDB_URI" --new-password-file "$EXAMPLE_DIR/bot.password" "ALTER USER bot PASSWORD"
```

```output
OK
OK
```

`EXAMPLE_DIR` is a writable directory you choose.
Continue in the CLI shell:

```cql docs-check session=main
GRANT READ, WRITE ON world TO bot
REVOKE WRITE ON world FROM bot
ALTER USER bot MANAGES USERS
ALTER USER bot NO MANAGES USERS
ALTER USER bot LIMIT max_connections = 4, statements_per_sec = 200
SHOW USERS
DROP USER bot
```

```output
OK
OK
OK
OK
OK
1)
  name = admin
  manages_users = true
  grants:
    * = ADMIN
2)
  name = bot
  manages_users = false
  grants:
    world = READ
OK
```

## Syntax

```text
CREATE USER [IF NOT EXISTS] user VERIFIER verifier [MANAGES USERS]
ALTER USER user VERIFIER verifier
ALTER USER user [NO] MANAGES USERS
ALTER USER user LIMIT max_connections = n [, statements_per_sec = n]
ALTER USER user LIMIT statements_per_sec = n
DROP USER [IF EXISTS] user
SHOW USERS
GRANT right [, right ...] ON table | * TO user
REVOKE right [, right ...] ON table | * FROM user
```

The CLI accepts PASSWORD in place of VERIFIER and prompts twice unless you provide `--new-password-file`.
VERIFIER accepts a quoted value or a binary client parameter.
LIMIT requires MANAGES USERS and accepts either setting once, in either order.
Zero disables a limit; `statements_per_sec` is at most 65536.
Lowering `max_connections` leaves existing connections open and refuses new logins until their count falls below the limit.
A rolling one-second statement window retains unexpired admissions when a rate limit is lowered.
CANCEL, KILL and ROLLBACK remain available when the statement limit is reached.
Limit refusals return RATE_LIMITED with `reason` and `retry_after_ms` without applying the request or changing its transaction.
Dropping and recreating a user does not transfer the old user's authenticated sessions to the new identity.
An AUTH proof made before that user's deletion or verifier change is refused without counting a bad password.
An unconfirmed users-record replacement returns OUTCOME_UNKNOWN with `stage=users_record` and publishes the whole visible record.
After an unknown outcome, an unchanged retry succeeds only after the visible record is durable.
See [Protocol](../PROTOCOL.md) for authentication encoding.

## Arguments

- `user`: `[a-z_][a-z0-9_]*`, from 1 through 63 bytes.
- `right`: READ, WRITE, or ADMIN; ADMIN includes WRITE, and WRITE includes READ.
- `table`: a specific table, including one not yet created.
- `*`: all tables, including tables created later.
- `MANAGES USERS`: permission to manage users, rights, migration listing, and backups.
- Conditional clauses: skip an existing or absent user without changing it; normal rights still apply.

## Returns

- CREATE, ALTER, DROP, GRANT, and REVOKE: `OK`.
- SHOW USERS: user names, MANAGES USERS state, and grants.

## Errors

| Error | When | What to do |
| --- | --- | --- |
| PERMISSION_DENIED | You lack MANAGES USERS or change another user's password without it. | Request access. |
| INVALID_ARGUMENT | The name, verifier, rights, or final manager removal is invalid. | Correct the reported argument. |
| INVALID_ARGUMENT | You change a user or a right on a server started with `--read-only`. | Run it on a server started without `--read-only`. |
| RATE_LIMITED | Repeated failures temporarily ban the source address. | Wait for the returned retry_after_ms. |
| AUTH_FAILED | Login credentials are incorrect. | Check the password or use [Reset a forgotten password](../PASSWORD_RESET.md). |

## Notes

You may change your own password without MANAGES USERS.
The last user who manages users cannot be dropped or lose that right.
Changes affect connections already logged in.
Passwords use their exact UTF-8 bytes, without normalization.

GRANT and REVOKE apply comma-separated lists atomically.
REVOKE removes the named right and every higher right; revoking WRITE from ADMIN leaves READ.
Dropping a table removes its table-specific grants, while wildcard grants remain.

## Limits

Verifiers use SCRAM-SHA-256 with 4096 through 1000000 iterations and a salt of at least 16 bytes.
See [Users and rights](../USERS.md) for bootstrap and statement permissions.
