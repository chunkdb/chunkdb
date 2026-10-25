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
SHOW USERS
DROP USER bot
```

```output
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
DROP USER [IF EXISTS] user
SHOW USERS
GRANT right [, right ...] ON table | * TO user
REVOKE right [, right ...] ON table | * FROM user
```

The CLI accepts PASSWORD in place of VERIFIER and prompts twice unless you provide `--new-password-file`.
VERIFIER accepts a quoted value or a binary client parameter.
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
