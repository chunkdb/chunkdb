# Users and rights

A user is a login identity with a password and rights on tables.
You can give a game client READ access while reserving WRITE for its server.

## First administrator

For a downloaded server, choose the first administrator and password before startup:

```sh
export CHUNKDB_ADMIN_USER=admin
export CHUNKDB_ADMIN_PASSWORD='choose-a-long-private-password'
./chunkdb_server --data-dir ./data --backup-dir ./backups
```

The administrator has MANAGES USERS and ADMIN on all tables.
Later starts load saved users and ignore bootstrap settings.
Without saved users or bootstrap settings, native startup fails.
The [Docker](DOCKER.md) entrypoint generates a password when you provide none.

Clients authenticate using SCRAM-SHA-256, which proves knowledge of a password without sending the password itself.
Use TLS when you need transport encryption; SCRAM does not encrypt queries or results.
You can change your own password through the CLI:

```sh
chunk-cli --uri "$CHUNKDB_URI" "ALTER USER admin PASSWORD"
```

The CLI prompts twice and computes the password verifier locally.
See [User statements](reference/USER_STATEMENTS.md) for creation, password files, and conditional forms.

## Rights

ADMIN includes WRITE, and WRITE includes READ.
Rights apply to whole tables, including grants naming tables not yet created.
A wildcard grant on `*` applies to every table, including future tables.
Dropping a table removes its specific grants.

| Statement | Required right |
| --- | --- |
| GET BLOCK, GET CHUNK, GET AREA, SCAN CHUNKS, DESCRIBE, WATCH | READ on the table |
| SET BLOCK, DELETE BLOCK, SET CHUNK | WRITE on the table |
| ALTER TABLE, DROP TABLE, RECOVER TABLE, CREATE SLOT, DROP SLOT | ADMIN on the table |
| CREATE TABLE, SHOW METRICS, RELOAD TLS | ADMIN on all tables |
| FLUSH WAL | WRITE on at least one table |
| SHOW TABLES, SHOW SLOTS | None; results include accessible tables only |
| ACK | An open slot watch |
| PING | None |
| User management, GRANT, REVOKE, BACKUP TO, SHOW MIGRATIONS | MANAGES USERS |
| MIGRATE | The inner statement's rights |

Changing your own password is the exception to the user-management requirement.
A table with no rights for your user appears absent with NO_TABLE.
Missing rights otherwise produce PERMISSION_DENIED.
Rights changes affect existing connections immediately.
An open watch whose user loses READ on its table ends with NO_TABLE as soon as the change is applied, and the connection closes.
Pushes not yet written to the socket are dropped; a push already partly written closes the connection without the error.
An ACK the server processes after the change is applied is refused; one processed at the same moment may still be accepted.
Dropping a user ends its connections: an open watch, and the next statement on any other connection, receive `AUTH_REQUIRED reason=user_dropped`, and the connection closes.
Connections of a dropped user never get the rights of a user created again under the same name, nor change its password.
Slot ownership is matched by user name: a user created again under that name can take over a slot the dropped one watched without ADMIN on `*`.
Changing a user's password, by the user or an administrator, does not end that user's open connections or watches.

Migration replay checks current rights before returning skipped or conflict.
For repeatable lists that drop tables, use a deployment user with wildcard ADMIN.
This grant survives table removal; table-specific ADMIN does not.

## Password protection

Stored verifiers permit offline password guessing, so protect your data directory and backups.
The users file permits owner-only access: mode 0600 on Linux/macOS and a protected owner-only ACL on Windows.
Startup tightens existing users-file permissions.
Failure to obtain operating-system randomness is an error.
Passwords use exact UTF-8 bytes without normalization.
The last user with MANAGES USERS cannot lose that permission or be dropped.

`--auth none` disables user authentication and grants every connection all rights.
Use it only for local development.
For a forgotten password, follow [Reset a forgotten password](PASSWORD_RESET.md).
