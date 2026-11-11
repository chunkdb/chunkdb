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
On TLS, clients bind the login to the connection; see [Channel binding](#channel-binding).
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
| WATCH SLOT on a slot another user owns | READ on the table and ADMIN on all tables |
| SET BLOCK, DELETE BLOCK, SET CHUNK | WRITE on the table |
| ALTER TABLE, DROP TABLE, RECOVER TABLE, CREATE SLOT, DROP SLOT | ADMIN on the table |
| CREATE TABLE, SHOW METRICS, RELOAD TLS | ADMIN on all tables |
| FLUSH WAL | WRITE on at least one table |
| SHOW TABLES, SHOW SLOTS | None; results include accessible tables only |
| SHOW MIGRATIONS | None; results include records on tables with ADMIN, and GRANT and REVOKE records with MANAGES USERS; applying users show with MANAGES USERS |
| ACK | An open slot watch |
| PING | None |
| User management, GRANT, REVOKE, BACKUP TO | MANAGES USERS |
| MIGRATE | The inner statement's rights |

Changing your own password is the exception to the user-management requirement.
A table with no rights for your user appears absent with NO_TABLE.
Missing rights otherwise produce PERMISSION_DENIED.
Rights changes affect existing connections immediately.
An open watch whose user loses READ on its table ends as soon as the change is applied: the server sends NO_TABLE best-effort and closes the connection.
A watch of a slot its user does not own ends the same way when that user loses ADMIN on `*`.
Pushes not yet written to the socket are dropped; a push already partly written, or a socket that cannot take more, closes the connection without the error.
An ACK the server processes after the change is applied is refused; one processed at the same moment may still be accepted.
Dropping a user ends its connections: an open watch ends at once with a best-effort `AUTH_REQUIRED reason=user_dropped`, any other connection receives it on its next statement, and the connection closes.
Connections of a dropped user never get the rights of a user created again under the same name, nor change its password.
A slot belongs to the user named as its owner, matched by name; DROP USER leaves slots unchanged, so a dropped owner's slot can be attached only with ADMIN on `*` until a user of that name exists again.
Changing a user's password, by the user or an administrator, does not end that user's open connections or watches.

Migration replay checks current rights before returning skipped or conflict.
A name recorded for a table without your ADMIN, or for GRANT or REVOKE without MANAGES USERS, returns PERMISSION_DENIED.
For repeatable lists that drop tables, use a deployment user with wildcard ADMIN.
This grant survives table removal; table-specific ADMIN does not.

## Channel binding

A login made over TLS can be bound to that connection.
A bound login fails when something between the client and the server ends TLS, even if the client accepted a wrong certificate.
The chunkdb clients bind every login made over `chunks://`.
To refuse logins that are not bound, start the server with `--auth-channel-binding require`, or require it for single users:

```cql
ALTER USER bot REQUIRE CHANNEL BINDING
```

Binding needs TLS that ends at the server: behind a load balancer that ends TLS, turn channel binding off in the client and do not require it.
An unbound login of a user that requires binding fails with the same AUTH_FAILED as a wrong password.
A user that requires binding cannot log in to a server without a `chunks://` listener, and a password reset keeps the requirement; start the server with TLS to log in and run `ALTER USER bot NO REQUIRE CHANNEL BINDING`.

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
