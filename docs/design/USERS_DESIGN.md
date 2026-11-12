# Authentication and rights
The user-facing [users guide](../USERS.md) and [protocol reference](../PROTOCOL.md) define the supported surface.

## Exchange and storage

HELLO 3 USER and AUTH carry SCRAM-SHA-256 messages as binary parameter frames.
The server stores salt, iteration count and stored/server keys rather than passwords; clients validate the server signature.
A login over TLS can be bound to the connection with `tls-exporter` or `tls-server-end-point`; the server computes the binding data from its own TLS session and compares it before it checks the proof.
The client speaks first and knows its TLS version, so it picks the type without a negotiation round; every protocol 3 server takes both, and `y,,` on TLS is refused as a downgrade.
A binding mismatch is reported apart from a wrong password because it depends on neither the user nor the password; a user that requires binding is not, so the reply does not tell that the user exists.
Unknown-user authentication uses a derived salt and the same failure message as an invalid password.
Passwords use UTF-8 bytes without SASLprep normalization.

chunkdb.users is checksummed and atomically replaced with file/directory syncs.
It stores users, verifier data, management capability, the channel binding requirement, grants and the unknown-user secret.
Existing sessions consult current rights on each statement, so changes take effect without reconnecting.
Every change of the users wakes the feed, and open watches recheck READ on their table from a published view of the rights, without the registry lock.
Each user has an in-memory incarnation, assigned at creation or load; a session whose user was dropped, or dropped and created again, has no user and closes.
Rights form READ < WRITE < ADMIN and wildcard grants cover future tables.
Dropping a table removes its specific grants.

## Admission and recovery

Bootstrap creates the first management user from supplied credentials only when no users exist.
The last management user cannot be dropped or demoted.
Offline password reset requires writer ownership and completes valid pending migration recovery before changing a verifier; invalid or inconsistent recovery is refused.
Named grant/revoke/drop migrations capture users and ledger participants in their durable redo decision.
Backup captures the durable users file with schema and ledger under metadata admission.
See [durability](../DURABILITY_CONTRACT.md) for failure outcomes.
