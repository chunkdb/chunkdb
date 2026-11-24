# Authentication and rights
The user-facing [users guide](../USERS.md) and [protocol reference](../PROTOCOL.md) define the supported surface.

## Exchange and storage

HELLO 3 USER and AUTH carry SCRAM-SHA-256 messages as binary parameter frames.
The server stores salt, iteration count and stored/server keys rather than passwords; clients validate the server signature.
A login over TLS can be bound to the connection with `tls-exporter` or `tls-server-end-point`; the server computes the binding data from its own TLS session and compares it before it checks the proof.
The client speaks first and knows its TLS version, so it picks the type without a negotiation round; every protocol 3 server takes both, and `y,,` on TLS is refused as a downgrade.
A binding mismatch is reported apart from a wrong password because it depends on neither the user nor the password; a user that requires binding is not, so the reply does not tell that the user exists.
Unknown-user authentication uses a derived salt and the same failure message as an invalid password; its iteration count and salt length are those of one existing user's verifier, chosen by the name, or 4096 and 16 bytes when there are no users, so the challenge looks like that of a user who exists.
Passwords use UTF-8 bytes without SASLprep normalization.
A user has at most two verifiers, so a password rotates without a moment in which no password logs in.
The server sends one salt and iteration count before the client proves its password, so an added verifier must share them; the proof is checked against both verifiers.
AUTH succeeds only while the user still has the verifier the proof matched.

chunkdb.users is checksummed and atomically replaced with file/directory syncs.
Its checked CKDU format stores users, verifier data, management capability, the channel binding requirement, grants,
two unsigned limit fields and the unknown-user secret; see [storage format](../STORAGE_FORMAT.md#users).
Earlier development formats are refused before permission changes or bookkeeping,
including in a standalone UserRegistry. There is no users feature bit or catalog ownership link.
Existing sessions consult current rights on each statement, so changes take effect without reconnecting.
Every change of the users wakes the feed, and open watches recheck READ on their table from a published view of the rights, without the registry lock.
Each user has an in-memory incarnation, assigned at creation or load; a session whose user was dropped, or dropped and created again, has no user and closes.
Rights form READ < WRITE < ADMIN; wildcard and prefix grants cover future tables, and a user gets the highest of its exact, prefix and wildcard grants.
A user without prefix grants is checked with two map lookups per statement; prefix grants add a walk over the user's prefixes that sort at or before the table name.
MONITOR, BACKUP and REPLICATION are server rights, granted ON * and stored as user flags; they include no table right; ADMIN on `*` also allows every MONITOR statement and RESTORE TABLE.
Dropping a table removes its specific grants.

## Admission and recovery

Bootstrap creates the first management user from supplied credentials only when no users exist.
The last management user cannot be dropped or demoted.
Offline password reset requires writer ownership and completes valid pending migration recovery before changing a verifier; invalid or inconsistent recovery is refused.
Named grant/revoke/drop migrations capture users and ledger participants in their durable redo decision.
Backup captures the durable users file with schema and ledger under metadata admission.
See [durability](../DURABILITY_CONTRACT.md) for failure outcomes.
