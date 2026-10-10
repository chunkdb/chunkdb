# Authentication and rights in 2.0

The user-facing [users guide](../USERS.md) and [protocol reference](../PROTOCOL.md) define the supported surface.

## Exchange and storage

HELLO 3 USER and AUTH carry SCRAM-SHA-256 messages as binary parameter frames.
The server stores salt, iteration count and stored/server keys rather than passwords; clients validate the server signature.
Channel binding is not used; TLS protects statement and data confidentiality.
Unknown-user authentication uses a derived salt and the same failure message as an invalid password.
Passwords use UTF-8 bytes without SASLprep normalization.

chunkdb.users is checksummed and atomically replaced with file/directory syncs.
It stores users, verifier data, management capability, grants and the unknown-user secret.
Existing sessions consult current rights on each statement, so changes take effect without reconnecting.
Rights form READ < WRITE < ADMIN and wildcard grants cover future tables.
Dropping a table removes its specific grants.

## Admission and recovery

Bootstrap creates the first management user from supplied credentials only when no users exist.
The last management user cannot be dropped or demoted.
Offline password reset requires writer ownership and completes valid pending migration recovery before changing a verifier; invalid or inconsistent recovery is refused.
Named grant/revoke/drop migrations capture users and ledger participants in their durable redo decision.
Backup captures the durable users file with schema and ledger under metadata admission.
See [durability](../DURABILITY_CONTRACT.md) for failure outcomes.
