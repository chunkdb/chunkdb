# Read and write behavior

You address blocks by block coordinates and chunks or areas by chunk coordinates.
A table's chunk geometry determines which blocks belong together.

## Reads

- An absent block returns NULL; a present block follows its column types.
- An unwritten chunk returns NULL without a version token.
- Reading an unwritten chunk does not reserve a revision or advance the completed write frontier.
- GET CHUNK can return a written empty chunk after its blocks are deleted.
- GET AREA and SCAN CHUNKS include only chunks with present blocks.
- Area reads and scan pages need a transaction to share one consistent snapshot.

## Writes

- Values and conditional versions are checked before changing data.
- SET BLOCK changes the named columns; other values of an existing block remain.
- SET CHUNK replaces every column and rejects a mismatched schema version.
- A version is an equality token for the whole chunk, including changes to other blocks.
- Ordinary write durability follows the table's configured mode.

## Metadata and streams

ALTER can wait for running table statements before publishing a schema change.
A failure leaving the table unavailable fences it until RECOVER TABLE or a writer restart.
Named migrations record one repeatable metadata change and completion together.

WATCH streams committed changes in revision order; slot watches deliver only durable changes.
A backup records a separate completed cut for each copied table.
SHOW METRICS returns authenticated Prometheus text.

See [CQL reference](CQL.md), [Transactions](TRANSACTIONS.md), [Change feed](CHANGE_FEED.md), and [Durability](DURABILITY_CONTRACT.md).
