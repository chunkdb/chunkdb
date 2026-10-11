# Protocol foundation: reply shapes and compatibility

This defines the A1–A9 and G7 protocol contracts for the protocol foundation.
The interfaces below let later features extend replies without moving known fields.

## Values and extension rules (A1, A5)

Notation below describes decoded RESP3 values, not CQL syntax.
An epoch is the existing 16-byte identity represented by 32 lowercase hexadecimal characters.
A revision, schema version, connection id, timestamp and transaction id are unsigned integers; coordinates remain signed.
`null` means RESP3 null, strings naming fields or push kinds are bulk strings, and forms/tags are bulk bytes.

- Ordinary arrays and push arrays accept at least the known length and ignore trailing fields, including nested fixed-prefix entries.
- Schema-sized rows require at least the described column count and ignore additional trailing values; known columns retain their declared positions.
- Maps ignore unknown keys; an unknown push is fully decoded under existing depth/byte bounds, then ignored without consuming an ordinary reply.
- An unknown push never advances a client's delivered or ACK position; malformed RESP framing remains a protocol error.
- Known fields retain their types and positions throughout 2.x; an ordinary reply changes shape only after an explicit clause or HELLO option.
- Unknown DESCRIBE column types retain their type text and return raw bulk bytes for that column alone; known columns keep typed decoding.
- Integers in the signed int64 range use `:<decimal>\r\n`; integers outside it use `(<decimal>\r\n`, including future negative i128 sums.
- Go retains exact integers with `math/big.Int` plus checked int64/uint64 accessors; JS uses `bigint` above its exact number range.
- CLI JSON emits integer numbers within inclusive `[-9007199254740991,9007199254740991]` and decimal strings outside that range, consistently for values, metadata and errors.

The WATCH map and schema-map push replace the inspected development shapes in this coordinated server/client change.
Existing ordinary replies otherwise remain unchanged; no version string selects a new shape.
Clients can accept legacy WATCH/schema/change shapes when talking to older development servers, without weakening validation of new shapes.

## Handshake and session ownership (A2)

Syntax is `HELLO 3 [USER name $1] [WITH key = value, ...]`.
The authenticated reply retains `protocol`, `server_version`, `server_signature`, `max_line_bytes`, `max_parameters`, `max_area_chunks`, `max_response_bytes`, and `max_scan_limit`.
It adds the following fields; the SCRAM intermediate exchange remains unchanged, and negotiated options become visible only after successful authentication.

| Field | Shape and meaning |
| --- | --- |
| `connection_id` | Nonzero u64, unique during this server process; not a durable idempotency id. |
| `features` | Array of supported protocol capability names; these differ from enabled on-disk features. |
| `options` | Map of actually applied option names to effective values. |
| `not_applied` | Map of ignored requested names to `{reason}`; unknown names use `reason=unknown_option`. |
| `idle_timeout_ms` | Effective configured idle timeout; current default 60000. |
| `limits` | Map containing all seven limits below. |

Known options are `client_name` (text), `statement_timeout_ms` (nonnegative milliseconds; zero disables), and `default_tag` (bytes, maximum 256).
Names are case-insensitive and normalized to lowercase; duplicate names are `INVALID_ARGUMENT reason=duplicate_option`.
Invalid known values are `INVALID_ARGUMENT reason=option_value option=<name>`, not authentication failures.
Unknown options are lexically parsed, accepted and reported as not applied; they never increment authentication-failure counters.
Parameters in WITH follow the same framing rules as all other parameters, independently of SCRAM's authentication parameter.
Unset options use server defaults; explicit per-statement TAG overrides `default_tag`, including an empty tag.

| Limit key | Task 51 value and ownership |
| --- | --- |
| `max_area_chunks` | 256, currently enforced by `kMaxChunkRangeChunks`; task 53 replaces the constant with its configured limit. |
| `max_area_response_bytes` | 67108864, currently enforced; report the effective value after flags exist. |
| `txn_max_written_chunks` | 64, currently enforced in statement, commit and intent decoding paths; task 53 changes all three together. |
| `txn_max_read_chunks` | 1024, currently enforced; task 53 supplies the configured limit. |
| `max_watches` | Effective server configuration; current default 64. |
| `max_returning_rows` | 65536, the declared ceiling from E3; RETURNING remains unavailable until task 55. |
| `max_expression_nodes` | 256, the declared ceiling from E1; expressions remain unavailable until task 55. |

Unavailable capabilities do not become supported merely because their numeric ceilings appear in HELLO.
The initial capability names are `protocol_foundation`, `ordered_versions`, `read_metadata`, `feed_progress`, `slot_filters`, `slot_takeover`, and `lineage`.
`statement_timeout`, `write_tags`, `returning`, `expressions`, `watch_catalog`, and `feed_coalescing` appear only when their actual implementation is available.
Task 51 stores negotiated session options in a session structure that task 54 can move off the worker stack.
Applying a timeout requires task 54's cancellation-before-commit rules; applying a default tag requires task 53's durable commit-group path.
Stored timeout/tag values remain in `not_applied` with `reason=implementation_pending` until their semantic owners activate them.

## Grammar-independent framing (A3)

One lexical pass counts every occurrence of `$` followed by decimal digits outside quoted literals and identifiers.
It recognizes single-quoted literals, doubled quote escapes, `x'…'`, `b'…'`, and the specified double-quoted identifiers.
Malformed non-quote tokens do not stop this pass; an unterminated quote occupies the rest of the request line.
`$` without digits is not a parameter token; `$0` and oversized indices are tokens for framing but invalid for statement semantics.

The count determines how many following bulk frames to consume, independently of the maximum numeric index.
For valid unique contiguous parameters, frames retain their existing numeric-index order: frame 1 binds $1 even when $2 appears first in the text.
Only the drain of a rejected statement treats that many frames as opaque input without binding indices.
The current semantic rule remains: indices are unique, contiguous and start at 1; repeated or gapped references fail with SYNTAX after draining their occurrence count.
Thus `bad $1 $1` drains two frames, and `bad $3` drains one; neither uses the maximum index as its frame count.
Quoted `'$9'` drains none, even on an otherwise invalid statement.
Clients use this lexical count when sending arbitrary statements, rather than guessing intended parameters after a malformed quote.

Parsed, resolvable statements retain per-column and aggregate parameter bounds, checked before allocating payloads.
Parse failures, unknown commands and failures to resolve parameter types drain frames under `--max-unparsed-parameter-bytes`, default 1048576 payload bytes in aggregate.
Draining uses bounded scratch storage and checked length arithmetic; it never allocates the advertised frame length.
After a successful drain the server returns exactly one SYNTAX, UNKNOWN_COMMAND or semantic error and reads the next request.
An over-bound frame, malformed length/terminator, truncated frame or framing count overflow yields PROTOCOL and closes the connection.
The essential regression pipelines good request, unparsable request plus frames, good request, and checks all three ordered replies.

## Errors and outcomes (A4)

The wire form is `-ERR CODE [key=value ...] [free text]\r\n`.
Keys precede free text; values are whitespace-free UTF-8 with percent escapes for whitespace, `%` and `=`.
Clients expose `code`, known typed keys, unknown keys for diagnostics, message and retry class; classification never depends on free text.
Unknown codes are generic fatal errors; malformed escape sequences remain opaque diagnostic values, rather than changing classification.

| Class | Codes | CLI exit |
| --- | --- | --- |
| `reread` | VERSION_MISMATCH, CONDITION_FAILED, SCHEMA_MISMATCH | 2 |
| `rerun` | CONFLICT, exclusively transaction conflicts | 3 |
| `backoff` | BUSY for server load, DISK_FULL, RATE_LIMITED | 4 |
| `unknown_outcome` | OUTCOME_UNKNOWN | 5 |
| `fatal` | Every other or unknown code; includes MIGRATION_MISMATCH, SLOT_BUSY, CHECK_FAILED, CANCELLED, TIMEOUT, KILLED | 1 |

Success exits 0; CLI local usage errors use fatal exit 1.
`--json` emits `{error:{code,class,keys,message}}` and preserves exact numeric metadata.
Known keys include `reason`, `table`, `column`, `current`, `expected`, `actual`, `epoch`, `revision`, `connection_id`, `retry_after_ms`, and `stage`.
VERSION_MISMATCH and SCHEMA_MISMATCH preserve `current=<version>`; E3's CONDITION_FAILED also carries `current=<version>`.
Additional expected/actual metadata may supplement those keys, never replace them; migration reuse becomes MIGRATION_MISMATCH, and an attached slot becomes SLOT_BUSY.
Every existing error call site is audited; CONFLICT must not remain a generic schema/migration/slot code.
OUTCOME_UNKNOWN replaces the ambiguous INTERNAL prefix and records the failed publication stage; no client retries it automatically.
SHOW COMMIT and durable ID resolution belong to task 53, so task 51 documents the unresolved outcome without pretending resolution already exists.
CANCELLED/TIMEOUT caused by statement cancellation always mean not applied; after the commit point the statement returns its actual outcome.
A lost connection after a write was sent is likewise not evidence that the write was unapplied.

## Read shapes and ordered positions (A6, A7)

DESCRIBE keeps existing fields and adds `epoch`, `revision`, `dims`, and `chunk_columns`; current values are dims 2 and empty chunk columns.
The existing `version` remains the schema version, distinct from the table commit revision.
A chunk version is its producing commit revision, ordered only within the same table epoch.
DESCRIBE's revision is the completed frontier, never the largest allocated revision or a per-chunk counter.
Read-only DESCRIBE returns null for revision because its process cannot establish the writer's completed frontier.
Read-write DESCRIBE returns the numeric frontier from the existing producer registry, without cross-process scanning or write bookkeeping.
Aborted/reserved revisions become terminal gaps; the frontier may cross them only after they cannot produce a later commit.
The existing clock-then-producer-bound sampling order must be preserved, including recovery's reserved ceiling; outstanding producers prevent frontier advancement.
DESCRIBE computes this without creating a feed, archive worker or per-write optional bookkeeping for an unwatched table.
Read-write opening refuses missing clock and initialized-marker records when chunk artifacts or a capped startup inventory prevent proving fresh initialization.
The refusal precedes recovery-record publication; restore a consistent backup or reinitialize the whole store.
Manifest-only initialization remains valid, and read-only inspection keeps its unknown frontier null.

| Statement clauses | Reply |
| --- | --- |
| GET BLOCK, none | Existing `[values…]` or null. |
| GET BLOCK WITH VERSION | `[chunk_version, row_or_null]`. |
| GET BLOCK WITH SCHEMA | `[schema_version, row_or_null]`. |
| GET BLOCK WITH VERSION WITH SCHEMA | `[chunk_version, schema_version, row_or_null]`, regardless of clause order. |
| GET AREA, none | Existing array of `[cx, cy, form]` entries. |
| GET AREA WITH SCHEMA | `[schema_version, entries]`. |

Row, version and schema metadata are captured under the same table lease and chunk read/snapshot boundary.
An absent block with a retained committed chunk version still returns that version.
The version is null when no committed version remains retained, including never-written chunks and collected empty chunks.
An empty or torn WAL header alone is not a committed version.
Persistent versions for collected chunks belong to task 52's tombstones.
For transaction-private modified chunks the version is null, because no commit produced those private values yet; the committed snapshot version remains available before modification.
Schema is known even when a block is absent or an area is empty; an empty area is `[]`, never a null payload.
Task 52's 3D area entry is `[cx,cy,cz,form]`; dimensions select the coordinate prefix before trailing extensions are ignored.
Area reads keep their existing consistency semantics across chunks; WITH SCHEMA does not invent an atomic multi-chunk snapshot.

## Push envelopes (A1, A8, A9)

Every unsolicited event is a RESP3 push whose first element is its kind.
The schema payload is a map inside a two-element push: `>2`, bulk `schema`, then a RESP3 map.
A naked map would be indistinguishable from an ordinary reply, so it is not the wire container.

| Kind | Known prefix |
| --- | --- |
| WATCH ordinary reply | Map `{epoch, revision}` with the established starting completed position. |
| `schema` | `[schema, {epoch,revision,schema_version,dims,columns,chunk_columns}]`. |
| `change` | `[change,epoch,revision,commit_time_ms,user,schema_version,blocks,tag,txn,chunk_changes]`. |
| `progress` | `[progress,epoch,revision]`. |
| `resync` | `[resync,epoch,revision,parent_epoch,fork_revision]`. |
| `chunks` | `[chunks,epoch,revision,commit_time_ms,user,schema_version,chunks,tag,txn]`. |
| `catalog` | `[catalog,epoch,revision,commit_time_ms,user,events,txn]`. |

`columns` and `chunk_columns` use the existing column-description maps; unknown map keys and type names follow A1.
Block entries are `[x,y,before,after]` in 2D or `[x,y,z,before,after]` in 3D.
Chunk-change entries are `[cx,cy,before,after]` or `[cx,cy,cz,before,after]`; before/after are chunk-column rows or null.
Chunk push entries are `[cx,cy,version,form_or_null]` or `[cx,cy,cz,version,form_or_null]`; null form denotes deletion.
Block axes retain the existing integer-or-`[chunk,local]` representation when an absolute position exceeds int64; chunk axes are signed int64.
Catalog events are extensible maps with `kind`, `action`, `name`, `table`, and `data`; irrelevant keys are null and unknown keys are ignored.
Task 56 defines each catalog event's data map and authorization before emitting it; task 51 reserves this envelope without claiming WATCH CATALOG exists.
`txn` is a nullable unsigned u64 directory counter, encoded as an integer or big number under the boundary rule above.
It has the same type in change, chunks, catalog, repeated-ID and SHOW COMMIT replies.
Current change pushes send `tag=null`, `txn=null`, and `chunk_changes=[]`; later owners fill these fields without moving them.
Task 53's repeated-ID reply is `{repeated:true,txn,versions}`, and SHOW COMMIT is `{txn,versions,durable}` or null: canonical D5 includes txn.

## Progress and durable consumer ownership (A8)

Progress advertises the completed frontier when changes were filtered or the table was idle, at most once per `--feed-progress-ms` (default 1000).
A slot's progress cannot pass its durable frontier; it cannot pass changes that must still be sent to that watch.
Change, chunks and progress revisions become ACK-eligible only after their entire frames have been written, not after encoding or queueing.
Clients update their delivered bound only after returning that event to the application; schema/resync/unknown and reserved catalog events do not advance it.
ACK remains monotonic, in the watch epoch and no greater than the last eligible change, chunks or progress revision.

A `chunks` push carries mutations exactly like `change`, so a fully written `chunks` frame delivered to the application is ACK-eligible and starts (or keeps) the deadline of the oldest unacknowledged mutation; progress stays ACK-eligible but never starts or extends the timeout; schema, resync and unknown pushes move nothing.

CREATE SLOT persists normalized AREA, COLUMNS, NO BEFORE and CHUNKS filters with its ACK position.
WATCH omitting filters inherits them; explicitly supplied filters must match exactly after canonicalization, or return INVALID_ARGUMENT reason=slot_filters.
Column projections persist stable column ids in declared order; equivalent area bounds normalize identically, independent of spelling.
A dropped projected column makes the slot lost with reason=schema_filter; it never silently broadens or changes the projection.
Changing filters requires creating a different slot; IF NOT EXISTS must compare filters rather than silently accept incompatible existing state.
Task 51 implements these fixed filters, including a CHUNKS event for each relevant committed chunk; task 56 adds lagging-consumer coalescing and multiple areas.
Task 52 changes the form format/dimensions without changing the chunk push prefix selected by dims.

CREATE SLOT accepts each AREA, COLUMNS, NO BEFORE and CHUNKS suffix once, in any order after `ON table`.
SHOW SLOTS retains `table`, `name`, `epoch`, `acked`, `retained_bytes` and `lost`, adding the following keys.

| Key | Shape |
| --- | --- |
| `filters` | Map `{area,columns,column_ids,no_before,chunks}`. |
| `filters.area` | Null for no area; otherwise `[lower_axis_0,…,lower_axis_d-1,upper_axis_0,…,upper_axis_d-1]`, with signed coordinates and inclusive bounds. |
| `filters.columns` | Null for all columns; otherwise `[bulk_name_or_null,…]`, aligned with `column_ids`. |
| `filters.column_ids` | Null for all columns; otherwise `[unsigned_column_id,…]`, containing persisted stable IDs in projection order. |
| `filters.no_before` | Boolean; false by default. |
| `filters.chunks` | Boolean; false by default. |
| `lost_reason` | Null for a live slot; `retention_limit` or `schema_filter` for a persisted loss. |

Area arrays contain `2*d` values; the current 2D order is `[x0,y0,x1,y1]`, and clients do not hard-code four entries.
A rename changes only the display name; a dropped ID retains its position with a null display name.
Persisted IDs determine WATCH and IF NOT EXISTS identity, rather than names that can change.
Existing lost records map to `retention_limit`: `FeedSlots::Retain` currently persists loss only when retained bytes exceed the slot limit.
`Column::id` is never reused; `RenameColumn` preserves it, while `DropColumn` removes the column from the live schema.
The listing captures slot filters and the live schema under one table lease so names cannot race a schema replacement.

`WATCH … ACK TIMEOUT ms` requires a slot and a positive interval; omission disables this watch deadline.
The monotonic clock starts when the first unacknowledged change or chunks frame finishes writing; later mutations do not refresh it.
ACK removes covered timestamps, making the oldest remaining mutation the new deadline basis; partial writes never start the clock.
Expiry closes the owning connection with a best-effort TIMEOUT reason=ack_timeout and retains only safely published ACK state.
A partial frame, pinned TLS retry or blocked socket cannot delay closure or receive an injected mid-frame error.
All accepted TCP connections, including TLS, set keepalive with `--tcp-keepalive-ms` default 60000 as the idle probe interval.
Platform rounding and probe counts are documented; this OS probe interval is not a promise of an exact consumer-death detection time.

TAKEOVER serializes with old-claim ACK validation, revokes that claim, stops its reader/pump, and closes its connection with KILLED reason=slot_takeover.
It publishes only the last actually received and validated ACK, including a pending durable publication, before attaching the new owner.
It never automatically ACKs merely sent/delivered changes, chunks or progress; unacknowledged revisions remain replayable.
The replacement starts at the persisted ACK; crash before persistence can replay events, never lose acknowledged durable progress.
A claim generation fences late old ACKs; no two valid claims can send or update the same slot concurrently.
Lock ordering never waits for socket drainage or worker shutdown while holding table/catalog locks.
Concurrent ownership handoffs return SLOT_BUSY reason=takeover_pending rather than create a second owner.
An ordinary attach needs READ; takeover of the same user's connection needs READ, and another user's connection additionally needs ADMIN on `*`, matching G2.

## Lineage and restoration (A9)

A new table has `parent_epoch=null` and `fork_revision=null`.
A restore from a known cut creates a new epoch and persists the immediate source epoch plus its completed cut revision.
Restoring a restored table records the immediate parent, not a misleading transitive ancestor; revisions preserve their numeric values across the fork.
Both fields are nullable manifest TLVs owned by a registered lineage incompatibility feature; checksum/size/type validation precedes publication.
Resync always carries both fields; unknown lineage supplies nulls, never a guessed relationship.
An equal parent epoch and consumer revision at or below the fork establishes compatible ancestry; a later revision or unrelated epoch requires rebuilding.
Continuation requires the necessary replay prefix to survive, or the consumer to be exactly at the fork.
Otherwise the lineage-bearing resync requires rebuilding; backups do not supply historical catch-up files.

Source evidence: `backup_create.cpp` enumerates `L_*` chunk directories and copies checkpoint images/current WAL prefixes, then saves slot metadata in lines 224–227.
It does not copy `.chunkdb.feed/`'s retained historical WAL/base files into the backup inventory (`feed_slot_records.hpp` names that directory).
`backup_restore.cpp:152–155` changes the slot epoch and advances every restored slot's written position to the backup cut.
`FeedSlots::Reader` in `feed_slots.cpp` rejects positions preceding the earliest retained slot; lineage metadata alone cannot replace omitted history.

## Explicit features, recovery and rollback (G7)

The initial named registry is `feed_slots` (incompat bit 1), `migrations` (bit 2), `table_lineage` (new bit 4), `slot_filters` (new bit 8), and `feature_activation` (new bit 16).
Lineage and slot-filter records need new ownership bits because an older build already knows bits 1 and 2 but cannot preserve their new metadata.
`feature_activation` owns the directory activation journal and is directory-scoped; enabling it alone atomically publishes one checked root manifest.
New data directories enable these baseline features at creation, before serving requests; every new table inherits the directory's table-scoped features.
Existing directories preserve their recorded bits until `ENABLE FEATURE name`; CREATE SLOT/MIGRATE/restore must not implicitly set a missing bit.
An operation needing a disabled feature returns INVALID_ARGUMENT reason=feature_disabled feature=<name> before changing state.
ENABLE requires ADMIN on `*`, is idempotent when enabled, and rejects unknown names with INVALID_ARGUMENT reason=unknown_feature.
Features are monotonic: dropping the last slot or migration never clears their bits, and no automatic DISABLE operation is provided.

Other feature activations require `feature_activation` first on an existing directory, or return INVALID_ARGUMENT reason=feature_required feature=feature_activation.
This explicit prerequisite makes older writers reject the directory before any partial multi-manifest decision can exist.
Enabling validates the complete directory/table target set before writing anything and excludes concurrent table creation/drop/publication.
A checked directory activation journal durably records target table identities and feature-bit unions before publishing any replacement manifest.
Recovery merges those bits into the validated manifests; it does not overwrite unrelated schema/options with stale whole-manifest images.
Recovery finishes that decision before serving requests; before the journal decision a failure leaves existing manifests unchanged.
After that decision an incomplete publication returns OUTCOME_UNKNOWN stage=feature_activation and fences serving until recovery completes.
Each manifest uses the established atomic checked publication and directory-sync rules; no unsynced shortcut or fallback is allowed.
The journal is cleared only after all affected manifests and directories are durable; replay is idempotent across every publication crash point.

SHOW STATUS requires ADMIN on `*` and returns `{directory,tables,activation_pending}`.
The directory map is `{enabled,compat,ro_compat,incompat}`; table maps add `name` and appear in the `tables` array.
`enabled` is `[bulk_feature_name,…]`, containing known feature names.
The raw masks are checked unsigned u64 values, preserving unsupported bits explicitly.
Unknown keys and feature names remain extensible; later status fields do not change these prefixes.

`activation_pending` is a boolean derived from the protected activation journal/state, not inferred from feature masks.
The status snapshot serializes with feature activation and table create/drop/publication, without adding bookkeeping to ordinary reads.
An unhealthy serving fence is not bypassed: SHOW STATUS returns the corresponding recovery error if it cannot obtain a truthful protected snapshot.
This prevents a fabricated `activation_pending=false` after an uncertain journal publication.

Manifest masks currently occupy u32 fields; the wire uses exact unsigned integer values without assuming a JavaScript number width.
`chunkdb_verify --features` reads the registry, manifests and activation journal without changing them and reports missing/conflicting feature ownership.
Unknown incompatibility bits reject every open; unknown read-only-compatible bits allow only read-only opens; unknown compatible bits are preserved.
COMPATIBILITY lists each feature's scope, format ownership, minimum reader/writer and rollback procedure.
Rollback to a build lacking an enabled incompatibility feature requires an explicitly supported export/rebuild; clearing a bit is never a rollback procedure.

## Session and lineage handoffs

1. **A2 applied keys.** The foundation parses/stores all three keys, applies client_name, and reports timeout/tag in `not_applied` with `reason=implementation_pending`.
   Timeout cancellation activates `statement_timeout_ms`; durable commit groups activate `default_tag`.
   Each activation moves the key into `options` and adds its capability to `features`, preserving the HELLO reply shape.
   A retained value never appears as applied before real semantics exist; activation does not require another handshake.
2. **A9 continuation.** A parent position at/below the fork continues only when its required replay prefix survives, or it is exactly at the fork.
   Otherwise the consumer receives the lineage-bearing resync and rebuilds; a compatible ancestor does not justify skipping missing changes.
   Backups retain the state cut without catch-up history.

## Invariants and verification plan

- Framing: lexical occurrence count alone determines consumption; bounded drains return one reply and never consume a pipelined request as payload.
- Positions: published frontier never crosses an active producer, and a slot's sent/ACK frontier never crosses unsent relevant changes or durable progress.
- Reads: schema, values and version agree on one read boundary; no allocation revision is presented as a committed version.
- Ownership: one live claim per slot; stale claim ACKs are fenced, timeout ignores progress, and takeover cannot lose an already persisted ACK.
- Formats: new bits are declared before dependent bytes are visible, feature activation recovers wholly, and backup lineage describes its actual cut.
- Extension: unknown kinds/keys/trailing fields survive in all three decoders without consuming replies or corrupting known typed columns.

Tests cover repeated/gapped/quoted/malformed parameters and over-bound drains; signed/bignum boundaries and exact JS/CLI output, including both inclusive JSON cutoff boundaries and their immediate neighbors; all error classes/unknown metadata.
They cover every read composition/null case, paused producers/frontier gaps, longer envelopes, unknown types/kinds, quiet and filtered progress, partial socket writes and ACK ceilings.
Slot tests cover restart/filter mismatch/drop, oldest-change timeout, racing takeover/ACK/disconnect, keepalive options and preservation of persisted ACKs.
Format/crash tests cover activation before/after each durable decision/publication, corrupt lineage/journals, restore cuts, repeated restores and retained/missing-history decisions.
Verifier tests reject feature ownership contradictions and damaged records without mutating data.
The implementation gate includes full ctest, MinGW, ASan, TSan, crash and stress jobs plus Go vet/race tests, JS build/tests, CLI tests and independent per-repository reviews.

Linux measurements run only through `~/chunkdb-bench/measure.sh`, under coordinator-reserved windows, with both profiles, paired intervals and that window's noise floor.
Separate budgets measure unused sessions/watches, lexical framing, plain reads versus WITH clauses, ordered-frontier bookkeeping, expanded feed encoding/progress, filters and slot ownership.
Any per-commit metadata cost is measured separately rather than hidden in an aggregate; feature-off paths keep their existing encoding/locking/worker paths.
Pass criterion: no relaxed scenario has paired median below −5% with its 95% interval entirely below −5%; absolute results and window provenance accompany each feature.
