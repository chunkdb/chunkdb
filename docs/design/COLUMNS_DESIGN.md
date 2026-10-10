# Typed columns and schema history in 2.0

The [CQL reference](../CQL.md) defines supported types and conversions; the [storage reference](../STORAGE_FORMAT.md) specifies their encoding.

## State layout

Each column has a permanent id, name, type, null/required flags and optional default.
Fixed-width values are column-major packed arrays with optional validity bitmaps; presence is a separate bitmap.
Text and bytes values use ordered VARS entries keyed by column id and block index.
Absent blocks, NULL values and padding use canonical zero bits.
Tables retain at least one fixed-width column and bound total fixed payload and variable bytes.

## Schema evolution

The version-4 manifest stores current columns and the history needed to rebuild previous schemas.
Images and WAL frames identify their schema version; loading translates values by stable column ids and recorded conversions.
New columns receive their default, null, or zero/empty value as their flags require during translation; dropped columns disappear and renames preserve identity.
ADD of a required column needs a default for existing blocks.
Widening and explicit conversion policies publish one new schema version atomically.
Narrowing without conversion first publishes a temporary constraint, validates stored values and then commits or removes the constraint.
Writer restart removes an interrupted narrowing constraint without committing its proposed type change.

## Wire form

Chunk transfers carry revision, schema version, presence, fixed payload and variable entries.
SET CHUNK refuses a mismatched schema before changing data.
Typed SET BLOCK fills unspecified columns according to defaults/nullability and validates all supplied values before mutation.
Whole-chunk writes include their text/bytes values; they do not depend on an additional metadata API.
