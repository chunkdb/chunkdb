# Extensible storage in 2.0

The normative [storage reference](../STORAGE_FORMAT.md) defines the current format and [compatibility](../COMPATIBILITY.md) defines the 2.x promise.
The [superseded proposal](../releases/pre-2.0-extensible_format_design.md) is historical.

## Feature admission

The data-directory manifest protects directory-level features and each table manifest protects its own artifacts.
Both carry incompat, ro_compat and compat flag sets.
Unknown incompat refuses opening; unknown ro_compat permits read-only opening only; compat can be ignored.
An artifact's flags must be a subset of its manifest's flags.
An unknown entry can be skipped after bounds/checksum validation only when covered by a feature flag the reader does not know; otherwise it is corruption.
Compatible data must be safe to discard when a writer rewrites an artifact.

## Typed extension points

Manifest options use typed length/value entries.
Images have an ordered section directory, raw sizes and checksums; compression does not change the canonical state checksum.
WAL frames carry typed metadata fields and records, each with explicit bounds.
The schema history translates images and frames from their recorded schema version to the current columns.
Existing fields retain their meaning throughout 2.x.

## Current identities

The directory manifest is CKDM version 1 and the table manifest is CKMF version 4.
Table incompat bit 0 enables slots, writer metadata and collection frames.
Directory incompat bit 1 enables named migrations and their redo journal.
Internal record versions do not identify earlier product releases.
Unsupported old layouts are refused rather than converted in place.
