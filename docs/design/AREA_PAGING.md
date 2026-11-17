# Configured limits and area pages

This note specifies the limit and paging portion of [#136](https://github.com/chunkdb/chunkdb/issues/136).
The directory configuration owns `--max-area-chunks`, `--max-area-response-bytes`, `--txn-max-written-chunks` and `--txn-max-read-chunks`.
Their positive defaults are 4096 requested cells, 64 MiB, 1024 written chunks and 8192 read chunks.
The same immutable values reach direct store admission, transaction bookkeeping and the corresponding HELLO limits, including its existing top-level area fields.
Current request admission uses these values; checked historical records remain bounded by their admitted extent and structural validation rather than current request limits.

GET AREA without LIMIT or AFTER retains its array reply and rejects a selector exceeding its configured requested-cell or encoded-response bound.
LIMIT or AFTER selects a page map containing `chunks`, `more` and `after`.
The chunks retain the existing coordinate-plus-form entry layout and complete chunk metadata under projection.
The page limit defaults to `max_area_chunks` and an explicit limit must be positive and no greater than that bound.
AFTER has exactly the table's two or three axes and names a cell inside the selector; zero in the third axis remains explicitly three-dimensional.
Enumeration follows ascending x, y and z chunk coordinates and resumes strictly after that cell.
Range and Euclidean-ball selectors use checked coordinates and clipping at signed endpoints without enumerating an unbounded bounding box.

Each page bounds visited requested cells, including empty cells, separately from returned populated chunks.
When more cells remain, `after` names the last completely admitted visited cell; a completed page sets it to null.
An empty page can advance across empty cells, while an unfinished page must advance its cursor.
Encoded reply accounting includes the map, array, cursor, coordinates and complete checked forms before appending them.
A pending candidate that fits a page by itself but not the current page remains after the returned cursor and is reconsidered by the next independent request.
A candidate that cannot fit an otherwise empty page produces an error without returning a partial reply or advancing past that candidate.
Retained chunk state, metadata, variable values and encoded output have explicit owners; incremental admission does not first accumulate an unbudgeted collection of forms.

Independent pages do not imply one snapshot or an unchanged table between requests.
Within a transaction each page uses the original snapshot and private overlays, and successful statement publication adds its complete observed coordinate set atomically to the configured read bound.
Byte or count refusal leaves private transaction data and bookkeeping unchanged.
Client helpers preserve the cursor privately, validate page shape and active arity, and apply negotiated bounds before allocating aggregate results.
Regression cases cover exact byte and count boundaries, empty prefixes, cursor progress, negative and extreme coordinates, both dimensions, projected metadata and transaction refusal ownership.
