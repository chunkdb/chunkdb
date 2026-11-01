# Checked commit owner state and pages

This pure layout supports the [recovery owner contract](COMMIT_GROUP_RECOVERY.md) for [#136](https://github.com/chunkdb/chunkdb/issues/136).
It covers manifest scope, selected owner state and immutable tree pages, without publishing files or authorizing group replay.
Coverage values, observation capsules, replacement plans and restore provenance require their own checked payload codecs before runtime use.
All numeric fields are little-endian except the explicitly ordered key bytes.
Every reserved byte is zero and every complete extent is exact.

## Context and object references

A manifest scope is exactly 40 bytes: directory identity 16, owner identity 16, role u8 and seven zero bytes.
Both identities are nonzero, and role is standalone 1 or catalog 2.
Standalone admission additionally checks that the directory identity is the store's own root identity.
A catalog table requires the exact checked scope injected by its directory owner; names and paths cannot substitute for that context.
The scope codec does not acquire a catalog gate, initialize an owner or grant writer ownership.

An object reference is exactly 48 bytes: kind u8, seven zero bytes, complete raw bytes u64 and raw SHA-256 bytes 32.
The absent reference has every byte zero; a present reference has a known nonzero kind, nonzero length and nonzero digest.
Kinds are owner root 1, tree page 2, coverage value 3, retained-group value 4, covered-member value 5, observation capsule 6, replacement plan 7, restore provenance 8, certified journal inventory 9 and retained-ID inventory 10.
A reference's allowed kind depends on its field and is checked before object admission.
Owner-root references are exactly 512 bytes, page references are between 92 bytes and 64 KiB, certified journal inventories are exactly 128 bytes, and retained-ID inventories are between 164 bytes and 256 MiB.
Typed value, observation, plan and provenance root references are between 68 bytes and 64 KiB; larger collections use their checked pages rather than an unbounded root object.
Original decision, seal, image and WAL bodies keep their own typed native references and pre-allocation format bounds inside those payloads.
The name derived from its digest is never sufficient: the owner also checks exact length, complete raw digest, native record structure and context.
Referenced journal and ID objects retain their existing checked encodings rather than gaining an unchecked wrapper.

## Owner envelope

Owner roots and pages have this 64-byte header, a kind-specific payload and a final four-byte CRC32 covering the complete header and payload.
The header checksum covers its first 60 bytes.

| Offset | Bytes | Field |
| --- | --- | --- |
| 0 | 4 | `CKGO` |
| 4 | 2 | Tag 1 |
| 6 | 1 | Object kind |
| 7 | 1 | Standalone or catalog role |
| 8 | 8 | Complete bytes including checksums |
| 16 | 16 | Directory identity |
| 32 | 16 | Stable owner identity |
| 48 | 8 | Nonzero creating generation |
| 56 | 4 | Zero |
| 60 | 4 | Header CRC32 |

Header inspection requires exactly 64 input bytes and validates kind, scope, generation, checksum and the kind's byte ceiling before a caller allocates its complete buffer.
Roots are exactly 512 bytes and pages are at most 64 KiB; current session count limits do not enter admission.
Page admission permits an older creating generation only when it is not greater than the checked selecting root's generation.
Root admission requires its exact selected generation when that expectation is supplied.
A pure codec never treats a failed header or final checksum as an interrupted append.

## Selected owner root

The root payload occupies bytes 64 through 507, and its final CRC32 is at byte 508.

| Offset | Bytes | Field |
| --- | --- | --- |
| 64 | 1 | INACTIVE 0, ACTIVE 1 or PENDING 2 |
| 65 | 7 | Zero |
| 72 | 48 | Coverage-tree page reference |
| 120 | 48 | Retained-group tree page reference |
| 168 | 48 | Certified journal inventory reference |
| 216 | 48 | Retained-ID inventory reference |
| 264 | 104 | Reference-tree replay context and cut |
| 368 | 16 | Optional checked counter owner |
| 384 | 8 | Minimum counter reservation ceiling |
| 392 | 48 | Replacement-plan reference |
| 440 | 48 | Exact previous owner-root reference |
| 488 | 20 | Zero |

The 104-byte replay field contains directory identity, journal identity, journal generation u64, complete cursor u64, last group identity, last record kind u8, seven zero bytes and raw prefix SHA-256.
Its context and complete-boundary rules match the checked journal and ID formats.
Its directory agrees with the owner scope, and it is a complete prefix of the selected certified journal cut.
It can be earlier than that cut, and every later admitted decision remains an implicit reference owner until processed.

INACTIVE requires every payload byte after its state and padding to be zero.
ACTIVE requires the four selected references with their exact field kinds and a complete nonzero replay context, and has no replacement plan or previous-root reference.
An empty tree still has a checked empty page reference, rather than an ambiguous absent reference.
A zero counter owner requires ceiling zero; a present counter owner is checked separately against the native counter and cannot change to another owner or decrease its required ceiling in a successor.
A present counter owner cannot become absent even when its current required ceiling is zero.

PENDING carries the complete old selected context plus a replacement-plan reference and exact previous-root reference.
The immutable plan binds the checked previous root and complete final target root by raw length and SHA-256, and separately admits every replacement participant before installation.
The plan's complete payload codec is a runtime prerequisite; parsing a PENDING root alone does not authorize any installation.
Read-only admission cannot choose one of its candidate states or complete the plan.

Successor validation requires identical scope, nonwrapping generation advancement and a permitted state transition.
INACTIVE can become ACTIVE at the next generation only after the caller has durably staged all selected objects, and never becomes PENDING directly.
An ACTIVE root can advance to ACTIVE at the next generation for a selected-object-only publication that changes no canonical artifact.
An ACTIVE root can instead become PENDING at the next generation, with a previous-root reference equal to its complete checked predecessor bytes.
PENDING can only finish as the exact ACTIVE target bound by its checked replacement plan at the following generation.
It cannot be overwritten by another pending plan, republished as INACTIVE or abandoned for an old selected context.
The pure validator checks supplied fully admitted plan facts; it does not certify file sync or decide whether a runtime operation needs a pending plan.
An already installed exact final target is recognized by its raw digest without publishing another successor.

## Ordered tree keys

Coverage keys are 41 bytes: table epoch 16, dimensions u8 and ordered X, Y and Z bytes, eight per coordinate.
The table epoch is nonzero, dimensions are 2 or 3, and a two-dimensional key has Z zero.
An ordered coordinate is its signed two's-complement u64 bits XOR the high bit, then encoded most-significant byte first.
This key encoding sorts negative and positive coordinates in their numeric order without subtraction or overflow.
Retained-group keys are the 16 nonzero group identity bytes.
Covered-member keys are group identity bytes followed by an eight-byte most-significant-byte-first unsigned ordinal.
An ordinal's decision bounds are validated by the covered-member value codec, not guessed from its key.
Tree kinds coverage 1, retained groups 2 and covered members 3 require key widths 41, 16 and 24 respectively.
Leaf value references have the corresponding kinds coverage value 3, retained-group value 4 and covered-member value 5.

## Compressed radix pages

A page payload begins with tree kind u8, node kind u8, key width u8, prefix length u8, entry count u16, zero u16, subtree key count u64 and subtree owned bytes u64.
Its 24-byte prefix is followed by exactly prefix-length key bytes and the node's entries.
Node kinds are empty 0, leaf 1 and branch 2.
The page's complete raw extent follows from this physical entry count and prefix length, with checked arithmetic before allocating owned vectors.
Subtree counts and byte totals are validation claims and are never allocation sizes.

An empty page has zero prefix length, zero entries and zero subtree keys, and no additional payload.
Its subtree owned bytes equal its complete 92-byte page extent.
Empty pages are allowed only as a tree's root; branch children are nonempty.

A leaf has its complete key as prefix, zero entries and exactly one value reference after that key.
Its subtree key count is one and its owned bytes equal its complete page bytes plus the value reference's exact raw length.
The referenced value must subsequently pass its typed context and key-binding codec; a leaf does not make an arbitrary same-length object authoritative.

A branch has prefix length smaller than its key width and between two and 256 child entries.
Each child entry is exactly 72 bytes: edge u8, seven zero bytes, subtree key count u64, subtree owned bytes u64 and a tree-page reference.
Edges are strictly increasing unsigned bytes, each child's claimed key count is nonzero, and its owned bytes are at least its referenced page's raw extent.
The branch subtree count equals the checked sum of child counts.
Its owned bytes equal its complete page extent plus the checked sum of child owned bytes.
Owned-byte totals count these tree pages and their directly referenced typed value capsules only.
Native images, WALs, original decisions, seals and other artifacts referenced inside a capsule are separately charged by their owning subsystem and are not recursively counted as tree disk usage.
Single-child branches are noncanonical and are compressed instead of adding unbounded depth.

When the owner admits a child page, its scope and tree kind agree with the parent and its creating generation is not newer than the selecting root.
The child's prefix begins with the parent's complete prefix followed by the selected edge, and is strictly longer than the parent's prefix.
Its declared subtree totals equal the parent's entry after the child passes complete admission.
This strict prefix growth bounds traversal by the fixed key width and prevents cycles, duplicated key ranges and a child reused through incompatible edges.
Counts and owned-byte arithmetic cannot wrap.
A covered-member tree additionally refuses a fully admitted owned-byte total greater than 256 MiB.
Other tree totals describe directory-sized persistent metadata and do not require materializing that metadata in memory.
Readers use bounded page buffers and a bounded traversal stack, and copy-on-write updates retain immutable root references rather than copying every value.

## Pure internal API

`CommitOwnerScope` owns directory identity, owner identity and role.
`CommitOwnerObjectRef` owns kind, complete bytes and raw digest.
`CommitOwnerRoot` owns scope, generation, state, selected references, reference replay context and cut, counter binding, replacement plan and predecessor reference.
`CommitOwnerPage` owns scope, creating generation, tree kind, node kind, full prefix bytes, subtree totals, one optional leaf reference and its physically bounded child vector.
`CommitOwnerChild` owns edge, subtree totals and exact page reference.

The API serializes and parses the fixed manifest scope, inspects owner headers before complete reads, serializes and parses roots and pages, and creates or decodes the three canonical key encodings.
Separate validators bind selected journal and ID contexts, the reference replay proof, predecessor and target root facts, root page selection and parent-child relations.
Root page selection requires the coverage field's page to have coverage-tree kind and the retained-group field's page to have retained-group kind, despite both references sharing the generic tree-page object kind.
The caller supplies only facts obtained from complete bounded native record admission and raw digest measurement.
The API has no filesystem access, lease acquisition, feature activation, mutable global state, publication callback or fallback replay authority.
New owner runtime starts only after these codecs and the remaining typed value and plan codecs have independent review and regression controls.

## Required checks

Golden encodings pin exact scope, 512-byte roots, empty pages, both-dimensional coverage keys, negative coordinates and branch layouts.
Every truncation cut, unsupported tag or kind, reserved byte, wrong scope or role, future generation and final checksum corruption refuses.
Header tests use checksum-correct oversized declarations and prove refusal before a caller requests a full buffer.
Successor tests reject owner changes, a present counter becoming absent at zero ceiling, wraparound, inactive references, active-to-inactive regression, replaced pending plans and a valid same-length different final target.
Page tests reject mismatched physical counts, wrapped subtree sums, false owned-byte totals, duplicated edges, one-child branches, empty children, wrong prefix relations and wrong value kinds.
Child and selection tests use different valid same-length records to prove raw SHA and context bindings rather than relying on incidental checksum failure.
Omitting each targeted guard must make its corresponding regression fail at runtime with the production sources restored afterward.
