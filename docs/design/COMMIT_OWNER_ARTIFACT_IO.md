# Commit owner artifact I/O

This foundation supports [#136](https://github.com/chunkdb/chunkdb/issues/136), the [recovery owner](COMMIT_GROUP_RECOVERY.md) and the [checked owner formats](COMMIT_GROUP_OWNER_FORMAT.md).
It reads and stages exact immutable root and page objects and reads the fixed canonical owner root.
It does not publish a canonical owner root, activate grouping, install a pending plan or authorize a group from an object checksum.
Complete typed coverage, retained-group, observation, replacement-plan and restore-provenance admission remains a prerequisite for those mutations.

## Scope and native directory ownership

The caller supplies the admitted manifest scope; persisted object references never supply paths.
The canonical root name is `chunkdb.groups.owner` and the object directory name is `chunkdb.groups.objects`.
Each immutable object has exactly the lowercase 64-character hexadecimal encoding of its complete raw SHA-256 as its name.
Temporary names use a reserved private prefix and one generated nonce, with exclusive creation and no overwrite or retry loop on collision.
Only the checked root and page kinds are supported by this foundation; other reference kinds refuse rather than becoming opaque admitted payloads.

Read-only opening pins the actual root directory and refuses a symbolic link or reparse point at that leaf.
A writer additionally borrows an existing exclusive ProcessLock for its entire lifetime and verifies its admitted canonical directory, the pinned native root generation and the native identity of `root/.chunkdb.lock/writer.lock` against that lock.
The fixed `.chunkdb.lock` directory is itself opened relative to the pinned root with no-follow directory admission; matching a lock-file hard link under a different root is insufficient.
A lock for another directory, a missing lock, read-only access or an incompatible shared-writer context cannot create an artifact writer.
ProcessLock retains an actual root-directory native owner captured during acquisition before opening pathname-based lock children; inode numbers without a retained owner cannot protect against directory deletion and reuse.
Post-acquisition admission binds the fixed lock child under that pinned root to the acquired native lock before initialization is reported.
The root owner is close-on-exec or noninheritable, receives failure cleanup and outlives artifact borrowers; this adds startup ownership without ordinary request-path operations or a claim about existing heartbeat pathname races.
The caller retains the real lock until all artifact writers are destroyed; the foundation neither takes over nor releases that lock.

POSIX operations use pinned directory descriptors, `openat` with no-follow and close-on-exec flags, `fstat` regular-file checks and relative native publication names.
Root and object-directory opens require actual directories and refuse redirected child components.
Windows root opening uses OPEN_REPARSE_POINT and BACKUP_SEMANTICS and refuses reparse attributes.
Child directory and file opens use NtCreateFile with the pinned directory as RootDirectory, one fixed or digest-derived relative leaf and native no-reparse/type admission.
Object-directory handles deny delete sharing while a view uses them, and all publication remains relative to those exact native directory objects.
Child file handles allow read, write and delete sharing and validate the opened native regular-file object before reading.
NtSetInformationFile performs no-replace publication from the exact temporary handle with a relative destination in its pinned directory.
These documented native OS entrypoints are resolved from the already loaded ntdll module and require no additional package or link dependency; unavailable capability refuses before mutation without a pathname fallback.
No final-path reconstruction or check after an outside write substitutes for handle-relative admission.
No operation follows a record-supplied path or modifies an existing file in place.

Hard links to plain regular files are allowed.
Reusing an immutable object only reads its existing native generation and proves exact bytes and typed context; it never truncates, changes permissions on or repairs that inode.
A canonical-root read pins the selected file generation across replacement and returns its measured digest and parsed syntax, not publication permission.
A root or object symlink, reparse point, directory, FIFO or other nonregular leaf refuses before body reads or writes.

## Object namespace initialization

The object directory has a fixed `.scope` marker containing the exact 512-byte INACTIVE generation-one root for its manifest scope.
This marker establishes only namespace ownership and is never a selected owner state or an ACTIVE authorization.
The marker's bytes, checksums, identities, role, inactive state and generation must all agree exactly before any digest-named object is opened for staging.
An existing incompatible marker refuses without changing its bytes, any object or any foreign file.

A writer can create the private object directory and sync its name, then exclusively publish and sync the marker and its directory before creating any digest-named object.
An existing exact marker is likewise synced through its opened native file generation and directory before this writer first treats the namespace as durably admitted.
An interrupted empty namespace can be completed only when its native admission finds no object or foreign entry; reserved unpublished marker temporaries can remain untouched.
A missing marker alongside any digest-named object or foreign entry is damage rather than permission to adopt that directory.
An INACTIVE selected root may coexist with an initialized namespace and unreferenced staged objects; those objects do not prove that grouped frames were authorized.
Readers never create or complete a namespace, and reading an INACTIVE canonical root does not require an object directory to exist.
The foundation does not collect unrelated objects or stale temporaries.

## Bounded exact-reference reads

A digest reference is validated for its kind and format extent before opening its derived leaf.
The opened native file is measured through that same handle, which remains owned until the read completes.
The reader first admits the exact fixed header, scope, kind, creating generation and byte bound, then allocates only its bounded complete root or page buffer.
The measured extent must equal both the reference and the complete checked declaration; short reads, appended bytes and a valid different same-length record all refuse.
Raw SHA-256 covers every byte including embedded checksums, and complete typed parsing follows before a result is returned.
A page additionally has its required tree kind and selecting-generation bound checked by the pure codec.
Parent-child, value payload, complete tree, journal, ID and group admission are not inferred from a successful page read.

A canonical-root reader uses the same fixed 512-byte admission and returns the actual raw reference with the parsed root.
INACTIVE, ACTIVE and PENDING syntax can be reported as facts, but this API cannot select one side of a pending plan, replay a group or serve a table through unadmitted references.
The admitted owner supplies the directory and table generation observation bracket before using these facts.

## Immutable staging and durability

Staging accepts typed root or page values, serializes them through the reviewed codec and computes their exact raw reference before changing the namespace.
An INACTIVE staging convenience constructs the fixed generation-one empty root itself, instead of accepting a caller's supposedly empty record.
Staging a syntactically valid ACTIVE root snapshot is still private object preparation and cannot replace the canonical root.

A new object is written through an exclusive owner-private temporary native handle in the admitted object directory.
The writer completes every byte, performs actual file durability on that handle and publishes the digest name without replacement while retaining exact native generation ownership.
It closes that file owner and then syncs the object directory before exposing a durable result.
Newly created directory names and the `.scope` marker receive their own parent-directory durability before objects can be staged.
macOS file sync preserves F_FULLFSYNC and only the existing unsupported-capability fallback; POSIX directory sync and Windows native file and directory sync preserve the established strict error contract.
The implementation reuses native durability helpers on the pinned handles rather than reopening a canonical path to sync another generation.
A small internal declaration can expose those existing native helpers without changing their bodies or ordinary call paths.

If the digest name already exists, the writer admits that exact object and returns the existing reference only after matching its length, digest, complete typed value and context.
Matching bytes and a visible name do not prove prior durability: this foundation always syncs that exact opened file generation and its directory before reporting durable reuse.
Windows writer-only marker, reuse and final-object opens request native read and write rights initially, admit that exact generation and fail closed if sync rights are unavailable.
Readers retain read-only rights, reuse never changes contents or permissions, and no canonical pathname is reopened to acquire sync rights.
A conflicting or redirected existing entry refuses without replacement.
Publication uncertainty records whether this invocation installed the immutable name; a directory-sync failure does not report a durable stage even if the new object is readable.
An exact later retry must re-admit and sync that name before returning success, so a prior failed directory sync cannot disappear as unrecorded debt.
A write, close, sync or publication failure can clean only its own unpublished native generation through a deletion operation that cannot remove a substituted file; otherwise it retains the uncertain name.
An already published immutable object remains available for checked reuse and is not deleted as cleanup after uncertainty.

POSIX publication retains the original temporary descriptor through final-name admission.
It checks the source name's current native binding before an exclusive relative rename or no-replace link, but that precheck alone grants no authority.
After publication it opens the digest leaf without following links and requires its native device and inode, complete extent, raw SHA-256 and typed value to agree with the still-open owned temporary generation before file and directory sync can report a durable result.
A substituted generation returns an error even when the substitute is a valid same-length record, and cannot supply a selected-root reference.
Any leftover entry is unselected namespace work requiring admitted recovery or collection, not an admitted object.
The foundation never unlinks a POSIX replacement based on a racy pathname identity check.
An exclusive rename consumes its original source name itself; a no-replace hard-link fallback retains its private source link for admitted collection rather than performing an unsafe name-based unlink.
On failure, uncertain private or digest names are retained without unlink or replacement, including a substituted sentinel; this API claims no atomic unlink-if-inode operation or protection against arbitrary same-user in-place mutation.

## Publication boundary

This initial API exposes native I/O facts and durable private artifacts only.
It has no generic canonical-root write or compare-and-swap method accepting an arbitrary parsed CommitOwnerRoot.
Canonical INACTIVE installation must join the checked creation or ENABLE FEATURE decision, including its real manifest claim and exclusive writer ownership.
ACTIVE or PENDING publication additionally requires fully admitted typed values, exact selected reference closure and complete replacement-plan authorization.
Canonical publication APIs cannot use an optional callback that declares an unchecked root safe.
Canonical publication requires the existing owner-transition and lifecycle ordering rules.
Grouping stays unused while complete directory recovery and those authorizations are absent.

## Failure checks

Tests preserve the exact canonical-root bytes and unrelated sentinels through every rejected stage and malformed read.
They exercise wrong scopes and roles, absent or conflicting namespace markers, object-directory aliases, reparse leaves, hard links, regular-file substitution and mismatching writer leases.
Replacing a renamed root at its old spelling with a hard link to its original writer lock must still refuse because the retained root generation differs; failed acquisition and borrower destruction must release their native resources.
A valid same-length different root or page under a claimed digest must fail the raw SHA binding independently of native format checksums.
Sparse oversized files refuse from measured extent or fixed-header admission before a complete buffer is allocated.
Native rename and replacement gates prove that reads stay on their opened generation and staging never follows a replaced directory name into foreign files.
An independent native-inode substitution gate replaces a synced POSIX temporary leaf before publication, then requires refusal, no durable reference, unchanged selected-root bytes and an untouched unrelated sentinel.
Crashes cover directory creation, marker file and name sync, object temporary write, file sync, no-replace publication and directory sync, with exact old or complete new immutable bytes afterward.
A first directory-sync failure followed by exact-object reuse must perform the required sync before reporting durability.
Isolated omission controls target no-follow and native type admission, digest and measured-length checks, namespace scope, correct writer ownership and publication ordering.
Native and GCC focused tests precede platform CI, including actual Windows sharing and reparse behavior; no local speed measurements are involved.
