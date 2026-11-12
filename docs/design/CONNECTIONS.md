# Connections and statement admission

This note defines the ownership and cancellation invariants for #137.
The [concurrency rules](CONCURRENCY.md) and [durability contract](../DURABILITY_CONTRACT.md) continue to govern irreversible publication.

## Ownership

Each connection owns its socket, TLS state, input buffer, session and transaction independently of any worker stack.
Exactly one owner may access its socket or TLS object at a time.
An I/O owner handles idle sockets, partial input, TLS handshakes, authentication replies and output readiness.
A statement worker receives a fully framed request and returns the resulting output continuation.
The worker that wrote a reply waits for that connection's next request while no queued request needs a worker; a queued request, KILL, shutdown and the idle deadline return or close the connection, and incomplete input returns it to the I/O owner.
On a plain POSIX socket that wait starts with one blocking receive bounded by 2 ms, the single system call per request that main's worker makes; the socket stays blocking with 2 ms receive and send timeouts, set once, while the worker keeps the connection, and is nonblocking again before the I/O owner gets it back.
No wake reaches that receive: when every worker waits on a quiet connection, a queued request waits at most 2 ms plus the kernel's timer rounding, or for the one statement such a worker has just received, and KILL and shutdown take effect within the same bound; on main a connection without a worker waits without bound.
After the bound the worker waits on its poller, which a yield, KILL or shutdown wakes at once, so a client pausing longer than 2 ms between requests costs one timed-out receive and a poller wait.
A send from a worker that keeps the connection blocks for at most 2 ms, and an unfinished reply continues on the I/O owner.
TLS connections and Windows wait only on the poller: OpenSSL keeps record state across reads that a timed-out blocking read would have to resume, and a Windows socket is indeterminate after a receive times out.
Buffered complete requests retain their worker while bounded input and output credit remains available.
Ownership transfers publish state before waking the receiving owner.
Returned credit wakes the I/O owner only after a refused reservation.
A worker wakes the I/O owner after taking a queued request only when a dispatch was refused for queue space since the previous one.
The receiving owner drains wake notifications before scanning published state, so a coalesced wake cannot hide work; it reads the wake channel only after a wait reported it, and a later wake makes the next wait return at once.
Descriptors stay registered until the closing owner has finished every outstanding operation.
Queued work is discarded before execution on close; running pre-decision work accepts cancellation, while decision-entered work completes before teardown.
The final owner releases transaction snapshots, private writes, history registrations, slot claims and authentication tickets exactly once after that completion.
Shutdown requests closure and wakes owners before joining them.

HELLO, AUTH and session-control requests have reserved admission independent of occupied statement workers.
Partial TLS records preserve the required read/write readiness and their absolute transport deadline.
A pending TLS write pins both buffer and length across retries.
Failed-login delays and ban replies use scheduled deadlines and never sleep on a statement worker.
Per-connection and aggregate input/output bounds stop reading before accepting unbounded pipelines.
Framing plans inspect published schema and rights metadata without taking a table lease; execution revalidates them under its normal lease.
Before retaining parameter bodies, a request reserves its complete checked maximum payload against a shared byte budget without allocating that maximum.
Data framing has 2147483648 bytes of credit and authentication/session controls have a separate 16777216-byte reserve.
The reservation is the smaller of the sum of checked frame maxima and the statement-wide bound, with separately bounded opaque frames.
Admission refuses an individual request exceeding its pool instead of waiting for credit it can never obtain.
Ready requests wait in FIFO order for credit and reconsider admission on returned credit, a queue-head change or an absolute transport deadline.
The wait suspends the request's frame deadline, which restarts when its credit is granted; after `--client-io-timeout-ms` of waiting it receives `BUSY reason=input_budget` and closes, because its unread frames follow the line.
Removing a cancelled or closed waiter immediately pumps its successor even when the removed waiter held no credit.
Reserving a complete request prevents multiple incomplete bodies from exhausting all credit while each waits for another frame.
Credits remain charged while a worker owns the parameters and are released once on completion, refusal or final close.
A worker waiting for its connection's next request keeps that connection's data line credit and ordinary 65536-byte reply reservation, and returns both when it hands the connection back with no reply or line in progress.
Request-line buffers obey their negotiated line bound and a separate aggregate byte bound; reserved control capacity remains available while data framing is paused.
Unclassified request lines use reserved line credit, so data-only credit cannot prevent receiving a new HELLO or control line.
An idle connection on the I/O owner holds no line credit: a read that buffers nothing returns the credit it took, and a writable reply is not taken for input.
Replies charge their retained immutable state and encoded buffers against 2147483648 bytes of shared output credit.
Authentication and control replies use a separate 16777216-byte output reserve.
Execution reserves a checked conservative state, schema and encoding bound before collecting a large result snapshot.
Every statement reserves its bounded ordinary reply before execution and may upgrade that reservation before large snapshot capture or backup publication.
Credit refusal before execution returns BUSY; a reply lost after an irreversible decision closes the connection as an ambiguous outcome instead of claiming a retryable refusal.
The connection retains that reservation through the final encoded buffer even when the continuation has already finished.
SHOW CONNECTIONS reserves its complete response ceiling before enumeration and refuses a listing exceeding that ceiling.
An ordinary result that cannot obtain output credit refuses snapshot capture and returns BUSY before any response byte.
Unclassified and reserved control line limits grow only when a larger configured per-line limit requires it.

## Cancellation and deadlines

A statement progresses through queued, running, decision-entered and completed states.
Cancellation, timeout and irreversible decision admission arbitrate under the same statement state lock.
Only queued or running work can accept cancellation.
Every durable mutation checks cancellation before its first irreversible decision under the locks protecting that decision.
`CANCELLED` and statement `TIMEOUT` mean that the statement did not apply.
Decision-entered work completes with its real result, including existing ambiguous-outcome errors.
No cancellation check follows an irreversible decision merely to replace its result.
Read-only work checks cancellation at bounded traversal and lock-admission boundaries.
Chunk admission keeps explicit reader, exclusive-owner and queued-writer state under one mutex and waits on a stop-aware condition.
An uncontended acquisition checks that state before registering any stop callback; queued writers prevent new readers from overtaking them.
Wait registration, predicate evaluation and ownership release share that mutex, so sleeping cannot lose a release notification or depend on a spurious native try-lock failure.
A release notifies only while a thread waits; that count changes under the same mutex.
Statement admission uses cancellable overloads and adopts ownership only after checking the accepted stop; cancellation releases any concurrently acquired ownership before returning a not-applied result.
Cleanup and background paths keep uncancellable lock operations, and the Windows serial-reader configuration continues to exclude simultaneous readers.
Cancelling one request never carries over to the next request on that connection.

`--statement-timeout-ms` defaults to zero, which disables the timeout.
HELLO `statement_timeout_ms` overrides the default, including explicit zero.
Its checked monotonic interval starts when a complete request is admitted and includes queue wait.
Handshake, partial-input and output deadlines remain independent transport deadlines.
Large replies validate their total bound before their first byte and encode bounded continuations after releasing storage locks.
A reply continuation owns immutable chunk state, schema and projection until its final byte; no borrowed mutable storage spans survive lock release.
Output progress resets the progress deadline; stalled output closes the connection.
A lost connection after a commit can still leave the outcome unknown to its client.

## Session controls

`SHOW CONNECTIONS` returns bounded entries with exact connection ids, user, peer, state and statement verb.
The listing either returns every authorized entry within the response bound or returns an explicit limit error before output; it never silently truncates.
A user sees and controls only sessions from the same live user incarnation; ADMIN on `*` may inspect and control all sessions.
Names alone do not identify an incarnation after DROP and CREATE.
Authorization uses currently published rights at control admission.
`CANCEL id` returns `{cancelled: bool}`; missing, idle and decision-entered targets return false.
`KILL CONNECTION id` returns `{killed: bool}` and requests closure without discarding a decision-entered result.
Self-CANCEL returns false and self-KILL sends its control reply before closing.
KILL discards a pending read-only reply and releases its retained state immediately; only an irreversible outcome and the self-KILL acknowledgement retain output ownership until their reply ends.
WATCH has completed its initiating statement, so CANCEL returns false and KILL uses the existing accepted-ACK teardown.
A slot takeover enters its irreversible decision before publishing its ownership reservation, because that reservation changes how the old owner can return its connection.
Kill, cancel and feature activation emit audit events without statement parameters or credentials.

## User limits

`ALTER USER u LIMIT max_connections = n, statements_per_sec = n` accepts either key once and rejects unknown, duplicate or invalid keys before publication.
Zero disables a limit and the maximum accepted rate is 65536 statements per second.
Each user incarnation owns its authenticated connection count and rate history under a separate lock.
Authentication rechecks the same live incarnation and verifier generation before taking one connection ticket; a verifier or channel binding requirement change advances that generation.
Every ticket is released exactly once, including failed final HELLO output and user deletion.
Lowering a connection limit preserves existing connections and refuses new admissions until below the limit.
A rolling one-second rate window retains unexpired admissions when a limit is lowered.
CANCEL, KILL and ROLLBACK are exempt so rate limiting cannot prevent stopping work.
A refusal consumes the complete request, returns `RATE_LIMITED` with a reason and checked `retry_after_ms`, and preserves transaction state.

The checked users record appends mandatory little-endian `max_connections:u64` and `statements_per_sec:u64` to each user before the whole-file checksum.
Earlier development users layouts are refused before permission or recovery bookkeeping changes.
All writers validate field widths, verifiers, rights and limits before replacing a users file.
Backup, restore, migrations, verification and offline password reset use that same codec.

## Verification

Deterministic ownership hooks cover worker release, cancellation/decision arbitration, output authorization and descriptor teardown.
Regression tests prove both sides of every cancellation boundary and reopen durable state after failure.
Raw and TLS tests fill statement workers while new HELLO and control requests complete, exercise partial records and pipelined parameters, and close idle transactions on shutdown.
Waiting-worker tests cover a queued request taking the worker, KILL, shutdown, the idle deadline, partial input and statement timeouts.
Receive tests hold the worker just before its bounded receive while a request is queued, the connection is killed or the server stops, and check the socket mode and timeouts a connection carries while kept and after it is handed back.
Large-reply tests check exact framing, bounded encoded buffers, limit refusal before output and progress deadlines.
Paused output races concurrent writes and ALTER to prove a fixed reply snapshot and bound.
KILL and shutdown tests cover idle transactions and each COMMIT decision phase, verifying history and slot release without undoing a committed decision.
An oversized session listing must explicitly refuse instead of hiding entries.
User tests cover drop/recreate during authentication, admission races, rate-window boundaries, unchanged limits and exact ticket release.
The complete platform suites and independent review precede review readiness.
Hot-path budgets use paired runs and same-window noise controls on the measurement server, with scheduling and enabled-limit costs measured separately.
