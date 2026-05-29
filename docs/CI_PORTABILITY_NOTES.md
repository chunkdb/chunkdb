# CI & Test Portability Notes

This document records portability traps that have caused green-on-macOS /
red-on-Linux (and Windows) CI, and the rules that prevent them from recurring.
Read this before writing or changing networking, timing, or filesystem tests.

## Why this exists

Several integration tests passed on the author's macOS machine but failed
deterministically on Linux CI (ubuntu-latest) and on Windows. The root causes
were always the same class of mistake: **a test assumed OS-specific behavior
of sockets, timers, or buffers.** Each was reproduced on a real Ubuntu 22.04
host and root-caused before fixing. The fixes did not change the server; they
removed unwarranted assumptions from the tests.

## Rule 1 — Never assume OS socket-buffer sizes

`TestSlowResponseDrainDeadlineReleasesWorker` set the client `SO_RCVBUF=1024`
and a large (~8 MB) response, betting that the server would block on write and
hit its write deadline. That bet holds on macOS but not on Linux:

- Linux TCP send-buffer autotuning (up to `net.core.wmem_max`, 32 MB on some
  hosts) can absorb the whole response, so the server never blocks and the
  write deadline never fires (the connection closes later via the idle-read
  deadline instead).
- After the server closes a connection that still has megabytes buffered, a
  graceful close (drain + FIN) must deliver all buffered bytes first. Through a
  1024-byte receive buffer the delivery is so throttled that the client does
  **not** observe the close for tens of seconds (measured: >30 s while
  continuously draining).

**Do:** assert the observable invariant (the other client still gets served;
the server logs the termination). **Don't:** assert that a peer observes a TCP
close within a fixed, short window, and don't assume which I/O phase
(`phase=write` vs `phase=read`) the deadline fires in — that depends on buffer
autotuning. Accept either.

## Rule 2 — Don't assert single-worker scheduling outcomes

`TestPendingQueueSaturationRejectsNewConnections` asserted *exactly one*
"queue full" warning and a fixed served/rejected split among the excess
connections. With one worker, the accept loop can drain the single pending
slot between two rejections, producing a second (correct) overload warning;
and which excess connection is served depends on how the worker and accept
loop interleave — which varies by core count (single-core VPS vs. multi-core
runner).

**Do:** assert robust invariants — at least one rejection was logged, every
connection is accounted for, at least one was not served, and the server still
serves a fresh client afterward. **Don't:** assert exact counts that depend on
thread scheduling.

## Rule 3 — In a saturation/timeout test, sends may legitimately fail

When the server is expected to reset/close connections (queue saturation,
deadline termination), a client `send()` may return `EPIPE`/`ECONNRESET`.
Wrap such sends so the test does not abort with an uncaught exception. A reset
in these scenarios is expected behavior, not a test failure.

## Rule 4 — `connect()` succeeds before `accept()` on Linux

On Linux a TCP `connect()` to a listening socket completes (via the listen
backlog) as soon as the handshake finishes, regardless of whether the server
has called `accept()` yet — so a client the server will later reject still
*connects successfully*. Tests that count "rejected = failed to connect" are
wrong on Linux; detect rejection by the absence of a reply / a subsequent
close, not by a failed `connect()`.

## Rule 5 — Never put side-effectful calls inside `assert(...)`

`assert(expr)` expands to nothing when `NDEBUG` is defined (Release builds), so
any function call inside it is **not executed**. Past breakage: `assert(bind(...) == 0)`
and `assert(client.ReadLine() == "+OK")` silently skipped the `bind`/`ReadLine`
in Release. Always run the call first, store the result, then assert on the
result. The smoke suite currently builds in Debug on CI, so this is latent —
keep it that way by following this rule.

## Rule 6 — File-descriptor budget vs. `ulimit -n`

Default `ulimit -n` on Linux CI runners is 1024. The server's WAL-stream pool
plus per-connection sockets (`max_pending_clients`, default 1024) can exceed
that. The server now auto-fits `max_open_wal_streams` and `max_pending_clients`
to the real fd budget at startup (see `src/main.cpp`) and logs any adjustment.
When configuring high `max_pending_clients` for load, raise `ulimit -n`
accordingly (e.g. `ulimit -n 65535`).

## How to reproduce CI locally

CI uses Debug builds (no `CMAKE_BUILD_TYPE`), so `assert` is active:

```bash
# Linux/macOS, no TLS (matches "Build and Test")
BUILD_DIR=build CHUNKDB_WITH_TLS=OFF scripts/test/quick.sh

# With TLS (matches "Build and Test TLS")
BUILD_DIR=build CHUNKDB_WITH_TLS=ON scripts/test/quick.sh

# Flake guard, as CI runs it on ubuntu
ctest --test-dir build -R server_integration --repeat until-fail:5 --output-on-failure
```

If a test passes locally on macOS but you cannot explain why it would pass on
Linux, reproduce it on a Linux host before merging. Timing/socket tests must be
green on every target platform, not just the development machine.

## Known-open: Windows "Benchmark Snapshot" step

The Windows job's `Benchmark Snapshot` step (running `chunkdb_bench` and
`chunkdb_server_bench` with 50 concurrent clients) has been failing since
before these fixes; the Windows **smoke gate** passes. This step generates
performance numbers, not correctness signals. It needs a Windows/MinGW
environment (or the CI run logs) to diagnose — it cannot be reproduced from
Linux/macOS. Until then it is the one remaining red check.
