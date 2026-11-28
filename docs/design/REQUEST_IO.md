# Request I/O

This note covers the network side of the request path for #138: how connections are polled, read, dispatched and answered, what that costs, and the order of the changes that reduce it.
The connection model itself (ownership, admission, cancellation, deadlines) is in [connections](CONNECTIONS.md); every change here keeps its rules.

## Current path

One I/O thread (`IdleIo`) owns connections that are between statements, logging in, or writing a control reply.
It keeps socket interest registered with epoll on Linux and kqueue on macOS and FreeBSD, and reuses a `WSAPoll` array on Windows or a `poll` array on other platforms.
It serves connections with readiness events, due deadlines, buffered input or a state change, and retries connections waiting for queue space or line credit when it wakes.
One feed thread (`FeedIo`) owns watching connections and still scans its watches and rebuilds its interest list for `SocketPoller` on each pass.
A complete statement goes to the pending queue; a free worker takes it, runs it, writes its reply with the connection's ordinary 65536-byte reservation, and keeps the connection while its buffer holds more requests.
After a reply the worker waits once in a receive bounded by `kWorkerReceiveWaitMs` for the next request of a plain POSIX connection, then on a poller of its own until another request needs a worker.
Every read, on the I/O thread or a worker, takes at most 4096 bytes; line credit is charged as the line bound plus that one read.
Every reply is sent before the next buffered request is parsed, so a pipeline of N requests costs N sends.

## Costs

1. The feed thread's per-pass work grows with its watches, and on Windows and other platforms using a poll array, `IdleIo` still scans the array on each wait.
2. One send per reply: pipelined small requests pay one system call each for their replies.
3. 4096-byte reads: a large frame or a long pipeline pays one system call per 4096 bytes.

## Changes, in order

1. Readiness polling with registered interest is implemented for `IdleIo`, with one heap entry per connection that has a deadline.
   Registered interest and deadline scheduling for `FeedIo` remain follow-up work.
2. Gathered replies: while a worker's connection has another complete request buffered and its reply credit allows, the next reply is appended to the unsent one and both go in one send; a reply after an irreversible decision is never held behind a request that could fail, and the output deadline still counts from the first unsent byte.
3. Larger reads: reads take up to the free room of the connection's line credit, and line credit is charged as the line bound plus that read size, so the aggregate line bound still holds.

Each change is its own PR on the request path, measured with the commands below before and after, with absolute numbers, the machine and the run count.

## Method

`chunkdb_server_bench` measures requests/s, blocks/s and p50/p99 per scenario; `--idle-connections N` adds N connections that log in before each scenario and stay silent until it ends.
Spawn mode, from a Release build:

```
chunkdb_server_bench --server-mode spawn --port 0 --tests ping,set,get,world,canvas,simulation --clients 50 --server-workers 50 --requests 200000 --pipeline 1 --output json
chunkdb_server_bench --server-mode spawn --port 0 --tests ping,set,get,world,canvas,simulation --clients 50 --server-workers 50 --requests 200000 --pipeline 16 --output json
chunkdb_server_bench --server-mode spawn --port 0 --tests ping,set,get,world,canvas,simulation --clients 50 --server-workers 50 --requests 200000 --pipeline 16 --idle-connections 5000 --output json
```

The third run needs a descriptor limit above 10100 for the bench process (`ulimit -n`), since it holds both ends of every connection.
Change 1 is judged by the third run against the second, change 2 by the second against the first, and change 3 by the `set` and `world` rows.

## Measurements

Each run is recorded here with its numbers, machine, build and run count before the change it measures merges.
Paired results for registered readiness polling on `IdleIo` are pending.
