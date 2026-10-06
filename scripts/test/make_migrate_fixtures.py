#!/usr/bin/env python3
"""Regenerates the chunkdb_migrate fixtures in tests/fixtures/migrate.

The fixtures are data directories written by real old servers, so the
converter is tested against what those servers put on disk, not against our
own idea of it. Build the servers from the tags first, for example:

    git worktree add /tmp/c011 v0.1.1-preview && cmake -S /tmp/c011 -B /tmp/c011/b \
        -DCHUNKDB_WITH_TLS=OFF -DCHUNKDB_BUILD_TESTS=OFF && cmake --build /tmp/c011/b --target chunkdb_server

and the same for v1.0.0, v1.3.0 and e6e4a42 (the unreleased storage format of
the 2.0 development line). Then:

    scripts/test/make_migrate_fixtures.py --v011 <server> --v100 <server> \
        --v130 <server> --dev <server> --out tests/fixtures/migrate

Writes, for each fixture, the source directory `<name>/` and
`<name>.expected`: one line per populated chunk, `<cx> <cy> <payload_bits>|<presence_bits> <revision>`,
as the newest server that wrote the directory reads it after startup
recovery. <revision> is the persisted revision, or `-` for a chunk without
one (its token changes on every load).
"""

import argparse
import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time

GEOMETRY = ["--block-bits", "4", "--chunk-width", "4", "--chunk-height", "4",
            "--large-chunk-width", "2", "--large-chunk-height", "2"]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    def __init__(self, binary, data_dir, extra=(), env=None):
        data_dir = os.path.abspath(data_dir)
        self.port = free_port()
        args = [binary, "--host", "127.0.0.1", "--port", str(self.port), "--no-auth",
                "--data-dir", data_dir, "--log-level", "error", "--workers", "2",
                "--wal-group-commit-updates", "1", "--checkpoint-updates", "3", *GEOMETRY, *extra]
        self.proc = subprocess.Popen(args, env={**os.environ, **(env or {})},
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 10
        while True:
            try:
                self.sock = socket.create_connection(("127.0.0.1", self.port), timeout=10)
                break
            except OSError:
                if time.time() > deadline or self.proc.poll() is not None:
                    self.proc.kill()
                    self.proc.wait(timeout=20)
                    raise RuntimeError(f"{binary} did not start")
                time.sleep(0.05)
        self.buf = b""

    def _line(self):
        while b"\n" not in self.buf:
            data = self.sock.recv(65536)
            if not data:
                raise RuntimeError("connection closed")
            self.buf += data
        line, self.buf = self.buf.split(b"\n", 1)
        return line.rstrip(b"\r").decode()

    def _reply(self):
        line = self._line()
        if line.startswith("$"):
            n = int(line[1:])
            while len(self.buf) < n + 2:
                self.buf += self.sock.recv(65536)
            body, self.buf = self.buf[:n], self.buf[n + 2:]
            return body
        if line.startswith("*"):
            return [self._reply() for _ in range(int(line[1:]))]
        if line.startswith("-"):
            raise RuntimeError(line)
        return line[1:]

    def cmd(self, line, payload=None):
        data = (line + "\r\n").encode()
        if payload is not None:
            data += payload + b"\r\n"
        self.sock.sendall(data)
        return self._reply()

    def stop(self):
        self.sock.close()
        self.proc.send_signal(signal.SIGTERM)
        self.proc.wait(timeout=20)

    def kill(self):
        self.sock.close()
        self.proc.kill()
        self.proc.wait(timeout=20)


def bits(*values):
    return "".join(format(v, "04b") for v in values)


def session(binary, data, actions, extra=(), env=None, crash=False):
    """Runs `actions(server)`, then stops the server (kills it with `crash`)."""
    s = Server(binary, data, extra, env)
    try:
        actions(s)
    finally:
        if crash:
            s.kill()
        else:
            s.stop()


def write_v1x(args, data):
    # 0.1.1-preview: image v1 (no presence) and WAL v2.
    def v011(s):
        for i in range(5):
            s.cmd(f"SET {i % 4} {i // 4} {bits(i + 1)}")  # chunk (0,0): image v1, then WAL
        s.cmd(f"SET -1 -1 {bits(9)}")                      # chunk (-1,-1): WAL v2
        s.cmd(f"SET 13 9 {bits(15)}")                      # chunk (3,2): WAL v2
    session(args.v011, data, v011)

    # 1.0.0: image v2 with presence, WAL v3, UNSET, CHUNKSET STATE, MSET.
    def v100(s):
        s.cmd("UNSET 1 0")
        s.cmd(f"SET 2 2 {bits(7)}")
        for i in range(4):
            s.cmd(f"SET -{i + 1} -3 {bits(i + 4)}")        # chunk (-1,-1): image v2
        s.cmd("CHUNKSET 1 0 STATE " + bits(*(v % 16 for v in range(1, 17))) + "|" + "1010000000000101")
        s.cmd(f"MSET 16 -20 {bits(1)} 17 -19 {bits(2)} 18 -18 {bits(3)}")  # chunk (4,-5)
    session(args.v100, data, v100)

    # 1.3.0 with zrle images: CAS, BATCH, binary writes; then a crash.
    def v130(s):
        for i in range(4):
            s.cmd(f"SET {12 + i} {9 + i % 2} {bits(i + 1)}")  # chunk (3,2): image v3
        ver = s.cmd("CHUNKVER 1 0").decode()
        s.cmd(f"CHUNKCAS 1 0 {ver} STATE " + bits(*([5] * 16)) + "|" + "1111000000001111")
        ver = s.cmd("CHUNKVER 4 -5").decode()
        s.cmd(f"CHUNKBATCH 4 -5 {ver} SET 19 -17 {bits(6)} UNSET 16 -20")
        s.cmd("CHUNKSETBIN 6 6 8", bytes([0x12, 0x34, 0, 0, 0x56, 0, 0, 0x78]))
        s.cmd(f"SET 3 3 {bits(12)}")                       # chunk (0,0): WAL records
    session(args.v130, data, v130, ["--checkpoint-compression", "zrle"], crash=True)


def write_dev(args, data):
    # The 2.0 development format over the 1.x directory: v4 headers in the
    # middle of 1.x WALs, frames with revisions, v4/v5 images.
    def dev_plain(s):
        s.cmd(f"SET 0 1 {bits(10)}")                       # chunk (0,0): mid-stream v4 header
        ver = s.cmd("CHUNKVER -1 -1").decode()
        s.cmd(f"CHUNKCAS -1 -1 {ver} STATE " + bits(*([3] * 16)) + "|" + "0000111100001111")
        for i in range(4):
            s.cmd(f"SET {28 + i % 2} {-32 + i // 2} {bits(i + 2)}")  # chunk (7,-8): image v4
    session(args.dev, data, dev_plain)

    def dev_zrle(s):
        for i in range(4):
            s.cmd(f"SET {-8 + i % 2} {20 + i // 2} {bits(i + 6)}")   # chunk (-2,5): image v5
        s.cmd(f"SET 30 -30 {bits(1)}")                     # chunk (7,-8): WAL v4 over image v4
    session(args.dev, data, dev_zrle, ["--checkpoint-compression", "zrle"])

    # A crash inside a conditional write leaves a rollback intent.
    def dev_crash(s):
        ver = s.cmd("CHUNKVER 3 2").decode()
        try:
            s.cmd(f"CHUNKBATCH 3 2 {ver} SET 12 8 {bits(4)}")
        except RuntimeError:
            pass
        s.proc.wait(timeout=20)
    session(args.dev, data, dev_crash, env={"CHUNKDB_FAILPOINT_CRASH_CONDITIONAL_BEFORE_COMMIT_PUBLISH_ONCE": "1"},
            crash=True)


def dump(binary, source, extra=()):
    """Chunk states and revision kinds as `binary` reads a copy of `source`."""
    with tempfile.TemporaryDirectory() as tmp:
        copy = os.path.join(tmp, "data")
        shutil.copytree(source, copy, symlinks=True)
        tokens = []
        for _ in range(2):
            s = Server(binary, copy, extra)
            coords, cursor = [], None
            try:
                while True:
                    reply = s.cmd("CHUNKSCAN 1024" + (f" {cursor[0]} {cursor[1]}" if cursor else ""))
                    head = reply[0].decode()
                    coords += [tuple(map(int, item.decode().split())) for item in reply[1:]]
                    if head == "END":
                        break
                    cursor = head.split()[1:]
                states = {c: s.cmd(f"CHUNK {c[0]} {c[1]} STATE").decode() for c in coords}
                tokens.append({c: s.cmd(f"CHUNKVER {c[0]} {c[1]}").decode() for c in coords})
            finally:
                s.stop()
        lines = []
        for c in sorted(states):
            revision = tokens[0][c] if tokens[0][c] == tokens[1][c] else "-"
            lines.append(f"{c[0]} {c[1]} {states[c]} {revision}\n")
        return "".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    for name in ("v011", "v100", "v130", "dev"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    for name in ("v1x", "dev"):
        shutil.rmtree(os.path.join(args.out, name), ignore_errors=True)
    v1x = os.path.join(args.out, "v1x")
    write_v1x(args, v1x)
    dev = os.path.join(args.out, "dev")
    shutil.copytree(v1x, dev)
    # A crash in the middle of a 1.x record append: a torn tail.
    with open(os.path.join(v1x, "L_2_-3", "C_4_-5.wal"), "ab") as f:
        f.write(b"DLT1\x00\x00\x00")
    write_dev(args, dev)
    for name, reader in (("v1x", args.v130), ("dev", args.dev)):
        shutil.rmtree(os.path.join(args.out, name, ".chunkdb.lock"), ignore_errors=True)
        with open(os.path.join(args.out, f"{name}.expected"), "w") as f:
            f.write(dump(reader, os.path.join(args.out, name)))


if __name__ == "__main__":
    main()
