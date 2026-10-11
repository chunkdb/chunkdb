#!/usr/bin/env python3
"""Compare marked Markdown examples with output from the current server and CLI."""
import argparse
from contextlib import ExitStack
import difflib
import os
from pathlib import Path
import re
import selectors
import signal
import socket
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
FENCE = re.compile(r'^```([^\n]*)\n(.*?)\n```', re.M | re.S)
PROMPT = re.compile(rb'chunk\*?> $')


def examples(text):
    fences = list(FENCE.finditer(text))
    result = []
    for i, fence in enumerate(fences):
        tag, code = fence.groups()
        if 'docs-check' not in tag:
            continue
        if i + 1 == len(fences) or fences[i + 1].group(1) != 'output':
            raise ValueError('checked input must be followed by an output fence')
        if text[fence.end():fences[i + 1].start()].strip():
            raise ValueError('checked input and output must be adjacent')
        if re.fullmatch(r'cql docs-check(?: session=[a-z]+)?', tag):
            kind = 'cql'
        elif tag == 'sh docs-check-watch':
            kind = 'watch'
        elif tag == 'sh docs-check-shell':
            kind = 'shell'
        else:
            raise ValueError('unknown example tag: ' + tag)
        session = re.search(r'\bsession=([a-z]+)\b', tag)
        result.append((kind, session.group(1) if session else 'main', code, fences[i + 1]))
    return result


def normalize(text):
    # Only fields with documented volatility are replaced. Numeric data and
    # versions remain exact: each page runs against a fresh data directory.
    text = re.sub(r'^(Generated password: )[^\r\n]+', r'\1<generated-password>', text, flags=re.M)
    text = re.sub(r'^(start |schema )[0-9a-f]{32}(?=:)', r'\1<epoch>', text, flags=re.M)
    text = re.sub(r'(\bepoch = )[0-9a-f]{32}(?=,)', r'\1<epoch>', text)
    text = re.sub(r'^(change revision \d+ time_ms )-?\d+', r'\1<time-ms>', text, flags=re.M)
    text = re.sub(r'^(\d+\) name = [a-z_][a-z0-9_]*, applied_ms = )\d+', r'\1<time-ms>', text, flags=re.M)
    text = re.sub(r'^(server_version = )[^\r\n]+', r'\1<server-version>', text, flags=re.M)
    return text.replace('\r\n', '\n')


class Reader:
    def __init__(self, process):
        self.process = process
        self.pending = b''

    def until(self, predicate, label):
        deadline = time.monotonic() + 20
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ)
            while not predicate(self.pending):
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    raise RuntimeError('timeout waiting for ' + label + ': ' + self.pending.decode(errors='replace'))
                part = os.read(self.process.stdout.fileno(), 65536)
                if not part:
                    raise RuntimeError('process ended waiting for ' + label + ': ' + self.pending.decode(errors='replace'))
                self.pending += part
        result, self.pending = self.pending, b''
        return result.decode()


class Shell:
    def __init__(self, cli, env):
        self.process = subprocess.Popen([str(cli), '--uri', env['CHUNKDB_URI'], 'shell'],
                                        env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT)
        self.reader = Reader(self.process)
        self.reader.until(lambda data: PROMPT.search(data), 'initial shell prompt')

    def run(self, code):
        output = ''
        for line in code.splitlines():
            if not line.strip():
                raise ValueError('checked CQL must not contain empty lines')
            self.process.stdin.write((line + '\n').encode())
            self.process.stdin.flush()
            reply = self.reader.until(lambda data: PROMPT.search(data), 'reply to ' + line)
            output += re.sub(r'chunk\*?> $', '', reply)
        return output

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.write(b'exit\n')
            self.process.stdin.flush()
        try:
            self.process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.communicate()
            raise RuntimeError('CLI shell did not exit')
        if self.process.returncode:
            raise RuntimeError('CLI shell exited ' + str(self.process.returncode))


class Fixture:
    def __init__(self, server, cli, directory):
        self.server, self.cli, self.directory = server, cli, directory
        with socket.socket() as reserve:
            reserve.bind(('127.0.0.1', 0))
            port = reserve.getsockname()[1]
        self.env = os.environ.copy()
        self.env['CHUNKDB_ADMIN_USER'] = 'admin'
        self.env['CHUNKDB_ADMIN_PASSWORD'] = 'docs-fixture-private-password'
        self.env['CHUNKDB_PASSWORD'] = self.env['CHUNKDB_ADMIN_PASSWORD']
        self.env.pop('CHUNKDB_ADMIN_PASSWORD_FILE', None)
        self.env['CHUNKDB_URI'] = 'chunk://admin@127.0.0.1:' + str(port) + '/'
        self.env['EXAMPLE_DIR'] = directory
        self.env['PATH'] = str(cli.parent) + os.pathsep + self.env['PATH']
        self.process = subprocess.Popen([str(server), '--listen-uri', 'chunk://127.0.0.1:' + str(port) + '/',
                                         '--data-dir', directory + '/data', '--backup-dir', directory + '/backups',
                                         '--workers', '2'], env=self.env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self.shells = {}
        self.watch = None

    def start(self):
        # A real PING is the readiness signal. Startup races are bounded by an
        # explicit deadline; they do not weaken any production behavior.
        deadline = time.monotonic() + 20
        while True:
            result = subprocess.run([str(self.cli), '--uri', self.env['CHUNKDB_URI'], 'PING'], env=self.env,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5)
            if result.returncode == 0 and result.stdout == 'PONG\n':
                return
            if self.process.poll() is not None or time.monotonic() >= deadline:
                raise RuntimeError('server did not answer readiness PING: ' + result.stderr)
            if 'connection refused' not in result.stderr.lower():
                raise RuntimeError('server readiness failed: ' + result.stderr)

    def execute(self, kind, session, code):
        if kind == 'cql':
            if session not in self.shells:
                self.shells[session] = Shell(self.cli, self.env)
            return self.shells[session].run(code)
        if kind == 'shell':
            result = subprocess.run(['bash', '-eu', '-o', 'pipefail', '-c', code], env=self.env,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=20)
            if result.returncode:
                raise RuntimeError('example shell failed: ' + result.stdout)
            return result.stdout
        if self.watch is not None:
            raise ValueError('only one active watch example per page is supported')
        self.watch = subprocess.Popen(['bash', '-c', 'exec ' + code], env=self.env,
                                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self.watch_reader = Reader(self.watch)
        return self.watch_reader.until(lambda data: data.startswith(b'start ') and data.endswith(b'\n'), 'watch start')

    def finish_watch(self):
        # Read the complete change, including its after row, before stopping.
        # Checked watches use one-block changes, whose row completes the event.
        output = self.watch_reader.until(lambda data: re.search(rb'change revision [^\n]+\n  block [^\n]+\n$', data), 'watch change')
        self.watch.send_signal(signal.SIGINT)
        tail, _ = self.watch.communicate(timeout=10)
        if self.watch.returncode:
            raise RuntimeError('watch did not stop successfully: ' + tail.decode())
        self.watch = None
        return output + tail.decode()

    def close(self):
        errors = []
        for shell in self.shells.values():
            try:
                shell.close()
            except Exception as error:
                errors.append(str(error))
        if self.watch is not None and self.watch.poll() is None:
            self.watch.send_signal(signal.SIGINT)
            try:
                self.watch.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                self.watch.kill()
                self.watch.communicate()
                errors.append('watch failed to stop during cleanup')
        self.process.terminate()
        try:
            logs, _ = self.process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.communicate()
            errors.append('server failed to stop during cleanup')
        if self.process.returncode:
            errors.append('server exited ' + str(self.process.returncode))
        if errors:
            raise RuntimeError('; '.join(errors))


def check_page(page, server, cli, record=False):
    source = page.read_text()
    blocks = examples(source)
    if not blocks:
        return 0
    with tempfile.TemporaryDirectory(prefix='chunkdb-docs-') as directory, ExitStack() as stack:
        fixture = Fixture(server, cli, directory)
        stack.callback(fixture.close)
        fixture.start()
        results = []
        watcher = None
        for index, (kind, session, code, output) in enumerate(blocks):
            actual = fixture.execute(kind, session, code)
            results.append(actual)
            if kind == 'watch':
                watcher = index
            elif watcher is not None:
                if not re.fullmatch(r'SET BLOCK [^\n]+', code):
                    raise ValueError('a watch example requires one following SET BLOCK')
                results[watcher] += fixture.finish_watch()
                watcher = None
        if watcher is not None:
            raise ValueError('watch requires a following checked change statement')
        replacements = []
        for index, ((kind, session, code, output), actual) in enumerate(zip(blocks, results)):
            actual = normalize(actual.replace(directory, '<example-dir>')).rstrip('\n')
            expected = output.group(2)
            if record:
                replacements.append((output.start(2), output.end(2), actual))
            elif actual != expected:
                difference = '\n'.join(difflib.unified_diff(expected.splitlines(), actual.splitlines(),
                                                          fromfile='expected', tofile='actual', lineterm=''))
                raise AssertionError(str(page.relative_to(ROOT)) + ' example ' + str(index + 1) + '\n' + difference)
        if record:
            for start, end, replacement in reversed(replacements):
                source = source[:start] + replacement + source[end:]
            page.write_text(source)
    return len(blocks)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', type=Path, default=ROOT / 'build/chunkdb_server')
    parser.add_argument('--cli', type=Path, required=True)
    parser.add_argument('--record', action='store_true', help='replace expected output after reviewing current behavior')
    parser.add_argument('pages', nargs='*', type=Path)
    args = parser.parse_args()
    server, cli = args.server.resolve(), args.cli.resolve()
    if not server.is_file() or not cli.is_file() or cli.name != 'chunk-cli':
        parser.error('provide current built server and a CLI binary named chunk-cli')
    pages = args.pages or [ROOT / 'README.md', *sorted((ROOT / 'docs').glob('*.md')),
                           *sorted((ROOT / 'docs/reference').glob('*.md'))]
    count = 0
    for page in pages:
        checked = check_page(page.resolve(), server, cli, args.record)
        if checked:
            print(str(page.resolve().relative_to(ROOT)) + ': ' + str(checked) + ' examples')
        count += checked
    if not count:
        parser.error('no checked examples found')
    print('PASS: ' + str(count) + ' examples from the current build')


if __name__ == '__main__':
    main()
