#!/usr/bin/env python3
"""Run the named shell fences in QUICK_START against an owned Docker fixture."""
import argparse
import json
import os
from pathlib import Path
import re
import selectors
import signal
import shlex
import subprocess
import tempfile
import time
import uuid

BLOCKS = ('start', 'health', 'login', 'write', 'read', 'watch', 'watch-update',
          'reset', 'reset-login')


def extract(text):
    blocks = {}
    pattern = r'<!-- docker-quickstart:([\w-]+) -->\s*```sh\n(.*?)\n```'
    for match in re.finditer(pattern, text, re.S):
        name, code = match.groups()
        if name in blocks:
            raise ValueError('duplicate quick-start block: ' + name)
        blocks[name] = code
    if set(blocks) != set(BLOCKS):
        raise ValueError('quick-start blocks differ: ' + str(set(blocks) ^ set(BLOCKS)))
    return blocks


class Check:
    def __init__(self, args):
        self.args = args
        self.blocks = extract(args.page.read_text())
        for name in ('start', 'reset'):
            if self.blocks[name].count('ghcr.io/chunkdb/chunkdb:2.0.0') != 1:
                raise ValueError('expected published image changed in page block: ' + name)
        self.container = 'chunkdb-page-' + uuid.uuid4().hex[:12]
        self.volume = self.container + '-data'
        self.env = os.environ.copy()
        self.env['PATH'] = str(args.cli.resolve().parent) + os.pathsep + self.env['PATH']
        if args.cli.name != 'chunk-cli':
            raise ValueError('--cli must be named chunk-cli')
        self.port = None
        self.secrets = []
        self.passed = []
        self.log = args.logs / 'commands.log'
        self.watch = None

    def redact(self, text):
        text = re.sub(r'(Generated password: )[^\r\n]+', r'\1<redacted>', text)
        for secret in self.secrets:
            text = text.replace(secret, '<redacted>')
        return text

    def record(self, text):
        with self.log.open('a') as output:
            output.write(self.redact(text) + '\n')

    def run(self, command, check=True, timeout=60):
        self.record('$ ' + (command if isinstance(command, str) else ' '.join(command)))
        result = subprocess.run(command, shell=isinstance(command, str),
                                executable='/bin/bash' if isinstance(command, str) else None,
                                env=self.env, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=timeout)
        self.record(result.stdout)
        if check and result.returncode != 0:
            raise RuntimeError('command failed (see commands.log): ' + str(result.returncode))
        return result

    def code(self, name):
        code = self.blocks[name].replace('chunkdb-quickstart-data', self.volume)
        code = code.replace('chunkdb-quickstart', self.container)
        code = code.replace('ghcr.io/chunkdb/chunkdb:2.0.0', shlex.quote(self.args.image))
        code = code.replace('127.0.0.1:4242:4242', '127.0.0.1::4242')
        if self.port is not None:
            code = code.replace('127.0.0.1:4242/', '127.0.0.1:' + self.port + '/')
        return code

    def block(self, name, suffix=''):
        self.record('PAGE BLOCK ' + name)
        return self.run('set -eu\n' + self.code(name) + suffix)

    def health(self):
        deadline = time.monotonic() + 90
        while True:
            state = self.block('health').stdout.strip()
            if state == 'healthy':
                break
            if state not in ('starting', 'unhealthy') or time.monotonic() >= deadline:
                raise RuntimeError('container did not become healthy: ' + state)
            time.sleep(0.5)  # Poll the documented Docker health state, not protocol timing.
        address = self.run(['docker', 'port', self.container, '4242/tcp']).stdout.strip()
        if not re.fullmatch(r'127\.0\.0\.1:\d+', address):
            raise RuntimeError('unexpected loopback binding: ' + address)
        self.port = address.rsplit(':', 1)[1]

    def expect(self, output, *needles):
        for needle in needles:
            if needle not in output:
                raise RuntimeError('missing expected output: ' + needle)

    def watch_until(self, needle):
        deadline = time.monotonic() + 30
        data = b''
        with selectors.DefaultSelector() as selector:
            selector.register(self.watch.stdout, selectors.EVENT_READ)
            while needle.encode() not in data:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    raise RuntimeError('watch did not produce: ' + needle)
                chunk = os.read(self.watch.stdout.fileno(), 65536)
                if not chunk:
                    raise RuntimeError('watch ended before: ' + needle)
                data += chunk
        self.record(data.decode())

    def execute(self):
        self.block('start')
        built_image = self.run(['docker', 'image', 'inspect', self.args.image,
                                '--format', '{{.Id}}']).stdout.strip()
        running_image = self.run(['docker', 'inspect', self.container,
                                  '--format', '{{.Image}}']).stdout.strip()
        if built_image != running_image:
            raise RuntimeError('page started an image other than the freshly built image')
        self.health()
        with tempfile.TemporaryDirectory(prefix='chunkdb-page-login-') as directory:
            password = Path(directory) / 'password'
            # Retain the export from the page's login shell for subsequent page blocks.
            result = self.block('login', '\nprintf %s "$CHUNKDB_PASSWORD" > ' + shlex.quote(str(password)))
            self.env['CHUNKDB_PASSWORD'] = password.read_text()
        if not self.env['CHUNKDB_PASSWORD']:
            raise RuntimeError('page did not extract the generated password')
        self.secrets.append(self.env['CHUNKDB_PASSWORD'])
        self.expect(result.stdout, 'PONG')
        self.passed.append('generated-password login and health')
        self.block('write')
        self.expect(self.block('read').stdout, 'kind = 1', "name = 'grass'", 'wall',
                    'chunk 0 0', 'present = 2 of 4 blocks')
        self.passed.append('create, write, block and area reads')
        self.record('PAGE BLOCK watch\n$ ' + self.code('watch'))
        self.watch = subprocess.Popen(['bash', '-c', 'exec ' + self.code('watch')],
                                      env=self.env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                      start_new_session=True)
        self.watch_until('start ')
        self.block('watch-update')
        self.watch_until("'door'")
        self.watch.send_signal(signal.SIGINT)
        remainder, _ = self.watch.communicate(timeout=30)
        self.record(remainder.decode())
        if self.watch.returncode != 0:
            raise RuntimeError('watch Ctrl-C returned ' + str(self.watch.returncode))
        self.watch = None
        self.passed.append('watch start, change, Ctrl-C')
        old_password = self.env['CHUNKDB_PASSWORD']
        self.secrets.append('choose-a-new-private-password')
        self.block('reset')
        self.health()  # Docker may assign a different ephemeral port after restart.
        self.expect(self.block('reset-login').stdout, 'PONG')
        self.env['CHUNKDB_PASSWORD'] = 'choose-a-new-private-password'
        self.expect(self.block('read').stdout, 'kind = 3', "name = 'door'", 'wall')
        self.env['CHUNKDB_PASSWORD'] = old_password
        # Reuse the page's PING command with the old password to prove reset took effect.
        ping = self.code('reset-login').splitlines()[-1]
        rejected = self.run(ping, check=False)
        if rejected.returncode == 0 or 'AUTH_FAILED' not in rejected.stdout:
            raise RuntimeError('old password was not rejected')
        self.passed.append('offline admin reset, old password refusal, persisted data')
        logs = self.run(['docker', 'logs', self.container]).stdout
        if logs.count('Generated password: ') != 1:
            raise RuntimeError('restart generated a new bootstrap password')
        if self.run(['docker', 'exec', self.container, 'id', '-u']).stdout.strip() == '0':
            raise RuntimeError('container runs as root')
        self.passed.append('single bootstrap password and non-root runtime')

    def cleanup(self):
        if self.watch is not None:
            if self.watch.poll() is None:
                self.watch.send_signal(signal.SIGINT)
            try:
                data, _ = self.watch.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                self.watch.kill()
                data, _ = self.watch.communicate(timeout=10)
            self.record(data.decode())
        # Capture logs even on failure; remove only this harness's unique fixture.
        self.run(['docker', 'logs', self.container], check=False)
        removed = self.run(['docker', 'rm', '-f', self.container], check=False)
        volume = self.run(['docker', 'volume', 'rm', self.volume], check=False)
        if removed.returncode or volume.returncode:
            raise RuntimeError('fixture cleanup failed; see commands.log')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--page', type=Path, default=Path('docs/QUICK_START.md'))
    parser.add_argument('--image', default='chunkdb:quickstart-ci')
    parser.add_argument('--cli', type=Path)
    parser.add_argument('--logs', type=Path, default=Path('docker-quickstart-logs'))
    parser.add_argument('--check-blocks', action='store_true')
    args = parser.parse_args()
    if args.check_blocks:
        extract(args.page.read_text())
        print('PASS: all nine named shell fences present exactly once')
        return
    if args.cli is None or not args.cli.is_file():
        parser.error('--cli must name the built chunk-cli executable')
    args.logs.mkdir(parents=True, exist_ok=True)
    check = Check(args)
    try:
        check.execute()
    finally:
        check.cleanup()
        (args.logs / 'checks.json').write_text(json.dumps(check.passed, indent=2) + '\n')
    print('PASS:', len(check.passed), 'Docker quick-start groups')


if __name__ == '__main__':
    main()
