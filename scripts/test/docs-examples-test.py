#!/usr/bin/env python3
"""Validate example markings and normalizers without starting a server."""
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('docs_examples', Path(__file__).with_name('docs-examples.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ExampleTests(unittest.TestCase):
    def test_unclosed_marked_fences_rejected(self):
        valid = '```cql docs-check\nPING\n```\n\n```output\nPONG\n```\n'
        for suffix in ['```cql docs-check\nPING\n',
                       '```cql docs-check\nPING\n```\n\n```output\nPONG\n',
                       '```cql docs-check\nPING\n```output\nPONG\n```\n']:
            with self.subTest(suffix=suffix), self.assertRaisesRegex(ValueError, 'line'):
                module.examples(valid + suffix)

    def test_initial_prompt_failure_reaps_child(self):
        spawned = []
        popen = subprocess.Popen

        def start_child(*args, **kwargs):
            process = popen([sys.executable, '-c', 'import sys; sys.stdin.buffer.read()'], **kwargs)
            spawned.append(process)
            return process

        try:
            with mock.patch.object(module.subprocess, 'Popen', side_effect=start_child), \
                    mock.patch.object(module.Reader, 'until', side_effect=RuntimeError('initial prompt deadline')):
                with self.assertRaisesRegex(RuntimeError, 'initial prompt deadline'):
                    module.Shell(Path('chunk-cli'), dict(os.environ, CHUNKDB_URI='chunk://127.0.0.1/'))
            self.assertEqual(len(spawned), 1)
            self.assertIsNotNone(spawned[0].poll())
            self.assertTrue(spawned[0].stdout.closed)
            self.assertTrue(spawned[0].stdin.closed)
        finally:
            for process in spawned:
                if process.poll() is None:
                    process.kill()
                    process.communicate()

    def test_expected_output_required(self):
        for text in ['```cql docs-check\nPING\n```',
                     '```cql docs-check\nPING\n```\n\n```text\nPONG\n```',
                     '```cql docs-check\nPING\n```\nexplanation\n```output\nPONG\n```']:
            with self.subTest(text=text), self.assertRaises(ValueError):
                module.examples(text)

    def test_unknown_marker_rejected(self):
        with self.assertRaises(ValueError):
            module.examples('```sh docs-check-unknown\ntrue\n```\n\n```output\nOK\n```')
        with self.assertRaises(ValueError):
            module.examples('```cql docs-check-unknown\nPING\n```\n\n```output\nPONG\n```')

    def test_two_connections_and_watch_preserve_source(self):
        source = ('```cql docs-check session=first\nBEGIN\n```\n\n```output\nOK\n```\n'
                  '```cql docs-check session=second\nPING\n```\n\n```output\nPONG\n```\n'
                  '```sh docs-check-watch\nchunk-cli watch world\n```\n\n```output\nstart <epoch>:0\n```')
        examples = module.examples(source)
        self.assertEqual([(kind, session, code) for kind, session, code, _ in examples],
                         [('cql', 'first', 'BEGIN'), ('cql', 'second', 'PING'),
                          ('watch', 'main', 'chunk-cli watch world')])

    def test_only_volatile_metadata_changes(self):
        epoch = '0123456789abcdef0123456789abcdef'
        source = ('start ' + epoch + ':2\nchange revision 3 time_ms 123 user admin\n'
                  '1) name = step, applied_ms = 456, user = admin\n'
                  'server_version = 2.0.0\nGenerated password: secret\n')
        self.assertEqual(module.normalize(source),
                         'start <epoch>:2\nchange revision 3 time_ms <time-ms> user admin\n'
                         '1) name = step, applied_ms = <time-ms>, user = admin\n'
                         'server_version = <server-version>\nGenerated password: <generated-password>\n')
        data = "value = '" + epoch + "'\nvalue = 'epoch = " + epoch + ",'\nvalue = 'time_ms 123'\nversion = 42\n42\n"
        self.assertEqual(module.normalize(data), data)


if __name__ == '__main__':
    unittest.main()
