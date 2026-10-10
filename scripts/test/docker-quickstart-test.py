#!/usr/bin/env python3
"""Focused extraction/fixture checks; no Docker or network needed."""
import argparse
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('docker_quickstart', Path(__file__).with_name('docker-quickstart.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
PAGE = Path(__file__).resolve().parents[2] / 'docs/QUICK_START.md'


class PageTests(unittest.TestCase):
    def test_exact_page_commands(self):
        blocks = module.extract(PAGE.read_text())
        self.assertEqual(len(blocks), 9)
        self.assertIn('CREATE TABLE world', blocks['write'])
        self.assertIn('reset-password admin --password-file /dev/stdin', blocks['reset'])

    def test_duplicate_rejected(self):
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            module.extract(PAGE.read_text() + '\n<!-- docker-quickstart:health -->\n```sh\nfalse\n```')

    def test_missing_or_wrong_shell_rejected(self):
        text = PAGE.read_text()
        for broken in (text.replace('docker-quickstart:health', 'docker-quickstart:unknown'),
                       text.replace('```sh', '```text', 1)):
            with self.assertRaises(ValueError):
                module.extract(broken)

    def test_fixture_changes_only_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(page=PAGE, image='chunkdb:local-check',
                                      cli=Path(directory) / 'chunk-cli', logs=Path(directory))
            check = module.Check(args)
            check.port = '45423'
            for name in module.BLOCKS:
                code = check.code(name)
                self.assertNotIn('chunkdb-quickstart', code)
                self.assertNotIn('127.0.0.1:4242/', code)
            original = module.extract(PAGE.read_text())['write'].splitlines()
            rewritten = check.code('write').splitlines()
            self.assertEqual([line.split('"', 1)[1] for line in original],
                             [line.split('"', 1)[1] for line in rewritten])
            self.assertIn('127.0.0.1::4242', check.code('start'))
            check.secrets = ['dummy-secret']
            self.assertNotIn('dummy-secret', check.redact('dummy-secret'))
            self.assertNotIn('123secret', check.redact('Generated password: 123secret\n'))


if __name__ == '__main__':
    unittest.main()
