#!/usr/bin/env python3
"""Focused extraction/fixture checks; no Docker or network needed."""
import argparse
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest import mock

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

    def test_changed_page_image_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            page = Path(directory) / 'QUICK_START.md'
            page.write_text(PAGE.read_text().replace('ghcr.io/chunkdb/chunkdb:2.0.0',
                                                    'ghcr.io/chunkdb/chunkdb:2.1.0'))
            args = argparse.Namespace(page=page, image='chunkdb:local-check',
                                      cli=Path(directory) / 'chunk-cli', logs=Path(directory))
            with self.assertRaisesRegex(ValueError, 'published image changed'):
                module.Check(args)

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

    def test_reset_password_comes_from_page(self):
        with tempfile.TemporaryDirectory() as directory:
            page = Path(directory) / 'QUICK_START.md'
            page.write_text(PAGE.read_text().replace('choose-a-new-private-password', 'different documented secret'))
            args = argparse.Namespace(page=page, image='chunkdb:local-check',
                                      cli=Path(directory) / 'chunk-cli', logs=Path(directory))
            check = module.Check(args)
            secret = check.exported_password('reset-login')
            self.assertEqual(secret, 'different documented secret')
            check.secrets.append(secret)
            check.record(check.code('reset'))
            self.assertNotIn(secret, check.log.read_text())
            self.assertIn('<redacted>', check.log.read_text())
            # Run the actual reset-login fence, capturing its export just as the
            # initial login does; only the fixture CLI is replaced for this unit.
            check.blocks['reset-login'] = check.blocks['reset-login'].replace(
                'chunk-cli --uri chunk://admin@127.0.0.1:4242/ PING', "printf 'PONG\\n'")
            check.login('reset-login')
            self.assertEqual(check.env['CHUNKDB_PASSWORD'], secret)
            self.assertNotIn(secret, check.log.read_text())

    def test_cleanup_preserves_startup_error(self):
        with tempfile.TemporaryDirectory() as directory:
            check = mock.Mock(args=argparse.Namespace(logs=Path(directory)), passed=[])
            check.execute.side_effect = RuntimeError('startup failed')
            check.cleanup.side_effect = RuntimeError('fixture cleanup failed')
            with self.assertRaisesRegex(RuntimeError, 'startup failed.*fixture cleanup failed') as failure:
                module.execute_and_cleanup(check)
            self.assertEqual(str(failure.exception.__cause__), 'startup failed')
            check.cleanup.assert_called_once()
            self.assertEqual((Path(directory) / 'checks.json').read_text(), '[]\n')

    def test_cleanup_failure_is_not_ignored_after_success(self):
        with tempfile.TemporaryDirectory() as directory:
            check = mock.Mock(args=argparse.Namespace(logs=Path(directory)), passed=['login'])
            check.cleanup.side_effect = RuntimeError('fixture cleanup failed')
            with self.assertRaisesRegex(RuntimeError, 'fixture cleanup failed'):
                module.execute_and_cleanup(check)
            check.cleanup.assert_called_once()


if __name__ == '__main__':
    unittest.main()
