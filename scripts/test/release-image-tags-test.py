#!/usr/bin/env python3
"""Exercise the release workflow's tag calculation without publishing an image."""
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

WORKFLOW = Path(__file__).resolve().parents[2] / '.github/workflows/release-binaries.yml'


class ImageTagTests(unittest.TestCase):
    def calculate(self, tag):
        source = WORKFLOW.read_text()
        step = source.split('      - name: Image tags from release tag\n', 1)[1].split('      - uses:', 1)[0]
        script = '\n'.join(line[10:] for line in step.split('        run: |\n', 1)[1].splitlines())
        with tempfile.TemporaryDirectory(prefix='chunkdb-image-tags-') as directory:
            output = Path(directory) / 'outputs'
            result = subprocess.run(['bash', '-eu', '-o', 'pipefail', '-c', script],
                                    env=dict(os.environ, RELEASE_TAG=tag, GITHUB_OUTPUT=str(output)),
                                    text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            values = dict(line.split('=', 1) for line in output.read_text().splitlines()) if output.exists() else {}
        tags = re.findall(r'^            (ghcr.io/chunkdb/chunkdb:[^\n]+)$', source, re.M)
        for key, value in values.items():
            tags = [item.replace('${{ steps.image.outputs.' + key + ' }}', value) for item in tags]
        return result, values, tags

    def test_major_alias_follows_release(self):
        for tag, version, major in [('v2.0.0', '2.0.0', '2'), ('v2.4.1', '2.4.1', '2'),
                                    ('v3.0.0', '3.0.0', '3'), ('v1.9.0', '1.9.0', '1'),
                                    ('v2.1.0-rc.1', '2.1.0-rc.1', '2')]:
            with self.subTest(tag=tag):
                result, values, tags = self.calculate(tag)
                self.assertEqual(result.returncode, 0, result.stdout)
                self.assertEqual(values, {'version': version, 'major': major})
                self.assertEqual(tags, ['ghcr.io/chunkdb/chunkdb:' + version,
                                        'ghcr.io/chunkdb/chunkdb:' + major, 'ghcr.io/chunkdb/chunkdb:latest'])

    def test_invalid_tags_rejected(self):
        for tag in ['2.0.0', 'vnext', 'v3', 'v2.0', 'v02.0.0', 'v2.0.0+build', 'v2.0.0;echo bad']:
            with self.subTest(tag=tag):
                result, values, _ = self.calculate(tag)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(values, {})


if __name__ == '__main__':
    unittest.main()
