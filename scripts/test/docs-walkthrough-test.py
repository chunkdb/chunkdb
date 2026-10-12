#!/usr/bin/env python3
"""Check tutorial-to-reference copy paths against one server's existing state."""
import argparse
from contextlib import ExitStack
import importlib.util
from pathlib import Path
import tempfile

spec = importlib.util.spec_from_file_location('docs_examples', Path(__file__).with_name('docs-examples.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', type=Path, default=module.ROOT / 'build/chunkdb_server')
    parser.add_argument('--cli', type=Path, required=True)
    args = parser.parse_args()
    pages = ['docs/QUICK_START.md', 'README.md', 'docs/reference/BLOCKS.md',
             'docs/reference/MIGRATIONS.md', 'docs/reference/FEED.md']
    with tempfile.TemporaryDirectory(prefix='chunkdb-walkthrough-') as directory, ExitStack() as stack:
        fixture = module.Fixture(args.server.resolve(), args.cli.resolve(), directory)
        stack.callback(fixture.close)
        fixture.start()
        count = 0
        for page in pages:
            blocks = module.examples((module.ROOT / page).read_text())
            results = []
            watcher = None
            for index, (kind, session, code, expected) in enumerate(blocks):
                results.append(fixture.execute(kind, session, code))
                if kind == 'watch':
                    watcher = index
                elif watcher is not None:
                    results[watcher] += fixture.finish_watch()
                    watcher = None
            assert watcher is None, page
            for index, ((_, _, _, expected), result) in enumerate(zip(blocks, results)):
                actual = module.normalize(result).rstrip('\n')
                assert actual == expected.group(2), page + ' example ' + str(index + 1) + ':\n' + actual
            count += len(blocks)
            print(page + ': PASS on the same server')
    print('PASS: ' + str(count) + ' examples and a durable-slot replay after the tutorial')


if __name__ == '__main__':
    main()
