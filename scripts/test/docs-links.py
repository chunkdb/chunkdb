#!/usr/bin/env python3
"""Check local Markdown targets and heading anchors in repository documentation."""
from pathlib import Path
import re
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[2]
LINK = re.compile(r'!?\[[^\]]*\]\(([^\s)]+)(?:\s+"[^"]*")?\)')


def anchors(path):
    text = re.sub(r'^```.*?^```\s*$', '', path.read_text(), flags=re.M | re.S)
    result = set()
    counts = {}
    for match in re.finditer(r'^#{1,6}\s+(.+?)\s*#*$', text, re.M):
        title = match.group(1).lower().replace(' ', '-')
        title = re.sub(r'[^\w-]', '', title)
        number = counts.get(title, 0)
        counts[title] = number + 1
        result.add(title + ('-' + str(number) if number else ''))
    return result


def check():
    failures = []
    pages = [ROOT / 'README.md', ROOT / 'CONTRIBUTING.md', ROOT / 'CHANGELOG.md', *sorted((ROOT / 'docs').rglob('*.md'))]
    count = 0
    for page in pages:
        for match in LINK.finditer(page.read_text()):
            url = urlsplit(match.group(1))
            if url.scheme or url.netloc:
                continue
            target = (page.parent / unquote(url.path)).resolve() if url.path else page
            line = page.read_text()[:match.start()].count('\n') + 1
            label = str(page.relative_to(ROOT)) + ':' + str(line)
            if not target.exists():
                failures.append(label + ': missing ' + match.group(1))
            elif url.fragment and target.suffix == '.md' and unquote(url.fragment) not in anchors(target):
                failures.append(label + ': missing anchor ' + match.group(1))
            count += 1
    if failures:
        raise SystemExit('\n'.join(failures))
    print('PASS: ' + str(count) + ' local documentation links')


if __name__ == '__main__':
    check()
