#!/usr/bin/env python3
"""Read-only PR00 snapshot and local Markdown link checks; no network or Cargo."""

import hashlib
import json
from pathlib import Path
import re
from urllib.parse import unquote, urlsplit


def require(condition, message):
    if not condition:
        raise SystemExit(message)


def anchors(text):
    result = set(re.findall(r'<a id="([^"]+)"', text))
    duplicates = {}
    for heading in re.findall(r'^#{1,6}\s+(.+)$', text, re.M):
        slug = re.sub(r'[^\w\- ]', '', heading.lower()).replace(' ', '-')
        count = duplicates.get(slug, 0)
        duplicates[slug] = count + 1
        result.add(slug if count == 0 else f'{slug}-{count}')
    return result


def main():
    directory = Path(__file__).resolve().parent
    repository = directory.parents[1]
    manifest = json.loads((directory / 'provenance.json').read_text())
    documents = {p.name: p.read_text() for p in sorted(directory.glob('*.md'))}
    expected = {item['id']: item for item in manifest['sections']}
    found = {}
    local_links = 0
    online_links = 0
    for name, text in documents.items():
        require(not re.search(r'/Volumes/|file://|/Users/', text), f'{name}: machine-local path')
        fence = None
        for line in text.splitlines():
            match = re.match(r'^\s*(`{3,}|~{3,})', line)
            if match:
                marker = match[1]
                if fence is None:
                    fence = marker
                elif marker[0] == fence[0] and len(marker) >= len(fence):
                    fence = None
        require(fence is None, f'{name}: unclosed code fence')
        section_pattern = r'<!-- source-section: (\d{2}) -->\n(.*?)\n<!-- /source-section: \1 -->'
        matches = list(re.finditer(section_pattern, text, re.S))
        require(text.count('<!-- source-section: ') == len(matches), f'{name}: malformed source marker')
        require(text.count('<!-- /source-section: ') == len(matches), f'{name}: unmatched source marker')
        for match in matches:
            index, body = match[1], match[2]
            require(index in expected and index not in found, f'{name}: unknown/duplicate section {index}')
            record = expected[index]
            require(record['file'] == name, f'{name}: wrong source section owner {index}')
            require(len(body) == record['characters'], f'{name}: section {index} length changed')
            require(hashlib.sha256(body.encode()).hexdigest() == record['sha256'],
                    f'{name}: section {index} digest changed; review and update provenance explicitly')
            found[index] = name
        for target in re.findall(r'\]\(([^)]+)\)', text):
            target = target.strip()
            uri = urlsplit(target)
            if uri.scheme:
                require(uri.scheme == 'https' and bool(uri.netloc), f'{name}: invalid external URL {target}')
                online_links += 1
                continue
            require(not target.startswith('/'), f'{name}: absolute local link {target}')
            path = (directory / unquote(uri.path)).resolve() if uri.path else directory / name
            require(path.is_relative_to(repository), f'{name}: link escapes repository {target}')
            require(path.is_file(), f'{name}: missing local link {target}')
            if uri.fragment:
                fragment = unquote(uri.fragment)
                line = re.fullmatch(r'L(\d+)', fragment)
                if line:
                    require(1 <= int(line[1]) <= len(path.read_text().splitlines()),
                            f'{name}: line outside file {target}')
                else:
                    require(fragment in anchors(path.read_text()), f'{name}: missing anchor {target}')
            local_links += 1
    require(set(found) == set(expected) and len(found) == 25, 'missing source sections')
    combined = '\n'.join(documents.values())
    for identifier in [*(f'G{i}' for i in range(11)), *(f'M{i}' for i in range(7)), *manifest['gates']]:
        require(re.search(r'\b' + identifier + r'\b', combined), f'missing goal/stage/gate {identifier}')
    cards = documents['task-cards.md']
    fields = ['目标 ID / 前置', '状态 / 实际基线', '范围', '明确不做', '契约变化',
              '调用者迁移 / 删除时点', '测试及能力保持证据', '待外部验收', '完成证据', '回退边界 / commit']
    for identifier in manifest['review_units']:
        marker = f'<a id="{identifier.lower()}"></a>'
        require(cards.count(marker) == 1, f'missing/duplicate task card {identifier}')
        body = cards.split(marker, 1)[1].split('<a id=', 1)[0]
        for field in fields:
            require(f'| {field} |' in body, f'{identifier}: missing task field {field}')
    require(len(manifest['review_units']) == 17, 'expected 17 review units')
    print(f'PASS: {len(found)} complete source sections match recorded SHA-256 digests')
    print('PASS: G0-G10, M0-M6, all 10 gates and 17 task cards with required fields')
    print(f'PASS: {local_links} local file/anchor/line links; code fences and portable paths')
    print(f'INFO: {online_links} HTTPS references have valid URL form; remote availability not tested')
    print('INFO: documentation checks only; no runtime, compile, interop or consumer qualification')


if __name__ == '__main__':
    main()
