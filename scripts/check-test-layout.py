#!/usr/bin/env python3
"""Check explicit integration suites without compiling or executing test cases."""

import argparse
from collections import Counter
import json
from pathlib import Path
import re
import subprocess


MODULE_PATH = re.compile(r'^#\[path = "([^"\n]+)"\]\s*\nmod (\w+);', re.MULTILINE)


def inspect_package(package):
    tests = (Path(package["manifest_path"]).parent / "tests").resolve()
    roots = set(tests.glob("*.rs"))
    suites = set((tests / "suites").glob("*.rs"))
    covered = Counter()
    registered = set()
    sources = {}
    errors = []
    targets = [t for t in package["targets"] if "test" in t["kind"]]
    for target in targets:
        entry = Path(target["src_path"]).resolve()
        registered.add(entry)
        if entry in roots:
            covered[entry] += 1
            sources[entry.name] = {"target": target["name"], "filter": ""}
        if entry in suites:
            for relative, module in MODULE_PATH.findall(entry.read_text()):
                source = (entry.parent / relative).resolve()
                if not source.is_file():
                    errors.append(f"{entry.name}: missing module {relative}")
                if source in roots:
                    covered[source] += 1
                    sources[source.name] = {
                        "target": target["name"], "filter": module + "::"
                    }
    for source in sorted(roots):
        if covered[source] != 1:
            errors.append(f"{source.name}: expected one entry, found {covered[source]}")
    for suite in sorted(suites - registered):
        errors.append(f"suites/{suite.name}: no Cargo test target")
    return {
        "package": package["name"], "source_files": len(roots),
        "targets": len(targets), "sources": sources, "errors": errors,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit source-to-target mapping")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"], cwd=root
    ))
    members = set(metadata["workspace_members"])
    results = [inspect_package(p) for p in metadata["packages"] if p["id"] in members]
    errors = [(r["package"], e) for r in results for e in r["errors"]]
    if args.json:
        print(json.dumps(results, indent=2))
    else:
        for package, error in errors:
            print(f"{package}: {error}")
        print(f"Integration layout: {sum(r['source_files'] for r in results)} source files, "
              f"{sum(r['targets'] for r in results)} targets, {len(errors)} errors")
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
