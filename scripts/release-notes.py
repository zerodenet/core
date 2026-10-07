#!/usr/bin/env python3
"""Generate notes from an earlier ancestral tag in the same release channel."""
import argparse
from dataclasses import dataclass
from pathlib import Path
import re
import subprocess


@dataclass(frozen=True)
class Version:
    base: tuple
    stage: str
    sequence: int


def version(tag):
    match = re.fullmatch(
        r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
        r"(?:-(dev|alpha|beta|rc)\.([1-9][0-9]*))?", tag
    )
    if not match:
        return None
    return Version(tuple(map(int, match.group(1, 2, 3))),
                   match.group(4) or "stable", int(match.group(5) or 0))


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def ancestor(tag, current):
    return subprocess.run(
        ["git", "merge-base", "--is-ancestor", f"{tag}^{{commit}}", f"{current}^{{commit}}"],
        check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    ).returncode == 0


def previous_tag(current):
    target = version(current)
    if target is None or target.stage not in {"dev", "rc", "stable"}:
        raise ValueError(f"unsupported current release tag: {current}")
    git("rev-parse", "--verify", f"refs/tags/{current}^{{commit}}")
    candidates = []
    for tag in git("tag", "--list", "v*").splitlines():
        parsed = version(tag)
        if parsed is None or tag == current:
            continue
        if (parsed.base == target.base and parsed.stage == target.stage
                and parsed.sequence < target.sequence and ancestor(tag, current)):
            candidates.append((parsed.sequence, tag))
    if candidates:
        return max(candidates)[1]
    # Stable notes include the complete changes since the previous stable;
    # first dev/RC builds use that baseline too, rather than another channel.
    stable = []
    for tag in git("tag", "--list", "v*").splitlines():
        parsed = version(tag)
        if (parsed is not None and parsed.stage == "stable"
                and parsed.base < target.base and ancestor(tag, current)):
            stable.append((parsed.base, tag))
    return max(stable)[1] if stable else None


def render(current):
    previous = previous_tag(current)
    scope = f"{previous}..{current}" if previous else current
    subjects = git("log", scope, "--no-merges", "--format=%s").splitlines()
    subjects = [s for s in subjects if not re.match(
        r"(?:release(?:[: ]|$)|chore\(release\):|build\(release\):)", s, re.I
    )]
    lines = ["## Assets", "", "| Platform | Archive |", "|---|---|"]
    for platform, archive in [
        ("Linux x86_64 (GNU/glibc)", "zero-linux-x86_64.tar.gz"),
        ("Linux x86_64 (musl)", "zero-linux-x86_64-musl.tar.gz"),
        ("macOS x86_64", "zero-darwin-x86_64.tar.gz"),
        ("macOS aarch64", "zero-darwin-aarch64.tar.gz"),
        ("Windows x86_64", "zero-windows-x86_64.zip"),
    ]:
        lines.append(f"| {platform} | `{archive}` |")
    lines += ["", "## Changes", ""]
    lines += [f"- {subject}" for subject in subjects] or ["- No code changes."]
    lines += ["", "---", f"Generated from `{scope}` by GitHub Actions."]
    if previous is None:
        lines.append("Initial release in this channel; no earlier ancestral baseline is available.")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--base-only", action="store_true")
    args = parser.parse_args()
    content = (previous_tag(args.tag) or "") if args.base_only else render(args.tag)
    if args.output:
        args.output.write_text(content, encoding="utf-8")
    else:
        print(content, end="\n" if args.base_only else "")


if __name__ == "__main__":
    main()
