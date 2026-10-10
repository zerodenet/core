#!/usr/bin/env python3
"""Prepare a nightly candidate using the existing authoritative version policy.

This script never pushes, tags, or calls GitHub. The workflow owns publication
and requires successful CI for the exact candidate commit before tagging.
"""
import argparse
import json
from pathlib import Path
import re
import subprocess
import tomllib


RELEASE_FILES = {"Cargo.toml", "release/breaking-changes.md", "release/promotion-source"}
PRERELEASE = re.compile(
    r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-(dev|rc)\.([1-9][0-9]*)"
)


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def ancestor(ref, head):
    result = subprocess.run(["git", "merge-base", "--is-ancestor", ref, head], check=False)
    if result.returncode not in (0, 1):
        raise RuntimeError(f"Cannot inspect ancestry of {ref}")
    return result.returncode == 0


def release(*args):
    return subprocess.check_output(["bash", "scripts/release.sh", *args], text=True).strip()


def workspace_version():
    return tomllib.loads(Path("Cargo.toml").read_text())["workspace"]["package"]["version"]


def retry_candidate(branch, head, tag):
    if git("show", "-s", "--format=%s", head) != f"release: nightly {tag}":
        return False
    parents = git("show", "-s", "--format=%P", head).split()
    if len(parents) != 1:
        return False
    source = Path("release/promotion-source").read_text().strip()
    changed = set(git("diff", "--name-only", parents[0], head).splitlines())
    return source == f"{branch}@{parents[0]}" and changed <= RELEASE_FILES


def plan(branch):
    head = git("rev-parse", "HEAD")
    current = workspace_version()
    result = {"branch": branch, "sha": head, "version": current, "tag": "v" + current}
    match = PRERELEASE.fullmatch(current)
    if not match:
        if re.fullmatch(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", current):
            return dict(result, mode="skip", reason="stable requires manual promotion")
        raise ValueError(f"Unsupported branch version: {current}")
    stage = "dev" if branch == "develop" else "rc"
    if match[4] != stage:
        raise ValueError(f"{branch} cannot automatically publish {current}")
    base = match.group(1, 2, 3)
    candidates = []
    for tag in git("tag", "--list", "v*").splitlines():
        parsed = PRERELEASE.fullmatch(tag[1:])
        if (parsed and parsed.group(1, 2, 3) == base and parsed[4] == stage
                and ancestor(f"refs/tags/{tag}^{{commit}}", head)):
            candidates.append((int(parsed[5]), tag))
    if not candidates:
        raise ValueError(f"No ancestral {stage} tag for {'.'.join(base)}; seed this line manually")
    sequence, previous = max(candidates)
    if sequence > int(match[5]):
        raise ValueError(f"Branch version is behind {previous}")
    result["previous_tag"] = previous
    if git("rev-parse", f"refs/tags/{previous}^{{commit}}") == head:
        release("--verify-tag", previous)
        return dict(result, mode="existing", tag=previous, reason="no new commits; inspect release assets")
    if retry_candidate(branch, head, result["tag"]):
        release("--check")
        release("--verify-tag", result["tag"])
        return dict(result, mode="retry", reason="reuse untagged nightly candidate")
    return dict(result, mode="prepare", reason="new commits since branch release")


def prepare(branch, expected_sha):
    if git("status", "--porcelain"):
        raise ValueError("Candidate checkout must be clean")
    result = plan(branch)
    if result["sha"] != expected_sha:
        raise ValueError("Source commit changed since planning")
    if result["mode"] != "prepare":
        return dict(result, pushed_candidate=False)
    stage = "dev" if branch == "develop" else "rc"
    version = release("--next", stage)
    if stage == "dev":
        release(version, "--start-development")
    else:
        release(version, "--seal-only")
    Path("release/promotion-source").write_text(f"{branch}@{expected_sha}\n")
    release("--check")
    changed = set(git("diff", "--name-only").splitlines())
    if not changed <= RELEASE_FILES:
        raise ValueError(f"Unexpected release changes: {changed - RELEASE_FILES}")
    git("add", "--", *sorted(RELEASE_FILES))
    git("commit", "-m", f"release: nightly v{version}")
    head = git("rev-parse", "HEAD")
    release("--check-transition", expected_sha, head)
    return dict(result, version=version, tag="v" + version, sha=head, pushed_candidate=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["plan", "prepare"])
    parser.add_argument("--branch", choices=["main", "develop"], required=True)
    parser.add_argument("--expected-sha")
    args = parser.parse_args()
    if args.operation == "prepare" and not args.expected_sha:
        parser.error("prepare requires --expected-sha")
    result = (plan(args.branch) if args.operation == "plan"
              else prepare(args.branch, args.expected_sha))
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
