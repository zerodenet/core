#!/usr/bin/env python3
"""Check the actual resolved dependency graph of public runtime contracts."""
import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parent.parent


def cargo(*args):
    return subprocess.check_output(["cargo", *args], cwd=ROOT, text=True)


def dependencies(package):
    tree = cargo("tree", "--locked", "-p", package, "--no-default-features",
                 "--edges", "normal", "--prefix", "none")
    return {line.split()[0] for line in tree.splitlines() if line.strip()}


def main():
    metadata = json.loads(cargo("metadata", "--locked", "--no-deps", "--format-version", "1"))
    protocols = {
        package["name"] for package in metadata["packages"]
        if Path(package["manifest_path"]).is_relative_to(ROOT / "protocols")
    }
    upper = {"zero-config", "zero-engine", "zero-proxy", "zero-runtime"}
    checks = {
        "zero-traits": upper | protocols | {"tokio", "tokio-util", "zero-platform-tokio", "zero-error"},
        "zero-core": upper | protocols | {"tokio", "tokio-util", "zero-platform-tokio", "zero-error"},
        "zero-error": upper | protocols | {"zero-platform-tokio", "tokio", "tokio-util"},
        "zero-platform-tokio": upper | protocols,
        "zero-config": {"zero-engine", "zero-proxy", "zero-runtime", "zero-platform-tokio", "zero-transport", "tokio", "tokio-util"},
    }
    failed = False
    for package, forbidden in checks.items():
        reached = dependencies(package) - {package}
        violations = sorted(reached & forbidden)
        print(f"{package}: {len(reached)} resolved dependencies; forbidden={violations}")
        failed |= bool(violations)
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
