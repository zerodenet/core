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
                 "--edges", "normal", "--prefix", "none", "--format", "{p}")
    return {line.removesuffix(" (*)") for line in tree.splitlines() if line.strip()}


def protocol_identity(package):
    """Cargo's actual path package, distinct from registry http v1.x."""
    return f'{package["name"]} v{package["version"]} ({Path(package["manifest_path"]).parent})'


def main():
    metadata = json.loads(cargo("metadata", "--locked", "--no-deps", "--format-version", "1"))
    protocols = {
        protocol_identity(package) for package in metadata["packages"]
        if Path(package["manifest_path"]).is_relative_to(ROOT / "protocols")
    }
    upper = {"zero-config", "zero-engine", "zero-proxy", "zero-runtime"}
    checks = {
        "zero-traits": upper | {"tokio", "tokio-util", "zero-platform-tokio", "zero-error"},
        "zero-core": upper | {"tokio", "tokio-util", "zero-platform-tokio", "zero-error"},
        "zero-error": upper | {"zero-platform-tokio", "tokio", "tokio-util"},
        "zero-platform-tokio": upper,
        "zero-transport": upper,
        # Protocol validation remains reachable through config. Concrete
        # protocol runtimes must never be direct production host dependencies.
        "zero-runtime": {"zero-engine", "zero-proxy", "zero-transport"},
        "zero-config": {"zero-engine", "zero-proxy", "zero-runtime", "zero-platform-tokio", "zero-transport", "tokio", "tokio-util"},
    }
    failed = False
    runtime = next(package for package in metadata["packages"] if package["name"] == "zero-runtime")
    direct_protocols = sorted(
        dependency["name"] for dependency in runtime["dependencies"]
        if dependency["kind"] != "dev" and dependency.get("path")
        and Path(dependency["path"]).is_relative_to(ROOT / "protocols")
    )
    print(f"zero-runtime: direct production protocols={direct_protocols}")
    failed |= bool(direct_protocols)
    for package, forbidden in checks.items():
        identities = dependencies(package)
        reached = {identity.split()[0] for identity in identities} - {package}
        violations = sorted(reached & forbidden)
        if package in {"zero-traits", "zero-core", "zero-error", "zero-platform-tokio", "zero-transport"}:
            violations.extend(sorted(identities & protocols))
        print(f"{package}: {len(reached)} resolved dependencies; forbidden={violations}")
        failed |= bool(violations)
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
