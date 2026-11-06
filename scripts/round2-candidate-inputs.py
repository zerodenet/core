#!/usr/bin/env python3
"""Exact round2 CI inputs; never regenerate or update the validation lock."""
import argparse
import hashlib
import json
import os
import platform
import shutil
from pathlib import Path
import subprocess
import sys


def digest(data):
    return hashlib.sha256(data).hexdigest()


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    fixture = Path(".github/round2/candidate.Cargo.lock")
    expected = Path(str(fixture) + ".sha256").read_text().split()[0]
    frozen = fixture.read_bytes()
    if digest(frozen) != expected:
        sys.exit("candidate lock fixture digest mismatch")
    output = Path(".round2-evidence")
    output.mkdir(exist_ok=True)
    actual = Path("Cargo.lock")
    if args.verify:
        used = actual.read_bytes()
        (output / "used.Cargo.lock").write_bytes(used)
        tools = {}
        for command in (["rustc", "--version", "--verbose"], ["cargo", "--version"], ["protoc", "--version"]):
            if shutil.which(command[0]):
                result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                tools[command[0]] = {"command": command, "exit": result.returncode, "output": result.stdout}
        (output / "tools.json").write_text(json.dumps({"tools": tools,
            "platform": platform.platform(), "rustup_toolchain": os.environ.get("RUSTUP_TOOLCHAIN")}, indent=2) + "\n")
        result = {"expected_sha256": expected, "actual_sha256": digest(used)}
        (output / "lock-verification.json").write_text(json.dumps(result, indent=2) + "\n")
        if used != frozen:
            sys.exit("candidate Cargo.lock changed during verification")
        print("unchanged candidate Cargo.lock SHA-256:", expected)
        return
    if actual.exists() and actual.read_bytes() != frozen:
        sys.exit("refusing to overwrite an existing different Cargo.lock")
    actual.write_bytes(frozen)
    (output / "candidate.Cargo.lock").write_bytes(frozen)
    sources = {name: digest(Path(name).read_bytes()) for name in git("ls-files").splitlines()
               if Path(name).is_file()}
    manifest = (json.dumps(sources, sort_keys=True, indent=2) + "\n").encode()
    (output / "source-inputs.json").write_bytes(manifest)
    proof = {"checkout_sha": git("rev-parse", "HEAD"), "tree": git("rev-parse", "HEAD^{tree}"),
             "tracked_diff_sha256": digest(subprocess.check_output(["git", "diff", "HEAD", "--binary"])),
             "source_inputs_sha256": digest(manifest), "candidate.Cargo.lock_sha256": expected,
             "python": sys.version, "requested_rust_toolchain": "1.99.0",
             "qualification": "input provenance only; check results and raw workflow logs separately"}
    (output / "checkout.json").write_text(json.dumps(proof, indent=2) + "\n")
    print(json.dumps(proof, indent=2))


if __name__ == "__main__":
    main()
