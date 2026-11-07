#!/usr/bin/env python3
"""Retain actual workspace stdout and exit results in the existing CI artifact."""
import datetime
import hashlib
import json
import os
import platform
from pathlib import Path
import re
import subprocess
import sys


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for data in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(data)
    return digest.hexdigest()


def command(arguments):
    result = subprocess.run(arguments, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return {"command": arguments, "exit": result.returncode, "output": result.stdout}


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def main():
    mode, filename, *arguments = sys.argv[1:]
    log = Path(filename)
    metadata = Path(str(log) + ".json")
    if mode == "start":
        if metadata.exists():
            raise SystemExit("refusing to overwrite an actual test record")
        record = {
            "started_utc": now(), "command": arguments, "state": "started_without_confirmed_exit",
            "checkout": command(["git", "rev-parse", "HEAD"]),
            "Cargo.lock_sha256": sha("Cargo.lock"),
            "source_inputs_sha256": sha(".round2-evidence/source-inputs.json")
            if Path(".round2-evidence/source-inputs.json").exists() else None,
            "host": {"platform": platform.platform(), "cpu_count": os.cpu_count()},
            "simulation": bool(os.environ.get("ZERO_TEST_ARGS")),
            "tools": [command(args) for args in (
                ["rustc", "--version", "--verbose"], ["cargo", "--version"], ["protoc", "--version"])],
            "environment": {key: os.environ.get(key) for key in (
                "RUSTUP_TOOLCHAIN", "RUST_MIN_STACK", "CARGO_BUILD_JOBS", "CARGO_TARGET_DIR",
                "CARGO_PROFILE_TEST_DEBUG", "CARGO_INCREMENTAL", "RUNNER_OS")},
        }
    elif mode == "finish":
        record = json.loads(metadata.read_text())
        text = log.read_text(errors="replace")
        record.update(ended_utc=now(), exit=int(arguments[0]), state="exited",
                      raw_log_sha256=sha(log), used_lock_sha256=sha("Cargo.lock"),
                      checkout_after=command(["git", "rev-parse", "HEAD"]))
        record["lock_unchanged"] = record["used_lock_sha256"] == record["Cargo.lock_sha256"]
        record["targets"] = []
        current = None
        for line in text.splitlines():
            if re.match(r"\s*(Running |Doc-tests )", line):
                current = {"target": line.strip(), "summaries": []}
                record["targets"].append(current)
            if line.startswith("test result: "):
                if current is None:
                    current = {"target": "unidentified; inspect raw log", "summaries": []}
                    record["targets"].append(current)
                current["summaries"].append(line)
        record["binary_sha256"] = {}
        for name in re.findall(r"\bRunning[^\n]*\(([^)]+)\)", text):
            path = Path(name)
            if path.is_file():
                record["binary_sha256"][name] = sha(path)
        record["qualification"] = (
            "simulation; not Rust qualification" if record["simulation"] else
            "actual command result only; final candidate also requires all scheduled checks")
    else:
        raise SystemExit("expected start or finish")
    metadata.write_text(json.dumps(record, indent=2) + "\n")


if __name__ == "__main__":
    main()
