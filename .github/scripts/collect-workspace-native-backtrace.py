#!/usr/bin/env python3
"""Retain symbolic test-worker stacks, then delete every raw process core."""
import os
from pathlib import Path
import subprocess


def main():
    root = Path(os.environ["RUNNER_TEMP"])
    output = root / "zero-workspace-backtraces"
    output.mkdir(exist_ok=True)
    target = Path("target").resolve()
    for core in (root / "zero-workspace-cores").glob("core.*"):
        report = output / (core.name + ".txt")
        try:
            # NT_FILE mappings retain the exact actual test executable even
            # when the crashing task's comm is merely tokio-rt-worker.
            probe = subprocess.run(["gdb", "--batch", "-iex", "set auto-load off",
                                    "-c", str(core), "-ex", "info proc mappings"],
                                   capture_output=True, text=True, timeout=60)
            executable = None
            for line in probe.stdout.splitlines():
                fields = line.split()
                if not fields:
                    continue
                path = Path(fields[-1])
                if path.is_absolute() and path.is_relative_to(target) and path.is_file() and os.access(path, os.X_OK):
                    executable = path
                    break
            if executable is None:
                raise RuntimeError("no actual workspace test executable in core mappings")
            with report.open("w") as stream:
                subprocess.run(["gdb", "--batch", "-iex", "set auto-load off",
                                "-ex", "set print frame-arguments none",
                                "-ex", "set backtrace limit 96",
                                "-ex", "thread apply all bt", str(executable), str(core)],
                               stdout=stream, stderr=subprocess.STDOUT, timeout=60, check=True)
        except Exception as error:
            with report.open("a") as stream:
                stream.write(f"symbolic native diagnosis failed: {error}\n")
        finally:
            core.unlink()


if __name__ == "__main__":
    main()
