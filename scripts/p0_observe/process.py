"""OS process facts. Context switches are not wakeups or energy measurements."""
import os
import platform
import subprocess
import time
from pathlib import Path


class ProcessGone(Exception):
    pass


def cpu_seconds(value):
    days, _, clock = value.rpartition("-")
    parts = [float(v) for v in clock.split(":")]
    if len(parts) not in (2, 3):
        raise ValueError("invalid process CPU time")
    total = parts[-1] + parts[-2] * 60
    if len(parts) == 3:
        total += parts[-3] * 3600
    return total + (int(days) * 86400 if days else 0)


def parse_linux(stat, status, ticks, page_size):
    # comm may contain whitespace and ')' characters; fields begin at state.
    fields = stat[stat.rindex(")") + 2:].split()
    status_values = dict(line.split(":", 1) for line in status.splitlines() if ":" in line)
    switches = {}
    for key in ("voluntary_ctxt_switches", "nonvoluntary_ctxt_switches"):
        switches[key] = int(status_values[key]) if key in status_values else None
    return {
        "identity": "linux-start-ticks:" + fields[19],
        "cpu_seconds": (int(fields[11]) + int(fields[12])) / ticks,
        "rss_bytes": max(0, int(fields[21])) * page_size,
        "threads": int(fields[17]),
        "context_switches": switches,
        "source": "linux-procfs",
    }


def parse_macos(output):
    # ps lstart has five fields, followed by cumulative CPU time and RSS KiB.
    fields = output.split()
    if len(fields) != 7:
        raise ProcessGone("process not available")
    return {
        "identity": "macos-lstart:" + " ".join(fields[:5]),
        "cpu_seconds": cpu_seconds(fields[5]),
        "rss_bytes": int(fields[6]) * 1024,
        "threads": None,
        "context_switches": None,
        "source": "macos-ps",
    }


def sample_process(pid, timeout=2):
    try:
        if platform.system() == "Linux":
            root = Path("/proc") / str(pid)
            first = (root / "stat").read_text()
            status = (root / "status").read_text()
            last = (root / "stat").read_text()
            args = (status, os.sysconf("SC_CLK_TCK"), os.sysconf("SC_PAGE_SIZE"))
            value = parse_linux(last, *args)
            if value["identity"] != parse_linux(first, *args)["identity"]:
                raise ProcessGone("PID was reused during capture")
        elif platform.system() == "Darwin":
            result = subprocess.run(
                ["/bin/ps", "-p", str(pid), "-o", "lstart=,time=,rss="],
                capture_output=True, text=True, timeout=timeout,
                env={**os.environ, "LC_ALL": "C", "TZ": "UTC"}, check=False,
            )
            value = parse_macos(result.stdout)
        else:
            raise RuntimeError("process observation supports Linux and macOS only")
    except (FileNotFoundError, ProcessLookupError) as error:
        raise ProcessGone("process exited") from error
    value.update(pid=pid, captured_at_monotonic_ns=time.monotonic_ns())
    return value


def cpu_percent(previous, current):
    if previous is None or previous["identity"] != current["identity"]:
        return None
    elapsed = (current["captured_at_monotonic_ns"] - previous["captured_at_monotonic_ns"]) / 1e9
    delta = current["cpu_seconds"] - previous["cpu_seconds"]
    return delta / elapsed * 100 if elapsed > 0 and delta >= 0 else None
