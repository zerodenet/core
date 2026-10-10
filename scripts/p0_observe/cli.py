"""Finite capture sessions, graceful stop and immutable result directories."""
import argparse
import json
import platform
import resource
import signal
import sys
import threading
import time
from pathlib import Path

from .ipc import KernelSampler, QueryError
from .process import ProcessGone, cpu_percent, sample_process
from .report import summarize
from .storage import Journal, LimitReached, save_json


def bounded_int(low, high):
    def parse(value):
        value = int(value)
        if not low <= value <= high:
            raise argparse.ArgumentTypeError(f"must be in {low}..{high}")
        return value
    return parse


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="operation", required=True)
    capture = commands.add_parser("capture", help="observe an existing PID; never start/reconfigure Zero")
    capture.add_argument("--pid", type=bounded_int(1, 2**31 - 1), required=True)
    capture.add_argument("--socket", type=Path, help="existing Unix control socket; omitted = OS facts only")
    capture.add_argument("--output-dir", type=Path, required=True, help="new private directory; must not exist")
    capture.add_argument("--interval", type=bounded_int(10, 300), default=20, help="seconds; default 20")
    capture.add_argument("--duration", type=bounded_int(1, 86400), default=600, help="seconds; default 600")
    capture.add_argument("--max-bytes", type=bounded_int(65536, 64 * 1024 * 1024), default=16 * 1024 * 1024)
    capture.add_argument("--phase", choices=("idle", "load", "recovery", "mixed"), default="mixed")
    capture.add_argument("--source-commit", help="optional manually verified running binary source SHA")
    report = commands.add_parser("report", help="stream a JSON summary from an existing capture")
    report.add_argument("samples", type=Path)
    return result


def collector_usage():
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {"cpu_seconds": usage.ru_utime + usage.ru_stime,
            "rss_peak_bytes": usage.ru_maxrss * (1024 if platform.system() == "Linux" else 1),
            "child_cpu_seconds": sum((resource.getrusage(resource.RUSAGE_CHILDREN).ru_utime,
                                      resource.getrusage(resource.RUSAGE_CHILDREN).ru_stime))}


def capture(args):
    if platform.system() not in ("Darwin", "Linux"):
        raise ValueError("capture supports Linux and macOS only")
    if args.source_commit and (len(args.source_commit) != 40 or
                               any(c not in "0123456789abcdef" for c in args.source_commit)):
        raise ValueError("source-commit must be a verified full lowercase SHA, or omitted")
    # Establish PID identity before creating an acceptance directory.
    initial = sample_process(args.pid)
    journal = Journal(args.output_dir, args.max_bytes)
    stop = threading.Event()
    old_handlers = {}
    for sig in (signal.SIGINT, signal.SIGTERM):
        old_handlers[sig] = signal.signal(sig, lambda *_: stop.set())
    sampler = KernelSampler(args.socket, args.pid, timeout=2) if args.socket else None
    started = time.monotonic()
    start_unix_ms = time.time_ns() // 1000000
    journal.write({
        "type": "start", "schema": "zero.p0.observation.v1", "started_at_unix_ms": start_unix_ms,
        "pid": args.pid, "process_identity": initial["identity"], "platform": platform.platform(),
        "phase": args.phase, "interval_seconds": args.interval, "duration_seconds": args.duration,
        "max_bytes": args.max_bytes, "source_commit_user_verified": args.source_commit,
        "kernel_queries": sampler is not None, "pid_socket_binding": "OS_peer_pid" if sampler else None,
        "cpu_basis": "delta_process_cpu_seconds/monotonic_window_seconds*100; one core=100%",
        "unavailable": ["wakeups", "energy", "allocations_per_packet", "end_to_end_latency"],
    })
    previous, reason, exit_code, count = None, "duration_complete", 0, 0
    next_sample = started
    try:
        while not stop.is_set() and time.monotonic() - started < args.duration:
            before = time.monotonic()
            process = sample_process(args.pid)
            if process["identity"] != initial["identity"]:
                reason, exit_code = "process_identity_changed", 2
                break
            process["cpu_percent_single_core"] = cpu_percent(previous, process)
            previous = process
            kernel, error = None, None
            if sampler:
                try:
                    kernel = sampler.read()
                except QueryError as failure:
                    error = failure.code
                except (OSError, ValueError, TypeError, KeyError):
                    error = "query_transport_or_format_error"
            # Check for exit/reuse while control queries were in flight.
            after_process = sample_process(args.pid)
            if after_process["identity"] != initial["identity"]:
                reason, exit_code = "process_identity_changed", 2
                break
            journal.write({
                "type": "sample", "sequence": count, "phase": args.phase,
                "sampled_at_unix_ms": time.time_ns() // 1000000,
                "elapsed_seconds": time.monotonic() - started,
                "capture_duration_ms": (time.monotonic() - before) * 1000,
                "process": process, "kernel": kernel, "kernel_error": error,
            })
            count += 1
            if error in ("socket_pid_mismatch", "core_instance_changed", "instance_identity_unavailable"):
                reason, exit_code = error, 2
                break
            # Skip missed ticks: never burst queries to catch up.
            next_sample += args.interval
            while next_sample <= time.monotonic():
                next_sample += args.interval
            stop.wait(max(0, min(next_sample, started + args.duration) - time.monotonic()))
        if stop.is_set():
            reason = "signal_stop"
    except ProcessGone:
        reason, exit_code = "process_exited", 2
    except LimitReached:
        reason = "log_size_limit"
    except (OSError, ValueError, RuntimeError):
        reason, exit_code = "observation_error", 2
    finally:
        journal.finish({"type": "end", "reason": reason, "samples": count,
                        "ended_at_unix_ms": time.time_ns() // 1000000,
                        "elapsed_seconds": time.monotonic() - started, "collector": collector_usage()})
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)
    report = summarize(journal.path)
    save_json(args.output_dir / "summary.json", report)
    print(json.dumps({"output_dir": str(args.output_dir), "reason": reason, "samples": count,
                      "kernel_samples": report["kernel_samples"]}))
    return exit_code


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if args.operation == "report":
            print(json.dumps(summarize(args.samples), indent=2))
            return 0
        return capture(args)
    except (OSError, ValueError, ProcessGone, RuntimeError) as error:
        # Do not print external response contents or command arguments.
        print(f"P0 observation failed: {type(error).__name__}", file=sys.stderr)
        return 2
