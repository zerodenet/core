"""Streaming summaries with bounded scope state. No energy/latency inference."""
import json
import math

from .ipc import METRICS, MAX_FRAME_BYTES

MAX_SCOPES = 256


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0


def traffic_delta(before, after, old_plane, new_plane):
    fence = ("core_instance_id", "config_revision", "generation", "stats_epoch")
    if any(before.get(k) != after.get(k) for k in fence):
        return None
    if old_plane.get("accounting_basis") != new_plane.get("accounting_basis"):
        return None
    old_time, new_time = before.get("sampled_at_monotonic_ns"), after.get("sampled_at_monotonic_ns")
    if not number(old_time) or not number(new_time) or new_time <= old_time:
        return None
    window = (new_time - old_time) / 1e9
    old, new = old_plane.get("counters", {}), new_plane.get("counters", {})
    available = set(old_plane.get("available_metrics", [])) & set(new_plane.get("available_metrics", []))
    deltas = {k: new[k] - old[k] if k in available and number(old.get(k)) and number(new.get(k))
              and new[k] >= old[k] else None for k in METRICS}
    return {"window_seconds": window, "delta": deltas,
            "per_second": {k: value / window if value is not None else None for k, value in deltas.items()}}


class Summary:
    def __init__(self):
        self.value = {"schema": "zero.p0.summary.v1", "samples": 0, "kernel_samples": 0,
                      "query_errors": {}, "cpu_percent_single_core": {"count": 0, "sum": 0, "max": None},
                      "rss_bytes_sampled_max": None, "capture_duration_ms_max": None,
                      "scope_planes": [], "scope_observations_skipped_at_limit": 0,
                      "unavailable": ["wakeups", "energy", "allocations_per_packet", "end_to_end_latency"],
                      "limitations": ["sampled_peaks_only", "no_workload_control", "observer_overhead_not_subtracted",
                                      "traffic_planes_and_scopes_are_not_additive", "one_traffic_page_per_sample"]}
        self.scopes = {}

    def add(self, record):
        kind = record.get("type")
        if kind == "start":
            self.value["run"] = record
        elif kind == "end":
            self.value["end"] = record
        elif kind == "sample":
            self.value["samples"] += 1
            process = record.get("process") or {}
            cpu = process.get("cpu_percent_single_core")
            if number(cpu):
                stat = self.value["cpu_percent_single_core"]
                stat["count"] += 1
                stat["sum"] += cpu
                stat["max"] = max(stat["max"] or 0, cpu)
            for target, value in (("rss_bytes_sampled_max", process.get("rss_bytes")),
                                  ("capture_duration_ms_max", record.get("capture_duration_ms"))):
                if number(value):
                    self.value[target] = max(self.value[target] or 0, value)
            error = record.get("kernel_error")
            if error:
                counts = self.value["query_errors"]
                counts[error] = counts.get(error, 0) + 1
            kernel = record.get("kernel")
            if kernel:
                self.value["kernel_samples"] += 1
                page = kernel.get("traffic_page") or {}
                status = kernel.get("traffic_status")
                if status not in ("available", None):
                    counts = self.value["query_errors"]
                    counts[status] = counts.get(status, 0) + 1
                for scope in page.get("scopes", []):
                    for plane in scope.get("planes", []):
                        self.add_scope(scope, plane)

    def add_scope(self, scope, plane):
        key = json.dumps([scope.get("scope"), plane.get("plane")], sort_keys=True)
        if key not in self.scopes:
            if len(self.scopes) >= MAX_SCOPES:
                self.value["scope_observations_skipped_at_limit"] += 1
                return
            entry = {"scope": scope.get("scope"), "plane": plane.get("plane"), "samples": 0,
                     "baseline_changes": 0, "last_interval": None, "latest": None}
            self.scopes[key] = {"entry": entry, "before": None}
            self.value["scope_planes"].append(entry)
        state = self.scopes[key]
        entry = state["entry"]
        before = state["before"]
        if before:
            delta = traffic_delta(before[0], scope, before[1], plane)
            entry["last_interval"] = delta
            if delta is None:
                entry["baseline_changes"] += 1
        entry["samples"] += 1
        entry["latest"] = {key: scope.get(key) for key in (
            "core_instance_id", "config_revision", "generation", "stats_epoch", "sampled_at_unix_ms",
        )}
        entry["latest"].update(accounting_basis=plane.get("accounting_basis"), counters=plane.get("counters"),
                               available_metrics=plane.get("available_metrics", []))
        state["before"] = (scope, plane)

    def finish(self):
        result = json.loads(json.dumps(self.value))
        cpu = result["cpu_percent_single_core"]
        cpu["sample_mean"] = cpu.pop("sum") / cpu["count"] if cpu["count"] else None
        if "end" not in result:
            result["limitations"].append("capture_has_no_end_record")
        return result


def summarize(path):
    summary = Summary()
    consumed = 0
    with path.open("rb") as source:
        while line := source.readline(MAX_FRAME_BYTES + 65537):
            consumed += len(line)
            if len(line) > MAX_FRAME_BYTES + 65536 or consumed > 64 * 1024 * 1024:
                raise ValueError("observation file exceeds bounded reader limits")
            if not line.endswith(b"\n"):
                summary.value["limitations"].append("truncated_final_record")
                break
            summary.add(json.loads(line))
    return summary.finish()
