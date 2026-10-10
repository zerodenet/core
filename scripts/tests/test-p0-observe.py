#!/usr/bin/env python3
"""Finite offline tests: OS parsing, epoch safety, bounded/private output."""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from p0_observe.cli import capture, parser
from p0_observe.ipc import KernelSampler, QueryError, project_scope
from p0_observe.process import cpu_percent, cpu_seconds, parse_linux, parse_macos
from p0_observe.report import MAX_SCOPES, Summary, summarize, traffic_delta
from p0_observe.storage import Journal, LimitReached


def scope():
    return {
        "scope": {"kind": "endpoint", "endpoint_id": "example"},
        "core_instance_id": "instance-a", "config_revision": 1, "generation": 4,
        "stats_epoch": "epoch-a", "sampled_at_monotonic_ns": 1_000_000_000,
        "planes": [{"plane": "inner", "accounting_basis": "plaintext_ip_boundary",
                    "available_metrics": ["rx_bytes", "tx_bytes"],
                    "counters": {"rx_bytes": 0, "tx_bytes": None, "rx_packets": None}}],
    }


class ProcessTests(unittest.TestCase):
    def test_macos_cpu_is_cumulative_time_and_rss_is_bytes(self):
        facts = parse_macos("Thu Oct  8 09:00:39 2026  1-02:03:04.25 1024")
        self.assertEqual(facts["cpu_seconds"], 93784.25)
        self.assertEqual(facts["rss_bytes"], 1048576)
        self.assertIsNone(facts["threads"])
        self.assertIsNone(facts["context_switches"])
        self.assertEqual(cpu_seconds("62:52.18"), 3772.18)

    def test_linux_fields_survive_spaces_and_parentheses_in_comm(self):
        fields = ["0"] * 22
        for i, value in {0: "S", 11: "120", 12: "30", 17: "8", 19: "345", 21: "100"}.items():
            fields[i] = value
        facts = parse_linux("7 (name with ) spaces) " + " ".join(fields),
                            "voluntary_ctxt_switches:\t12\n", 100, 4096)
        self.assertEqual(facts["cpu_seconds"], 1.5)
        self.assertEqual(facts["threads"], 8)
        self.assertEqual(facts["rss_bytes"], 409600)
        self.assertEqual(facts["context_switches"]["voluntary_ctxt_switches"], 12)
        self.assertIsNone(facts["context_switches"]["nonvoluntary_ctxt_switches"])

    def test_cpu_allows_multiple_cores_and_rejects_identity_or_clock_change(self):
        before = {"identity": "a", "cpu_seconds": 5, "captured_at_monotonic_ns": 1_000_000_000}
        after = {"identity": "a", "cpu_seconds": 9, "captured_at_monotonic_ns": 3_000_000_000}
        self.assertEqual(cpu_percent(before, after), 200)
        self.assertIsNone(cpu_percent(None, after))
        self.assertIsNone(cpu_percent(before, {**after, "identity": "b"}))
        self.assertIsNone(cpu_percent(after, before))


class StatisticsTests(unittest.TestCase):
    def test_zero_is_measured_and_null_stays_unavailable(self):
        first, last = scope(), scope()
        last["sampled_at_monotonic_ns"] += 2_000_000_000
        last["planes"][0]["counters"]["rx_bytes"] = 200
        delta = traffic_delta(first, last, first["planes"][0], last["planes"][0])
        self.assertEqual(delta["per_second"]["rx_bytes"], 100)
        self.assertIsNone(delta["per_second"]["tx_bytes"])
        last["planes"][0]["available_metrics"] = []
        delta = traffic_delta(first, last, first["planes"][0], last["planes"][0])
        self.assertIsNone(delta["per_second"]["rx_bytes"])

    def test_reset_generation_revision_restart_and_decrease_never_make_negative_rates(self):
        first, last = scope(), scope()
        first["planes"][0]["counters"]["rx_bytes"] = 100
        last["sampled_at_monotonic_ns"] += 1_000_000_000
        for key, value in (("stats_epoch", "b"), ("generation", 5), ("config_revision", 2),
                           ("core_instance_id", "b")):
            with self.subTest(key=key):
                changed = {**last, key: value}
                self.assertIsNone(traffic_delta(first, changed, first["planes"][0], changed["planes"][0]))
        result = traffic_delta(first, last, first["planes"][0], last["planes"][0])
        self.assertIsNone(result["per_second"]["rx_bytes"])

    def test_projection_excludes_unknown_configuration_and_retains_identity(self):
        raw = {**scope(), "private_key": "secret", "config": {"password": "secret"}}
        raw["scope"]["secret"] = "secret"
        output = json.dumps(project_scope(raw))
        self.assertNotIn("secret", output)
        self.assertIn("epoch-a", output)

    def test_many_deleted_identities_keep_summary_state_bounded(self):
        summary = Summary()
        for i in range(MAX_SCOPES + 10):
            value = scope()
            value["scope"]["endpoint_id"] = str(i)
            summary.add_scope(value, value["planes"][0])
        self.assertEqual(len(summary.scopes), MAX_SCOPES)
        self.assertEqual(summary.finish()["scope_observations_skipped_at_limit"], 10)

    def test_discovery_paging_and_stale_registry_restart_without_per_endpoint_queries(self):
        health = {"core_instance_id": "a", "config_revision": 2}
        page = {**health, "registry_revision": 3, "next_offset": 64, "total": 65, "scopes": []}
        calls = []

        def ask(path, pid, name, params, timeout):
            calls.append((name, params))
            if name == "health":
                return health
            if name == "capabilities":
                return {"traffic_statistics": {"queries": ["traffic_stats"]}}
            if name == "traffic_stats":
                if params["offset"]:
                    raise QueryError("conflict")
                return page
            return {}

        with patch("p0_observe.ipc.query", side_effect=ask):
            sampler = KernelSampler("socket", 7, 2)
            self.assertEqual(sampler.read()["traffic_status"], "available")
            self.assertEqual(sampler.read()["traffic_status"], "conflict")
            self.assertEqual(sampler.offset, 0)
        traffic = [p for name, p in calls if name == "traffic_stats"]
        self.assertEqual(len(traffic), 2)
        self.assertEqual(traffic[1]["expected_registry_revision"], 3)
        self.assertEqual(traffic[1]["limit"], 64)

    def test_capability_absence_keeps_legacy_stats_without_guessing(self):
        def ask(path, pid, name, params, timeout):
            return {"core_instance_id": "a", "config_revision": 1} if name == "health" else {}
        with patch("p0_observe.ipc.query", side_effect=ask) as calls:
            value = KernelSampler("socket", 7, 2).read()
        self.assertEqual(value["traffic_status"], "unsupported")
        self.assertFalse(any(c.args[2] == "traffic_stats" for c in calls.call_args_list))

    def test_query_capture_never_merges_two_instances(self):
        health_count = 0

        def ask(path, pid, name, params, timeout):
            nonlocal health_count
            if name == "health":
                health_count += 1
                return {"core_instance_id": "a" if health_count == 1 else "b", "config_revision": 1}
            return {}
        with patch("p0_observe.ipc.query", side_effect=ask):
            with self.assertRaisesRegex(QueryError, "core_instance_changed"):
                KernelSampler("socket", 7, 2).read()


class StorageTests(unittest.TestCase):
    def test_cap_has_complete_end_record_and_old_runs_are_never_overwritten(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "run"
            journal = Journal(directory, 8192)
            journal.write({"type": "start"})
            with self.assertRaises(LimitReached):
                journal.write({"type": "sample", "padding": "x" * 5000})
            journal.finish({"type": "end", "reason": "log_size_limit"})
            self.assertLessEqual(journal.path.stat().st_size, 8192)
            self.assertEqual(journal.path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
            self.assertEqual(summarize(journal.path)["end"]["reason"], "log_size_limit")
            with self.assertRaises(FileExistsError):
                Journal(directory, 8192)

    def test_truncated_capture_is_reported_and_metrics_not_fabricated(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "samples.jsonl"
            path.write_bytes(b'{"type":"sample","process":{"rss_bytes":0}}\n{"type":')
            result = summarize(path)
        self.assertEqual(result["rss_bytes_sampled_max"], 0)
        self.assertIsNone(result["cpu_percent_single_core"]["sample_mean"])
        self.assertIn("truncated_final_record", result["limitations"])
        self.assertIn("capture_has_no_end_record", result["limitations"])

    def test_capture_stops_on_reused_pid_without_writing_mixed_sample(self):
        with tempfile.TemporaryDirectory() as root:
            args = parser().parse_args(["capture", "--pid", "7", "--duration", "1",
                                        "--output-dir", str(Path(root) / "run")])
            facts = {"identity": "a", "cpu_seconds": 0, "rss_bytes": 1,
                     "captured_at_monotonic_ns": 1}
            with patch("p0_observe.cli.sample_process", side_effect=[facts, {**facts, "identity": "b"}]):
                self.assertEqual(capture(args), 2)
            result = json.loads((args.output_dir / "summary.json").read_text())
            self.assertEqual(result["samples"], 0)
            self.assertEqual(result["end"]["reason"], "process_identity_changed")


if __name__ == "__main__":
    unittest.main()
