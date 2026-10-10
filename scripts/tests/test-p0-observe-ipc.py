#!/usr/bin/env python3
"""Real Unix IPC framing and finite CLI execution, using a local fake provider."""
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
from p0_observe.ipc import MAX_FRAME_BYTES, QueryError, query


class Server:
    def __init__(self, path, reply):
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.bind(str(path))
        self.socket.listen()
        self.socket.settimeout(0.1)
        self.reply = reply
        self.requests = []
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stopped.set()
        self.thread.join(timeout=2)
        self.socket.close()

    def run(self):
        while not self.stopped.is_set():
            try:
                peer, _ = self.socket.accept()
            except socket.timeout:
                continue
            with peer:
                peer.settimeout(2)
                data = bytearray()
                try:
                    while b"\n" not in data:
                        part = peer.recv(4096)
                        if not part:
                            break
                        data.extend(part)
                    if data:
                        request = json.loads(data)
                        self.requests.append(request)
                        self.reply(peer, request)
                except (OSError, ValueError):
                    pass  # Timeout/mismatch tests intentionally close early.


def frame(peer, request):
    name = next(iter(request["request"]))
    if name == "health":
        result = {"core_instance_id": "mock-core", "config_revision": 3,
                  "engine_build_id": "mock-build", "healthy": True}
    elif name == "capabilities":
        result = {"traffic_statistics": {"queries": ["traffic_stats"]}}
    elif name == "stats":
        result = {"bytes_up": 123, "active_sessions": 1}
    else:
        result = {"core_instance_id": "mock-core", "config_revision": 3,
                  "registry_revision": 5, "scopes": [], "total": 0, "next_offset": None}
    peer.sendall(json.dumps({"id": request["id"], "ok": True, "result": {name: result}}).encode() + b"\n")


@unittest.skipUnless(sys.platform in ("darwin", "linux"), "Unix process observation")
class IpcTests(unittest.TestCase):
    def setUp(self):
        # Keep the Unix socket name below sockaddr_un limits on macOS.
        self.temp = tempfile.TemporaryDirectory(prefix="p0-", dir="/tmp")
        self.root = Path(self.temp.name)
        self.socket = self.root / "ipc"

    def tearDown(self):
        self.temp.cleanup()

    def test_cli_collects_real_os_and_query_frames_and_reports_finite_end(self):
        with Server(self.socket, frame) as server:
            result = subprocess.run([
                sys.executable, "-B", str(SCRIPTS / "p0-observe.py"), "capture",
                "--pid", str(os.getpid()), "--socket", str(self.socket),
                "--duration", "1", "--output-dir", str(self.root / "capture"),
            ], capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        records = [json.loads(v) for v in (self.root / "capture/samples.jsonl").read_text().splitlines()]
        self.assertEqual([r["type"] for r in records], ["start", "sample", "end"])
        self.assertEqual(records[-1]["reason"], "duration_complete")
        self.assertGreater(records[1]["process"]["rss_bytes"], 0)
        self.assertEqual(records[1]["kernel"]["health"]["core_instance_id"], "mock-core")
        self.assertEqual(records[1]["kernel"]["stats"]["bytes_up"], 123)
        self.assertTrue(all(r["type"] == "query" for r in server.requests))
        names = [next(iter(r["request"])) for r in server.requests]
        self.assertEqual(names, ["health", "capabilities", "stats", "traffic_stats", "health"])
        self.assertEqual(json.loads((self.root / "capture/summary.json").read_text())["kernel_samples"], 1)

    def test_peer_pid_mismatch_is_rejected_before_any_request(self):
        with Server(self.socket, frame) as server:
            with self.assertRaisesRegex(QueryError, "socket_pid_mismatch"):
                query(self.socket, os.getpid() + 1, "stats")
        self.assertEqual(server.requests, [])

    def test_oversize_responses_have_bounded_read(self):
        def large(peer, request):
            peer.sendall(b"x" * (MAX_FRAME_BYTES + 1) + b"\n")
        with Server(self.socket, large):
            with self.assertRaisesRegex(QueryError, "response_too_large"):
                query(self.socket, os.getpid(), "stats")

    def test_error_messages_never_enter_output(self):
        def denied(peer, request):
            peer.sendall(json.dumps({"ok": False, "id": request["id"],
                                    "error": {"code": "permission_denied", "message": "secret-config"}}).encode() + b"\n")
        with Server(self.socket, denied):
            with self.assertRaises(QueryError) as raised:
                query(self.socket, os.getpid(), "stats")
        self.assertEqual(str(raised.exception), "permission_denied")

    def test_slow_provider_times_out_instead_of_blocking_capture(self):
        def slow(peer, request):
            time.sleep(0.3)
            frame(peer, request)
        started = time.monotonic()
        with Server(self.socket, slow):
            with self.assertRaises((TimeoutError, QueryError)):
                query(self.socket, os.getpid(), "stats", timeout=0.05)
        self.assertLess(time.monotonic() - started, 1)

    def test_signal_stops_sampler_and_flushes_summary_without_stopping_target(self):
        directory = self.root / "capture"
        process = subprocess.Popen([
            sys.executable, "-B", str(SCRIPTS / "p0-observe.py"), "capture",
            "--pid", str(os.getpid()), "--duration", "600", "--output-dir", str(directory),
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                path = directory / "samples.jsonl"
                if path.exists() and b'"type":"sample"' in path.read_bytes():
                    break
                time.sleep(0.02)
            process.terminate()
            stdout, stderr = process.communicate(timeout=3)
            self.assertEqual(process.returncode, 0, stderr)
            self.assertEqual(json.loads(stdout)["reason"], "signal_stop")
            result = json.loads((directory / "summary.json").read_text())
            self.assertEqual(result["end"]["reason"], "signal_stop")
            os.kill(os.getpid(), 0)  # The observed process is still alive.
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()


if __name__ == "__main__":
    unittest.main()
