"""Existing Zero Query frames only; no commands, config or subscriptions."""
import json
import platform
import socket
import struct
import time

MAX_FRAME_BYTES = 2 * 1024 * 1024
METRICS = (
    "bytes_up", "bytes_down", "rx_bytes", "tx_bytes", "rx_packets", "tx_packets",
    "dropped_packets", "errors", "rx_dropped_packets", "tx_dropped_packets",
    "rx_errors", "tx_errors",
)


class QueryError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def query(path, pid, name, params=None, timeout=2):
    if name not in ("health", "capabilities", "stats", "traffic_stats"):
        raise QueryError("query_not_allowed")
    request = {"type": "query", "id": "p0-observer", "request": {name: params or {}}}
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        deadline = time.monotonic() + timeout
        sock.settimeout(timeout)
        sock.connect(str(path))
        if platform.system() == "Linux":
            peer = struct.unpack("3i", sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))[0]
        elif platform.system() == "Darwin":
            # Darwin sys/un.h: SOL_LOCAL=0, LOCAL_PEERPID=2.
            peer = struct.unpack("i", sock.getsockopt(0, 2, 4))[0]
        else:
            raise QueryError("peer_pid_unavailable")
        if peer != pid:
            raise QueryError("socket_pid_mismatch")
        sock.sendall(json.dumps(request).encode() + b"\n")
        data = bytearray()
        while b"\n" not in data:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise QueryError("query_timeout")
            sock.settimeout(remaining)
            block = sock.recv(min(65536, MAX_FRAME_BYTES + 1 - len(data)))
            if not block:
                raise QueryError("unexpected_eof")
            data.extend(block)
            if len(data) > MAX_FRAME_BYTES:
                raise QueryError("response_too_large")
    response = json.loads(data.split(b"\n", 1)[0])
    if not response.get("ok"):
        code = response.get("error", {}).get("code", "query_failed")
        # API messages may contain addresses/paths. Retain only known error codes.
        raise QueryError(code if code in ("unsupported", "conflict", "permission_denied") else "query_failed")
    if response.get("id") != "p0-observer":
        raise QueryError("response_id_mismatch")
    result = response.get("result", {}).get(name)
    if not isinstance(result, dict):
        raise QueryError("invalid_query_response")
    return result


def project_health(raw):
    return {key: raw.get(key) for key in (
        "engine_build_id", "core_instance_id", "config_revision", "started_at_unix_ms", "healthy",
    )}


def project_stats(raw):
    return {key: raw.get(key) for key in (
        "total_started", "active_sessions", "completed_sessions", "failed_sessions",
        "blocked_sessions", "bytes_up", "bytes_down",
    )}


def project_scope(raw):
    result = {key: raw.get(key) for key in (
        "core_instance_id", "config_revision", "generation", "stats_epoch",
        "epoch_started_at_unix_ms", "sampled_at_monotonic_ns", "sampled_at_unix_ms",
    )}
    result["scope"] = {key: raw.get("scope", {}).get(key) for key in (
        "kind", "tag", "endpoint_id", "peer_id", "name",
    ) if key in raw.get("scope", {})}
    result["activity"] = {key: raw.get("activity", {}).get(key) for key in (
        "active_stream_flows", "active_datagram_flows", "active_packet_routes",
    )}
    result["planes"] = [{
        "plane": plane.get("plane"), "accounting_basis": plane.get("accounting_basis"),
        "available_metrics": plane.get("available_metrics", []),
        "counters": {key: plane.get("counters", {}).get(key) for key in METRICS},
    } for plane in raw.get("planes", [])]
    return result


class KernelSampler:
    def __init__(self, path, pid, timeout):
        self.path, self.pid, self.timeout = path, pid, timeout
        self.offset, self.fence, self.instance = 0, {}, None
        self.capabilities = None
        self.traffic_supported = False

    def read(self):
        def ask(name, params=None):
            return query(self.path, self.pid, name, params, self.timeout)
        health = project_health(ask("health"))
        instance = health.get("core_instance_id")
        if not instance:
            raise QueryError("instance_identity_unavailable")
        if self.instance is not None and self.instance != instance:
            raise QueryError("core_instance_changed")
        self.instance = instance
        if self.capabilities is None:
            raw = ask("capabilities")
            self.capabilities = {"features": raw.get("features", []),
                                 "build_features": raw.get("build_features", []),
                                 "traffic_statistics": raw.get("traffic_statistics")}
            contract = raw.get("traffic_statistics") or {}
            self.traffic_supported = "traffic_stats" in contract.get("queries", [])
        value = {"health": health, "stats": project_stats(ask("stats")),
                 "traffic_page": None, "traffic_status": "unsupported"}
        if self.traffic_supported:
            try:
                page = ask("traffic_stats", {"offset": self.offset, "limit": 64, **self.fence})
                if (page.get("core_instance_id"), page.get("config_revision")) != (
                    instance, health.get("config_revision"),
                ):
                    raise QueryError("conflict")
                value["traffic_page"] = {key: page.get(key) for key in (
                    "core_instance_id", "config_revision", "registry_revision", "total", "next_offset",
                )}
                value["traffic_page"]["scopes"] = [project_scope(s) for s in page.get("scopes", [])]
                value["traffic_status"] = "available"
                self.offset = page.get("next_offset") or 0
                self.fence = {
                    "expected_core_instance_id": instance,
                    "expected_config_revision": page["config_revision"],
                    "expected_registry_revision": page["registry_revision"],
                } if self.offset else {}
            except QueryError as error:
                self.offset, self.fence = 0, {}
                value["traffic_status"] = error.code
        after = project_health(ask("health"))
        if after.get("core_instance_id") != instance:
            raise QueryError("core_instance_changed")
        if after.get("config_revision") != health.get("config_revision"):
            self.offset, self.fence = 0, {}
            raise QueryError("config_changed_during_capture")
        return value
