#!/usr/bin/env python3
"""Real routed DNS peer for the existing privileged Linux TUN operator.

The peer is outside the client's namespace and its connected subnet. Each
captured request must traverse TUN and the original marked physical socket;
the Rust operator also checks its actual scoped flows and close receipts.
"""
import json
import os
import socket
import subprocess
import sys
import threading


def ip(*arguments):
    subprocess.run(["ip", *arguments], check=True)


def main():
    namespace = f"zero-dns-{os.getpid()}"
    host_link = f"zd{os.getpid()}"
    peer_address = "203.0.113.53"
    stop = threading.Event()
    received = []
    failures = []
    server = None
    worker = None
    created = False
    try:
        ip("netns", "add", namespace)
        created = True
        ip("link", "add", host_link, "type", "veth", "peer", "name", "physical0",
           "netns", namespace)
        ip("address", "add", "192.0.2.1/30", "dev", host_link)
        ip("address", "add", f"{peer_address}/32", "dev", host_link)
        ip("link", "set", host_link, "up")
        ip("-n", namespace, "link", "set", "lo", "up")
        ip("-n", namespace, "address", "add", "192.0.2.2/30", "dev", "physical0")
        ip("-n", namespace, "link", "set", "physical0", "up")
        ip("-n", namespace, "route", "add", "default", "via", "192.0.2.1")
        server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server.bind((peer_address, 53))
        server.settimeout(0.1)

        def serve():
            while not stop.is_set():
                try:
                    packet, source = server.recvfrom(2048)
                except socket.timeout:
                    continue
                try:
                    # This fixture accepts the actual A question emitted by
                    # the Rust operator, retaining its transaction and question.
                    assert source[0] == "192.0.2.2", source
                    assert len(packet) >= 17 and packet[4:6] == b"\x00\x01"
                    end = 12
                    while packet[end]:
                        end += 1 + packet[end]
                    end += 5
                    assert end == len(packet) and packet[-4:] == b"\x00\x01\x00\x01"
                    response = (packet[:2] + b"\x81\x80\x00\x01\x00\x01\x00\x00\x00\x00"
                                + packet[12:] + b"\xc0\x0c\x00\x01\x00\x01\x00\x00\x00\x3c"
                                + b"\x00\x04\x7f\x00\x00\x01")
                    server.sendto(response, source)
                    received.append({"id": int.from_bytes(packet[:2], "big"), "source": source})
                except Exception as error:
                    failures.append(repr(error))
                    return

        worker = threading.Thread(target=serve)
        worker.start()
        environment = os.environ.copy()
        environment["ZERO_TUN_E2E_DNS_ADDR"] = f"{peer_address}:53"
        environment.pop("RUST_MIN_STACK", None)
        result = subprocess.run(["ip", "netns", "exec", namespace, *sys.argv[1:]],
                                env=environment)
        stop.set()
        worker.join()
        print("actual routed DNS peer: " + json.dumps({"received": received,
                                                       "failures": failures}), flush=True)
        if result.returncode:
            return result.returncode
        expected = [0x6fff, *range(0x7000, 0x7010), *range(0x7100, 0x7120)]
        if failures or [row["id"] for row in received] != expected:
            raise RuntimeError("actual DNS transactions were missing, duplicated, or reordered")
        return 0
    finally:
        stop.set()
        if worker is not None:
            worker.join()
        if server is not None:
            server.close()
        if created:
            subprocess.run(["ip", "netns", "delete", namespace], check=True)


if __name__ == "__main__":
    raise SystemExit(main())
