#!/usr/bin/env python3
"""Run native TUN operators on a routed veth, outside the Actions agent's network.

The test still uses real system DNS and internet peers. Only the test process
and its children enter this namespace; no live agent session is evicted to
make the original Source retirement gate pass.
"""
import ipaddress
import os
from pathlib import Path
import shutil
import subprocess
import sys


def run(*args):
    return subprocess.run(args, check=True)


def nameservers():
    for path in [Path("/run/systemd/resolve/resolv.conf"), Path("/etc/resolv.conf")]:
        if not path.exists():
            continue
        selected = []
        for line in path.read_text().splitlines():
            fields = line.split()
            if len(fields) >= 2 and fields[0] == "nameserver":
                address = ipaddress.ip_address(fields[1])
                if address.version == 4 and not address.is_loopback and not address.is_unspecified:
                    selected.append(str(address))
        if selected:
            return selected
    raise RuntimeError("no actual upstream system IPv4 DNS server for the routed namespace")


def main():
    if len(sys.argv) < 2:
        raise SystemExit("usage: tun-linux-network.py COMMAND [ARG ...]")
    namespace = f"zero-tun-{os.getpid()}"
    host_link = f"zt{os.getpid()}"
    resolver = Path("/etc/netns") / namespace
    forwarding = Path("/proc/sys/net/ipv4/ip_forward")
    previous_forwarding = forwarding.read_text()
    dns = nameservers()
    created = False
    rules = []
    try:
        run("ip", "netns", "add", namespace)
        created = True
        run("ip", "link", "add", host_link, "type", "veth", "peer", "name", "physical0", "netns", namespace)
        run("ip", "address", "add", "192.0.2.1/30", "dev", host_link)
        run("ip", "link", "set", host_link, "up")
        run("ip", "-n", namespace, "link", "set", "lo", "up")
        run("ip", "-n", namespace, "address", "add", "192.0.2.2/30", "dev", "physical0")
        run("ip", "-n", namespace, "link", "set", "physical0", "up")
        run("ip", "-n", namespace, "route", "add", "default", "via", "192.0.2.1")
        resolver.mkdir(parents=True)
        (resolver / "resolv.conf").write_text("".join(f"nameserver {address}\n" for address in dns))
        comment = ["-m", "comment", "--comment", namespace]
        for rule in [
            ["-t", "nat", "-I", "POSTROUTING", "-s", "192.0.2.2/32", *comment, "-j", "MASQUERADE"],
            ["-I", "FORWARD", "-i", host_link, *comment, "-j", "ACCEPT"],
            ["-I", "FORWARD", "-o", host_link, "-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", *comment, "-j", "ACCEPT"],
        ]:
            run("iptables", "-w", *rule)
            rules.append(rule)
        forwarding.write_text("1\n")
        print(f"actual TUN network: namespace={namespace}, interface=physical0, DNS={dns}", flush=True)
        result = subprocess.run(["ip", "netns", "exec", namespace, *sys.argv[1:]])
        remaining = subprocess.check_output(["ip", "netns", "pids", namespace], text=True).strip()
        if result.returncode == 0 and remaining:
            raise RuntimeError(f"successful operator left actual namespace processes alive: {remaining}")
        return result.returncode
    finally:
        failures = []
        for rule in reversed(rules):
            remove = ["-D" if value == "-I" else value for value in rule]
            try:
                run("iptables", "-w", *remove)
            except Exception as error:
                failures.append(error)
        try:
            forwarding.write_text(previous_forwarding)
        except Exception as error:
            failures.append(error)
        if created:
            try:
                run("ip", "netns", "delete", namespace)
            except Exception as error:
                failures.append(error)
        if resolver.exists():
            try:
                shutil.rmtree(resolver)
            except Exception as error:
                failures.append(error)
        if failures:
            raise RuntimeError(f"actual TUN fixture cleanup failed: {failures}")


if __name__ == "__main__":
    raise SystemExit(main())
