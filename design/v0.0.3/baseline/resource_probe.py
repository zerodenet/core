#!/usr/bin/env python3
"""Measure a built kernel inside an independently enforced 1 CPU/1 GiB cgroup.

The harness/echo peer share that cgroup; kernel RSS/threads/FD are sampled
separately. Compilation and reference protocol processes are excluded.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import copy
import hashlib
import json
import math
from pathlib import Path
import statistics
import threading
import time

from runtime_probe import DisposableKernel, EchoServer, UdpEchoServer, read_exact


class Sampler:
    def __init__(self, process):
        self.process = process
        self.pid = process.pid
        self.samples = []
        self.stop = threading.Event()
        self.phase = 'startup'
        self.error = None
        self.missing_memory_samples = 0
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        root = Path('/proc') / str(self.pid)
        while not self.stop.is_set():
            try:
                status = {}
                for line in (root / 'status').read_text().splitlines():
                    key, _, value = line.partition(':')
                    if key == 'State' and value.strip().startswith(('Z', 'X')):
                        return
                    if key in {'VmRSS', 'VmHWM', 'Threads'}:
                        status[key] = int(value.strip().split()[0])
                if not {'VmRSS', 'VmHWM', 'Threads'}.issubset(status):
                    # Linux can clear mm before the status State becomes Z.
                    # Retry only this transition, bounded while the process lives.
                    if self.process.poll() is not None:
                        return
                    self.missing_memory_samples += 1
                    if self.missing_memory_samples > 50:
                        raise RuntimeError('live process repeatedly missing procfs memory fields')
                    self.stop.wait(.01)
                    continue
                self.samples.append({'monotonic_ns': time.monotonic_ns(), 'phase': self.phase,
                                     'rss_bytes': status['VmRSS'] * 1024,
                                     'high_water_rss_bytes': status['VmHWM'] * 1024,
                                     'threads': status['Threads'],
                                     'fds': len(list((root / 'fd').iterdir()))})
            except (FileNotFoundError, ProcessLookupError):
                break
            except Exception as error:
                self.error = repr(error)
                break
            self.stop.wait(.02)

    def finish(self):
        self.stop.set()
        self.thread.join(timeout=2)
        assert self.error is None, self.error
        return self.samples


def cgroup_limits():
    root = Path('/sys/fs/cgroup')
    cpu = (root / 'cpu.max').read_text().split()
    memory = int((root / 'memory.max').read_text())
    assert cpu[0] != 'max' and int(cpu[0]) / int(cpu[1]) == 1, cpu
    assert memory == 1073741824, memory
    return {'cpu.max': ' '.join(cpu), 'memory.max': memory,
            'memory.swap.max': (root / 'memory.swap.max').read_text().strip()}


def transfer(stream, payload, rounds):
    elapsed = []
    for _ in range(rounds):
        started = time.monotonic_ns()
        stream.sendall(payload)
        assert read_exact(stream, len(payload)) == payload
        elapsed.append(time.monotonic_ns() - started)
    return elapsed


def udp_transfer(association, payload, rounds):
    elapsed = []
    for _ in range(rounds):
        started = time.monotonic_ns()
        association.exchange(payload)
        elapsed.append(time.monotonic_ns() - started)
    return elapsed


def trial(binary, number, profile):
    echo = EchoServer()
    udp_echo = UdpEchoServer()
    started = time.monotonic_ns()
    kernel = DisposableKernel(binary)
    sampler = Sampler(kernel.process)
    result = {'trial': number, 'startup_ns': time.monotonic_ns() - started}
    streams = []
    associations = []
    try:
        sampler.phase = 'idle'
        time.sleep(profile['idle_seconds'])
        result['connect_latency_ns'] = []
        sampler.phase = 'connect'
        for _ in range(profile['streams']):
            started = time.monotonic_ns()
            streams.append(kernel.socks_stream(echo.server_address))
            result['connect_latency_ns'].append(time.monotonic_ns() - started)
        sampler.phase = 'steady_transfer'
        payload = bytes(range(256)) * (profile['payload_bytes'] // 256)
        started = time.monotonic_ns()
        with ThreadPoolExecutor(max_workers=profile['streams']) as executor:
            futures = [executor.submit(transfer, stream, payload, profile['rounds']) for stream in streams]
            latencies = [latency for future in futures for latency in future.result()]
        elapsed = time.monotonic_ns() - started
        transferred = len(streams) * len(payload) * profile['rounds']
        result['transfer'] = {'bytes_per_direction': transferred, 'elapsed_ns': elapsed,
                              'bytes_per_second_per_direction': transferred * 1e9 / elapsed,
                              'median_roundtrip_ns': statistics.median(latencies),
                              'p95_roundtrip_ns': sorted(latencies)[math.ceil(len(latencies) * .95) - 1]}
        sampler.phase = 'udp_associations'
        for _ in range(profile['udp_associations']):
            associations.append(kernel.socks_udp_association(udp_echo.server_address))
        udp_payload = bytes(range(256)) * (profile['udp_payload_bytes'] // 256)
        started = time.monotonic_ns()
        with ThreadPoolExecutor(max_workers=profile['udp_associations']) as executor:
            futures = [executor.submit(udp_transfer, association, udp_payload, profile['udp_rounds'])
                       for association in associations]
            udp_latencies = [value for future in futures for value in future.result()]
        elapsed = time.monotonic_ns() - started
        transferred = len(associations) * len(udp_payload) * profile['udp_rounds']
        result['udp_transfer'] = {
            'bytes_per_direction': transferred, 'elapsed_ns': elapsed,
            'bytes_per_second_per_direction': transferred * 1e9 / elapsed,
            'median_roundtrip_ns': statistics.median(udp_latencies),
            'p95_roundtrip_ns': sorted(udp_latencies)[math.ceil(len(udp_latencies) * .95) - 1]}
        sampler.phase = 'ordinary_updates'
        update_times = []
        for sequence in range(profile['updates']):
            started = time.monotonic_ns()
            reply = kernel.apply(kernel.changed_rules(sequence))
            assert reply['ok'], reply
            update_times.append(time.monotonic_ns() - started)
            transfer(streams[sequence % len(streams)], b'ordinary-update-still-live', 1)
        result['ordinary_updates_ns'] = update_times
        sampler.phase = 'failed_rebuild'
        candidate = copy.deepcopy(kernel.config)
        candidate['inbounds'][0]['listen']['port'] = echo.server_address[1]
        failure = kernel.apply(candidate)
        assert not failure['ok'], failure
        result['failed_rebuild_reply'] = failure
        retained = 0
        for stream in streams:
            try:
                transfer(stream, b'after-failed-rebuild', 1)
                retained += 1
            except (OSError, EOFError):
                pass
        result['retained_streams_after_failed_rebuild'] = retained
        result['post_failure_runtime'] = kernel.query('runtime')
        sampler.phase = 'post_update_idle'
        time.sleep(profile['idle_seconds'])
        sampler.phase = 'shutdown_with_active_flows'
        result['shutdown'] = kernel.finish()
        assert result['shutdown']['graceful'] and result['shutdown']['exit_code'] == 0
    finally:
        kernel.finish()
        result['samples'] = sampler.finish()
        for stream in streams:
            stream.close()
        for association in associations:
            association.close()
        echo.close()
        udp_echo.close()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--trials', type=int, default=5)
    args = parser.parse_args()
    limits = cgroup_limits()
    profile = {'streams': 32, 'payload_bytes': 16384, 'rounds': 128,
               'udp_associations': 16, 'udp_payload_bytes': 1024, 'udp_rounds': 128,
               'updates': 50, 'idle_seconds': 2, 'trials': args.trials}
    result = {'limits': limits, 'profile': profile,
              'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              'measurement': {'sample_period_seconds': .02,
                              'kernel': 'procfs VmRSS/VmHWM, native threads and file descriptors',
                              'cgroup': 'includes Python harness and loopback echo peer',
                              'async_tasks': None,
                              'async_tasks_reason': 'old public executable has no authoritative task-count surface; threads are not Tokio tasks',
                              'qualification': 'TCP/UDP loopback resource baseline; Packet, capacity rejection and internal recovery fault injection remain separate'},
              'trials': []}
    for number in range(args.trials):
        result['trials'].append(trial(args.binary, number, profile))
        args.output.write_text(json.dumps(result, indent=2) + '\n')
        print(f'completed resource trial {number + 1}/{args.trials}', flush=True)
    root = Path('/sys/fs/cgroup')
    result['cgroup_memory_peak_bytes'] = int((root / 'memory.peak').read_text())
    result['cgroup_memory_events'] = (root / 'memory.events').read_text()
    args.output.write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    main()
