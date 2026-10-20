#!/usr/bin/env python3
"""Actual Combined Direct measurement. No builds, network changes, or public peers."""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import time

from kernel import Kernel
from peers import Peers, config
from proc_sample import Sampler


def write(path, data):
    path.write_text(json.dumps(data, indent=2) + '\n')


def cgroup():
    root = Path('/sys/fs/cgroup')
    return {name: (root / name).read_text().strip() for name in
            ('cpu.max', 'memory.max', 'memory.swap.max', 'memory.peak', 'memory.events', 'cpu.stat')}


def exchange(client, relay, payload):
    started = time.monotonic_ns()
    record = {'sent_bytes': len(payload), 'sent_payload_hex': payload[:32].hex(),
              'monotonic_ns': started, 'success': False}
    try:
        assert client.sendto(payload, relay) == len(payload)
        data, source = client.recvfrom(65535)
        record.update({'received_bytes': len(data), 'received_payload_hex': data[:32].hex(),
                       'relay_source': source, 'success': data == payload})
        if data != payload:
            record['error'] = 'received payload differs from actual sent payload'
    except (OSError, TimeoutError) as error:
        record['error'] = repr(error)
    record['elapsed_ns'] = time.monotonic_ns() - started
    return record


def burst(client, relay, size, count):
    records = {'sent': 0, 'received': 0, 'unique_received': 0, 'errors': []}
    expected = set()
    for number in range(count):
        payload = number.to_bytes(4, 'big') + b'B' * (size - 4)
        try:
            client.sendto(payload, relay)
            records['sent'] += 1
            expected.add(payload)
        except OSError as error:
            records['errors'].append(repr(error))
    received = set()
    until = time.monotonic() + 1
    client.settimeout(.05)
    while time.monotonic() < until and len(received) < len(expected):
        try:
            payload, _ = client.recvfrom(65535)
            records['received'] += 1
            if payload in expected:
                received.add(payload)
            else:
                records['errors'].append('unmatched response payload')
        except socket.timeout:
            pass
    client.settimeout(2)
    records['unique_received'] = len(received)
    records['not_observed'] = len(expected - received)
    return records


def trial(binary, output, domain, rotations, hold, payload_size, interval):
    peers = Peers()
    kernel = None
    sampler = None
    client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client.bind(('127.0.0.1', 0))
    client.settimeout(2)
    result = {'profile': 'direct_combined_fixed_domain' if domain else 'direct_combined_literal',
              'actual_source_peer': client.getsockname(), 'configured_target_count': 1,
              'policy_rotations_requested': rotations, 'payload_size': payload_size,
              'source_peer_receive_buffer': client.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF),
              'steps': [], 'tokio_tasks_peak': None, 'user_queue_bytes_peak': None,
              'native_open_attempts': None,
              'network_generation_rotation': {'status': 'not_measured',
                  'reason': 'fixed CLI has no safe hook; no TUN/route/system changes'},
              'actual_peer_task_id': None}
    try:
        kernel = Kernel(binary, config(peers, domain), output)
        sampler = Sampler(kernel.process, interval)
        result.update({'kernel_pid': kernel.process.pid, 'relay': kernel.relay,
                       'initial_listener_inode': kernel.listener_inode})
        sampler.phase = 'ready_idle'
        time.sleep(hold)
        for number in range(rotations + 1):
            family = ('only_ipv4' if number % 2 else 'auto') if not domain else (
                'only_ipv6' if number % 2 else 'only_ipv4')
            phase = f'policy_{number:03d}_{family}'
            sampler.phase = phase
            step = {'phase': phase, 'family': family, 'rotation': number,
                    'dns_ipv4_answer': peers.ipv4}
            if domain:
                peers.ipv4 = '127.0.0.1' if number % 4 < 2 else '127.0.0.2'
                step['dns_ipv4_answer'] = peers.ipv4
            if number or domain:
                candidate = copy.deepcopy(kernel.config)
                candidate['outbounds'][0]['dial'] = {'address_family': family}
                applied = kernel.apply(candidate)
                step['apply'] = applied
                if applied.get('ok'):
                    kernel.config = candidate
            step['exchanges'] = [exchange(client, kernel.relay,
                f'P{number:04d}-{packet:04d}'.encode().ljust(payload_size, b'x'))
                for packet in range(3)]
            step['active_flows'] = kernel.query('active_flows')
            time.sleep(hold)
            result['steps'].append(step)
            print(f'{result["profile"]} {phase}: '
                  f'{sum(item["success"] for item in step["exchanges"])}/3 payloads', flush=True)
        sampler.phase = 'ingress_queue_boundary_31_32_33'
        result['bursts'] = {str(count): burst(client, kernel.relay, payload_size, count)
                            for count in (31, 32, 33)}
        time.sleep(hold)
        sampler.phase = 'post_rotation_idle'
        time.sleep(hold)
        result['final_runtime'] = kernel.query('runtime')
    except BaseException as error:
        result['error'] = repr(error)
    finally:
        if kernel:
            if sampler:
                sampler.phase = 'shutdown_with_peer_live'
            result['shutdown'] = kernel.finish()
        if sampler:
            result['samples'] = sampler.finish()
            result['sampler_errors'] = sampler.errors
        client.close()
        peers.close()
        result['echo_packets'] = peers.packets
        result['dns_queries'] = peers.queries
        output.mkdir(parents=True, exist_ok=True)
        write(output / 'raw.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--rotations', type=int, default=16)
    parser.add_argument('--trials', type=int, default=3)
    parser.add_argument('--hold', type=float, default=.15)
    parser.add_argument('--sample-interval', type=float, default=.01)
    parser.add_argument('--payload-bytes', type=int, default=1024)
    args = parser.parse_args()
    assert 0 <= args.rotations <= 1024 and 1 <= args.trials <= 10
    assert 16 <= args.payload_bytes <= 65507
    args.output.mkdir(parents=True, exist_ok=True)
    before = cgroup()
    assert before['cpu.max'] == '100000 100000', before
    assert before['memory.max'] == '1073741824' and before['memory.swap.max'] == '0', before
    result = {'limits_before': before, 'command': os.sys.argv,
              'uname': list(os.uname()), 'python': os.sys.version,
              'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              'build_info': subprocess.run([str(args.binary), 'build-info'],
                  capture_output=True, text=True, check=True).stdout,
              'measurement_scope': 'Combined Direct CLI only; harness/echo/DNS share cgroup, process metrics separate',
              'unknowns': ['Tokio task census', 'user-space queue/heap attribution',
                           'exact native syscall open-attempt count', 'safe forced egress-generation rotation'],
              'trials': []}
    for number in range(args.trials):
        for domain in (False, True):
            result['trials'].append(trial(args.binary,
                args.output / f'trial-{number}-{"domain" if domain else "literal"}',
                domain, args.rotations, args.hold, args.payload_bytes, args.sample_interval))
            write(args.output / 'manifest.json', result)
    result['limits_after'] = cgroup()
    write(args.output / 'manifest.json', result)
    if any(trial.get('error') or trial.get('sampler_errors') for trial in result['trials']):
        raise SystemExit('measurement failures retained in raw output')


if __name__ == '__main__':
    main()
