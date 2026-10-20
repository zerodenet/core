#!/usr/bin/env python3
"""Actual 16-source fixed-domain Combined Direct: concurrent packets and policy changes."""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import selectors
import socket
import subprocess
import threading
import time

from kernel import Kernel
from peers import Peers, config
from proc_sample import Sampler
from worker import cgroup, write


class Clients:
    def __init__(self, count):
        self.sockets = []
        self.selector = selectors.DefaultSelector()
        for number in range(count):
            stream = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            stream.bind(('127.0.0.1', 0))
            stream.setblocking(False)
            self.sockets.append(stream)
            self.selector.register(stream, selectors.EVENT_READ, number)

    def packet(self, phase, peer, number, size):
        return f'{phase}:{peer:04}:{number:04}:'.encode().ljust(size, b'f')

    def collect(self, expected, timeout=1):
        received, errors, duplicates = set(), [], 0
        until = time.monotonic() + timeout
        while time.monotonic() < until and len(received) < len(expected):
            for key, _ in self.selector.select(min(.01, max(0, until - time.monotonic()))):
                data, sender = key.fileobj.recvfrom(65535)
                identity = (key.data, data)
                if identity not in expected:
                    errors.append({'peer': key.data, 'unmatched_prefix': data[:48].hex(), 'sender': sender})
                elif identity in received:
                    duplicates += 1
                else:
                    received.add(identity)
        missing = [{'peer': peer, 'payload_prefix': data[:48].hex()}
                   for peer, data in sorted(expected - received)]
        return {'sent': len(expected), 'unique_received': len(received),
                'not_observed': len(missing), 'missing': missing,
                'duplicates': duplicates, 'errors': errors}

    def send(self, relay, phase, count, size):
        expected, errors = set(), []
        for number in range(count):
            for peer, stream in enumerate(self.sockets):
                payload = self.packet(phase, peer, number, size)
                try:
                    assert stream.sendto(payload, relay) == len(payload)
                    expected.add((peer, payload))
                except OSError as error:
                    errors.append({'peer': peer, 'error': repr(error)})
        return expected, errors

    def overlap(self, kernel, candidate, phase, size):
        expected, errors, sent_times = set(), [], []
        stop, ready = threading.Event(), threading.Event()

        def send():
            try:
                for number in range(128):
                    if stop.is_set():
                        break
                    for peer, stream in enumerate(self.sockets):
                        payload = self.packet(phase, peer, number, size)
                        try:
                            assert stream.sendto(payload, kernel.relay) == len(payload)
                            expected.add((peer, payload))
                            sent_times.append(time.monotonic_ns())
                        except OSError as error:
                            errors.append({'peer': peer, 'error': repr(error)})
                    ready.set()
                    stop.wait(.002)
            except BaseException as error:
                errors.append({'sender_thread': repr(error)})
                ready.set()

        thread = threading.Thread(target=send)
        thread.start()
        try:
            if not ready.wait(2):
                raise TimeoutError('actual source sender never ran')
            reply = kernel.apply(candidate)
            frame = kernel.frames[-1]
            time.sleep(.01)
        finally:
            stop.set()
            thread.join(timeout=3)
        if thread.is_alive():
            raise RuntimeError('actual sender failed to join')
        result = self.collect(expected)
        result['send_errors'] = errors
        result['apply'] = reply
        result['apply_started_ns'] = frame['monotonic_ns']
        result['apply_elapsed_ns'] = frame['elapsed_ns']
        result['actual_send_times_ns'] = sent_times
        result['sent_during_apply'] = sum(frame['monotonic_ns'] <= t <=
            frame['monotonic_ns'] + frame['elapsed_ns'] for t in sent_times)
        return result

    def close(self):
        self.selector.close()
        for stream in self.sockets:
            stream.close()


def trial(binary, output, number, payload_size, kernel_type=Kernel):
    peers, clients = Peers(), Clients(16)
    kernel = sampler = None
    result = {'profile': 'direct_combined_16_sources_fixed_domain',
        'trial': number, 'payload_bytes': payload_size,
        'actual_source_peers': [stream.getsockname() for stream in clients.sockets],
        'client_receive_buffers': [stream.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
                                   for stream in clients.sockets],
        'steps': [], 'tokio_task_peak': None, 'user_queue_bytes_peak': None,
        'exact_native_open_attempts': None, 'actual_peer_task_ids': None,
        'network_generation_rotation': 'not_measured: no safe CLI hook; no route/system changes'}
    try:
        kernel = kernel_type(binary, config(peers, True), output)
        sampler = kernel.sampler
        result.update({'initial_listener_inode': kernel.listener_inode,
                       'kernel_pid': kernel.process.pid, 'relay': kernel.relay})
        for rotation in range(17):
            phase = f'{number}-{rotation}'
            family = 'only_ipv6' if rotation % 2 else 'only_ipv4'
            peers.ipv4 = '127.0.0.1' if rotation % 4 < 2 else '127.0.0.2'
            candidate = copy.deepcopy(kernel.config)
            candidate['outbounds'][0]['dial'] = {'address_family': family}
            sampler.phase = f'rotation-{rotation}'
            step = {'rotation': rotation, 'family': family, 'dns_ipv4': peers.ipv4}
            if rotation:
                step['overlap'] = clients.overlap(kernel, candidate, phase + '-overlap', payload_size)
                applied = step['overlap']['apply']
            else:
                applied = kernel.apply(candidate)
                step['initial_apply'] = applied
            if applied.get('ok'):
                kernel.config = candidate
            expected, errors = clients.send(kernel.relay, phase + '-steady', 3, payload_size)
            step['steady'] = clients.collect(expected)
            step['steady']['send_errors'] = errors
            step['active_flows'] = kernel.query('active_flows')
            time.sleep(.1)
            result['steps'].append(step)
            if rotation in (0, 8, 16):
                kernel.memory.capture(f'after_rotation_{rotation}')
            print(f'trial{number} rotation{rotation}: steady '
                  f'{step["steady"]["unique_received"]}/{step["steady"]["sent"]}', flush=True)
        sampler.phase = 'burst_31_32_33_per_source'
        result['bursts'] = {}
        for count in (31, 32, 33):
            expected, errors = clients.send(kernel.relay, f'{number}-burst-{count}', count, payload_size)
            result['bursts'][str(count)] = clients.collect(expected)
            result['bursts'][str(count)]['send_errors'] = errors
            kernel.memory.capture(f'after_burst_{count}')
        time.sleep(.15)
        result['final_runtime'] = kernel.query('runtime')
    except BaseException as error:
        result['error'] = repr(error)
    finally:
        if kernel:
            if sampler:
                sampler.phase = 'shutdown_all_sources_live'
            result['shutdown'] = kernel.finish()
            result['memory_diagnostic'] = {'directory': 'memory', 'events': kernel.memory.events,
                'errors': kernel.memory.errors, 'readiness_confirmed_ns': kernel.readiness_confirmed_ns,
                'light_start_requested_ns': kernel.sampler.start_requested_ns,
                'light_thread_entered_ns': getattr(kernel.sampler, 'thread_entered_ns', None),
                'first_light_sample_ns': kernel.sampler.samples[0]['sample_started_ns'] if kernel.sampler.samples else None,
                'last_live_light_sample': kernel.sampler.samples[-1] if kernel.sampler.samples else None,
                'last_full_snapshot_before_shutdown': 'shutdown_begin',
                'scope': 'additive diagnostic; snapshots pause between phases and change sampling overhead'}
        if sampler:
            result['samples'] = sampler.finish()
            result['sampler_errors'] = sampler.errors
        clients.close()
        peers.close()
        result['echo_packets'] = peers.packets
        result['dns_queries'] = peers.queries
        write(output / 'raw.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--diagnostic-role', choices=('old', 'new'), required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--trials', type=int, default=3)
    parser.add_argument('--payload-bytes', type=int, default=1024)
    args = parser.parse_args()
    assert 1 <= args.trials <= 3 and 128 <= args.payload_bytes <= 65507
    args.output.mkdir(parents=True)
    before = cgroup()
    assert before['cpu.max'] == '100000 100000'
    assert before['memory.max'] == '1073741824' and before['memory.swap.max'] == '0'
    result = {'limits_before': before, 'command': os.sys.argv, 'uname': list(os.uname()),
        'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        'build_info': subprocess.run([str(args.binary), 'build-info'], capture_output=True,
                                     text=True, check=True).stdout,
        'measurement_scope': 'separate old/current31 additive smaps diagnostic; kernel PID separate from harness; not qualification',
        'trials': []}
    profile = json.loads((Path(__file__).parent / 'profile.json').read_text())
    assert args.trials == 3 and args.payload_bytes == 1024
    assert result['binary_sha256'] == profile['binary'][args.diagnostic_role]['sha256']
    actual_features = next(line.removeprefix('features: ').split(',')
        for line in result['build_info'].splitlines() if line.startswith('features: '))
    assert actual_features == profile['features'], 'actual binary feature mismatch'
    result['diagnostic_profile'] = profile
    for number in range(args.trials):
        result['trials'].append(trial(args.binary, args.output / f'trial-{number}', number, args.payload_bytes))
        write(args.output / 'manifest.json', result)
    result['limits_after'] = cgroup()
    write(args.output / 'manifest.json', result)
    if any(t.get('error') or t.get('sampler_errors') or t.get('memory_diagnostic', {}).get('errors') for t in result['trials']):
        raise SystemExit('measurement errors retained')


if __name__ == '__main__':
    main()
