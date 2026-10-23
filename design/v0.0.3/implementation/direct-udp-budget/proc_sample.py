"""Linux process-owned socket samples; threads/queues are not Tokio task/heap counts."""
import ipaddress
from pathlib import Path
import threading
import time


def sockets(pid, protocol):
    root = Path('/proc') / str(pid)
    owned = {}
    for fd in (root / 'fd').iterdir():
        try:
            link = fd.readlink().as_posix()
        except FileNotFoundError:
            continue
        if link.startswith('socket:['):
            owned[link[8:-1]] = int(fd.name)
    rows = []
    for family in ('', '6'):
        table = root / 'net' / (protocol + family)
        for line in table.read_text().splitlines()[1:]:
            parts = line.split()
            if len(parts) < 10 or parts[9] not in owned:
                continue
            local, port = parts[1].split(':')
            packed = bytes.fromhex(local)
            if family:
                packed = b''.join(packed[i:i + 4][::-1] for i in range(0, 16, 4))
            else:
                packed = packed[::-1]
            tx, rx = parts[4].split(':')
            rows.append({'fd': owned[parts[9]], 'inode': int(parts[9]),
                         'family': 6 if family else 4,
                         'local_ip': str(ipaddress.ip_address(packed)),
                         'local_port': int(port, 16), 'state': parts[3],
                         'tx_queue_kernel_bytes': int(tx, 16),
                         'rx_queue_kernel_bytes': int(rx, 16)})
    return rows


def snapshot(pid, phase):
    root = Path('/proc') / str(pid)
    status = {}
    for line in (root / 'status').read_text().splitlines():
        key, _, value = line.partition(':')
        if key in {'VmRSS', 'VmHWM', 'Threads'}:
            status[key] = int(value.strip().split()[0])
    if len(status) != 3:
        raise ProcessLookupError('process address space exited')
    return {'monotonic_ns': time.monotonic_ns(), 'phase': phase,
            'rss_bytes': status['VmRSS'] * 1024,
            'high_water_rss_bytes': status['VmHWM'] * 1024,
            'native_threads': status['Threads'],
            'fd_count': len(list((root / 'fd').iterdir())),
            'udp': sockets(pid, 'udp'), 'tcp': sockets(pid, 'tcp'),
            'tokio_tasks': None, 'user_queue_bytes': None,
            'native_open_attempts': None}


class Sampler:
    def __init__(self, process, interval):
        self.process, self.interval = process, interval
        self.phase = 'startup'
        self.samples, self.errors = [], []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        while not self.stop.is_set():
            try:
                self.samples.append(snapshot(self.process.pid, self.phase))
            except (FileNotFoundError, ProcessLookupError):
                if self.process.poll() is not None:
                    break
            except Exception as error:
                self.errors.append(repr(error))
                break
            self.stop.wait(self.interval)

    def finish(self):
        self.stop.set()
        self.thread.join(timeout=3)
        if self.thread.is_alive():
            self.errors.append('sampler did not exit')
        return self.samples
