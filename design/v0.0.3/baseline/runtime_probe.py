"""Disposable real-kernel IPC and SOCKS fixtures, based on existing process tests.

Input shape: tests/management_lifecycle.rs and tests/live_status.rs at the
recorded source commit. Only synthetic identities and loopback peers are used.
"""

import copy
import json
import os
from pathlib import Path
import socket
import socketserver
import subprocess
import tempfile
import threading
import time


def free_port():
    with socket.socket() as stream:
        stream.bind(('127.0.0.1', 0))
        return stream.getsockname()[1]


def read_exact(stream, count):
    output = bytearray()
    while len(output) < count:
        part = stream.recv(count - len(output))
        if not part:
            raise EOFError(f'expected {count} bytes, received {len(output)}')
        output.extend(part)
    return bytes(output)


class EchoHandler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            while data := self.request.recv(65536):
                self.request.sendall(data)
        except (ConnectionError, OSError):
            pass


class EchoServer(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self):
        super().__init__(('127.0.0.1', 0), EchoHandler)
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.shutdown()
        self.server_close()
        self.thread.join(timeout=3)


class UdpEchoHandler(socketserver.BaseRequestHandler):
    def handle(self):
        payload, stream = self.request
        stream.sendto(payload, self.client_address)


class UdpEchoServer(socketserver.UDPServer):
    def __init__(self):
        super().__init__(('127.0.0.1', 0), UdpEchoHandler)
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.shutdown()
        self.server_close()
        self.thread.join(timeout=3)


class UdpAssociation:
    def __init__(self, control, relay, destination):
        self.control = control
        self.stream = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.stream.bind(('127.0.0.1', 0))
        self.stream.settimeout(5)
        self.relay = ('127.0.0.1' if relay[0] == '0.0.0.0' else relay[0], relay[1])
        self.header = bytes([0, 0, 0, 1]) + socket.inet_aton(destination[0]) + destination[1].to_bytes(2, 'big')

    def exchange(self, payload):
        self.stream.sendto(self.header + payload, self.relay)
        response, _ = self.stream.recvfrom(65536)
        assert response[:10] == self.header and response[10:] == payload

    def close(self):
        self.stream.close()
        self.control.close()


class DisposableKernel:
    def __init__(self, binary, directory=None):
        self.temporary = tempfile.TemporaryDirectory(prefix='zero-baseline-', dir='/tmp')
        self.root = Path(self.temporary.name)
        self.port = free_port()
        self.endpoint = self.root / 'control.sock'
        self.config = {
            'schema_version': 1,
            'inbounds': [{'tag': 'baseline-socks',
                          'listen': {'address': '127.0.0.1', 'port': self.port},
                          'protocol': {'type': 'socks5'}}],
            'outbounds': [],
            'route': {'rules': [], 'final': {'type': 'direct'}},
        }
        self.path = self.root / 'config.json'
        self.path.write_text(json.dumps(self.config))
        self.environment = {**os.environ, 'ZERO_DNS_STATE_DIR': str(self.root / 'dns'),
                            'NO_COLOR': '1'}
        self.log = (self.root / 'kernel.log').open('wb')
        self.process = subprocess.Popen(
            [str(binary), 'run', '--parent-lifetime-stdin', '--control-socket',
             str(self.endpoint), str(self.path)],
            stdin=subprocess.PIPE, stdout=self.log, stderr=self.log,
            env=self.environment,
        )
        self.frames = []
        self.stop_result = None
        self.directory = Path(directory) if directory else None
        deadline = time.monotonic() + 20
        while True:
            if self.process.poll() is not None:
                self.finish()
                raise RuntimeError('kernel exited before readiness')
            try:
                with socket.create_connection(('127.0.0.1', self.port), timeout=.2):
                    pass
                if self.endpoint.is_socket():
                    break
            except OSError:
                pass
            if time.monotonic() >= deadline:
                self.finish()
                raise TimeoutError('kernel readiness deadline')
            time.sleep(.02)

    def request(self, frame, label=None):
        started = time.monotonic_ns()
        request = {'id': f'baseline-{len(self.frames)}', **frame}
        with socket.socket(socket.AF_UNIX) as stream:
            stream.settimeout(15)
            stream.connect(str(self.endpoint))
            stream.sendall(json.dumps(request).encode() + b'\n')
            response = json.loads(stream.makefile('rb').readline())
        assert response.get('id') == request['id'], response
        self.frames.append({'label': label, 'request': request, 'response': response,
                            'elapsed_ns': time.monotonic_ns() - started})
        return response

    def query(self, name):
        params = {'filter': {}} if name in {'active_flows', 'recent_flows'} else {}
        return self.request({'type': 'query', 'request': {name: params}}, name)

    def command(self, method, params, label=None):
        return self.request({'type': 'command', 'method': method, 'params': params}, label or method)

    def apply(self, config, persistent=False):
        return self.command('config.apply' if persistent else 'config.apply_runtime',
                            {'config': config})

    def socks_request(self, command, destination):
        stream = socket.create_connection(('127.0.0.1', self.port), timeout=10)
        stream.settimeout(10)
        stream.sendall(bytes([5, 1, 0]))
        assert read_exact(stream, 2) == bytes([5, 0])
        host, port = destination
        stream.sendall(bytes([5, command, 0, 1]) + socket.inet_aton(host) + port.to_bytes(2, 'big'))
        header = read_exact(stream, 4)
        assert header[:2] == bytes([5, 0]), header
        if header[3] == 1:
            bound = read_exact(stream, 6)
            endpoint = (socket.inet_ntoa(bound[:4]), int.from_bytes(bound[4:], 'big'))
        elif header[3] == 4:
            bound = read_exact(stream, 18)
            endpoint = (socket.inet_ntop(socket.AF_INET6, bound[:16]), int.from_bytes(bound[16:], 'big'))
        elif header[3] == 3:
            bound = read_exact(stream, read_exact(stream, 1)[0] + 2)
            endpoint = (bound[:-2].decode(), int.from_bytes(bound[-2:], 'big'))
        else:
            raise AssertionError(header)
        return stream, endpoint

    def socks_stream(self, destination):
        return self.socks_request(1, destination)[0]

    def socks_udp_association(self, destination):
        control, relay = self.socks_request(3, ('0.0.0.0', 0))
        return UdpAssociation(control, relay, destination)

    def changed_rules(self, sequence):
        candidate = copy.deepcopy(self.config)
        candidate['route']['rules'] = [{
            'condition': {'type': 'domain', 'values': [f'unused-{sequence}.invalid']},
            'action': {'type': 'reject'},
        }]
        return candidate

    def finish(self):
        if self.stop_result is not None:
            return self.stop_result
        started = time.monotonic_ns()
        graceful = True
        self.process.stdin.close()
        try:
            code = self.process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            graceful = False
            self.process.terminate()
            try:
                code = self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                code = self.process.wait(timeout=3)
        self.log.close()
        self.stop_result = {'graceful': graceful, 'exit_code': code,
                            'elapsed_ns': time.monotonic_ns() - started}
        if self.directory:
            self.directory.mkdir(parents=True, exist_ok=True)
            (self.directory / 'ipc-frames.json').write_text(json.dumps(self.frames, indent=2) + '\n')
            (self.directory / 'runtime-input.json').write_text(json.dumps(self.config, indent=2) + '\n')
            (self.directory / 'shutdown.json').write_text(json.dumps(self.stop_result, indent=2) + '\n')
            (self.directory / 'runtime-output.txt').write_bytes((self.root / 'kernel.log').read_bytes())
        self.temporary.cleanup()
        return self.stop_result

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.finish()
