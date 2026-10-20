"""Disposable CLI kernel, port-zero listener discovery and unchanged V1 IPC."""
import json
import os
from pathlib import Path
import socket
import subprocess
import time
from proc_sample import sockets


class Kernel:
    def __init__(self, binary, config, output):
        self.output = output
        self.config = config
        output.mkdir(parents=True)
        self.frames = []
        self.path = output / 'config.json'
        self.path.write_text(json.dumps(config, indent=2) + '\n')
        self.endpoint = Path('/tmp') / f'budget-{output.name}.sock'
        self.log = (output / 'kernel.log').open('wb')
        self.process = subprocess.Popen(
            [str(binary), 'run', '--parent-lifetime-stdin', '--control-socket',
             str(self.endpoint), str(self.path)], stdin=subprocess.PIPE,
            stdout=self.log, stderr=self.log,
            env={**os.environ, 'ZERO_DNS_STATE_DIR': str(output / 'dns-state'), 'NO_COLOR': '1'})
        self.shutdown = None
        until = time.monotonic() + 20
        try:
            while time.monotonic() < until:
                if self.process.poll() is not None:
                    raise RuntimeError(f'kernel exited before readiness: {self.process.returncode}')
                if self.endpoint.is_socket():
                    udp = [row for row in sockets(self.process.pid, 'udp')
                           if row['local_ip'] == '127.0.0.1']
                    if len(udp) == 1:
                        self.relay = ('127.0.0.1', udp[0]['local_port'])
                        self.listener_inode = udp[0]['inode']
                        return
                time.sleep(.01)
            raise TimeoutError('port-zero Combined UDP listener readiness')
        except BaseException:
            self.finish()
            raise

    def query(self, name):
        return self.request({'type': 'query', 'request': {name: {'filter': {}} if name == 'active_flows' else {}}})

    def apply(self, config):
        return self.request({'type': 'command', 'method': 'config.apply_runtime', 'params': {'config': config}})

    def request(self, frame):
        request = {'id': f'budget-{len(self.frames)}', **frame}
        started = time.monotonic_ns()
        with socket.socket(socket.AF_UNIX) as stream:
            stream.settimeout(10)
            stream.connect(str(self.endpoint))
            stream.sendall(json.dumps(request).encode() + b'\n')
            response = json.loads(stream.makefile('rb').readline())
        self.frames.append({'request': request, 'response': response,
                            'monotonic_ns': started, 'elapsed_ns': time.monotonic_ns() - started})
        assert response.get('id') == request['id'], response
        return response

    def finish(self):
        if self.shutdown is not None:
            return self.shutdown
        started = time.monotonic_ns()
        self.process.stdin.close()
        graceful = True
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
        self.shutdown = {'graceful': graceful, 'exit_code': code,
                         'elapsed_ns': time.monotonic_ns() - started}
        (self.output / 'ipc.json').write_text(json.dumps(self.frames, indent=2) + '\n')
        (self.output / 'shutdown.json').write_text(json.dumps(self.shutdown, indent=2) + '\n')
        return self.shutdown
