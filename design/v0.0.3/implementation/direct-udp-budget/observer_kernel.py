"""Actual public-library observer process exposing the existing fan-in fixture API."""
import json
import os
from pathlib import Path
import select
import subprocess
import time
from proc_sample import sockets


class ObserverKernel:
    def __init__(self, binary, config, output, invalidate_network=False):
        self.output, self.config = output, config
        self.invalidates, self.epoch = invalidate_network, 0
        output.mkdir(parents=True)
        (output / 'config.json').write_text(json.dumps(config, indent=2) + '\n')
        self.log = (output / 'kernel.log').open('wb')
        self.frames, self.network_invalidations = [], []
        self.shutdown = self.task_observation = None
        self.process = subprocess.Popen([str(binary)], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=self.log, env={**os.environ, 'NO_COLOR': '1'})
        try:
            response = self.request({'op': 'start', 'config': config})
            assert response.get('ok'), response
            until = time.monotonic() + 20
            while time.monotonic() < until:
                if self.process.poll() is not None:
                    raise RuntimeError(f'library observer exited before readiness {self.process.returncode}')
                listeners = [r for r in sockets(self.process.pid, 'udp') if r['local_ip'] == '127.0.0.1']
                if len(listeners) == 1:
                    self.relay = ('127.0.0.1', listeners[0]['local_port'])
                    self.listener_inode = listeners[0]['inode']
                    self.request({'op': 'phase', 'phase': 'ready_idle'})
                    return
                time.sleep(.01)
            raise TimeoutError('actual port-zero library Combined listener readiness')
        except BaseException:
            self.finish()
            raise

    def read(self):
        ready, _, _ = select.select([self.process.stdout], [], [], 25)
        if not ready:
            raise TimeoutError('library observer did not return actual response')
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError('library observer stdout EOF before actual response')
        return json.loads(line)

    def request(self, command):
        command = {'id': f'observer-{len(self.frames)}', **command}
        started = time.monotonic_ns()
        self.process.stdin.write(json.dumps(command).encode() + b'\n')
        self.process.stdin.flush()
        response = self.read()
        self.frames.append({'request': command, 'response': response,
            'monotonic_ns': started, 'elapsed_ns': time.monotonic_ns() - started})
        assert response.get('id') == command['id'], response
        return response

    def apply(self, config):
        self.request({'op': 'phase', 'phase': f'policy_epoch_{self.epoch}'})
        if self.invalidates and self.epoch:
            result = self.request({'op': 'invalidate_network'})
            self.network_invalidations.append(result)
            assert result.get('ok'), result
        self.epoch += 1
        return self.request({'op': 'apply', 'config': config})

    def query(self, name):
        return self.request({'op': 'query', 'query': {name: {'filter': {}} if name == 'active_flows' else {}}})

    def finish(self):
        if self.shutdown is not None:
            return self.shutdown
        started = time.monotonic_ns()
        response, error = None, None
        graceful = True
        try:
            command = {'id': 'final-shutdown', 'op': 'shutdown'}
            self.process.stdin.write(json.dumps(command).encode() + b'\n')
            self.process.stdin.flush()
            self.process.stdin.close()  # Actual EOF lets the native stdin owner join.
            response = self.read()
            assert response.get('id') == 'final-shutdown', response
            graceful = bool(response.get('ok'))
            self.task_observation = response
            code = self.process.wait(timeout=15)
        except BaseException as failure:
            error, graceful = repr(failure), False
            if self.process.poll() is None:
                self.process.terminate()
            try:
                code = self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                code = self.process.wait(timeout=3)
        self.log.close()
        self.process.stdout.close()
        self.shutdown = {'graceful': graceful, 'exit_code': code,
            'elapsed_ns': time.monotonic_ns() - started, 'error': error,
            'proof_scope': 'actual library shutdown result; timeout/forced process termination is failure, never a native close receipt'}
        for name, value in [('ipc.json', self.frames), ('shutdown.json', self.shutdown),
                            ('task-observation.json', response),
                            ('network-invalidations.json', self.network_invalidations)]:
            (self.output / name).write_text(json.dumps(value, indent=2) + '\n')
        return self.shutdown
