#!/usr/bin/env python3
"""Exercise the real old C ABI with valid pointers and explicit ownership."""

import argparse
import ctypes
import hashlib
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import threading


def capture(library, output):
    api = ctypes.CDLL(str(library))
    api.zero_start.argtypes = [ctypes.c_char_p]
    api.zero_start.restype = ctypes.c_void_p
    api.zero_shutdown.argtypes = [ctypes.c_void_p]
    api.zero_shutdown.restype = None
    for name in ['zero_query', 'zero_execute']:
        function = getattr(api, name)
        function.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
        function.restype = ctypes.c_void_p
    api.zero_free_string.argtypes = [ctypes.c_void_p]
    api.zero_free_string.restype = None
    api.zero_last_error.argtypes = []
    api.zero_last_error.restype = ctypes.c_void_p

    def consume(pointer):
        if not pointer:
            return None
        try:
            return ctypes.string_at(pointer).decode()
        finally:
            api.zero_free_string(pointer)

    records = {'library_sha256': hashlib.sha256(library.read_bytes()).hexdigest(),
               'source_commit': '2eedc2b8d8e2b0e16dffe32d3f83d2f0f2dbcc8a',
               'pointer_bytes': ctypes.sizeof(ctypes.c_void_p), 'calls': []}
    symbols = subprocess.check_output(['nm', '-D', '--defined-only', str(library)], text=True)
    records['exported_zero_symbols'] = sorted(line.split()[-1] for line in symbols.splitlines()
                                              if line.split()[-1].startswith('zero_'))
    assert records['exported_zero_symbols'] == sorted([
        'zero_start', 'zero_shutdown', 'zero_query', 'zero_execute',
        'zero_free_string', 'zero_last_error'])
    with tempfile.TemporaryDirectory(prefix='zero-ffi-baseline-') as temporary:
        root = Path(temporary)
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        config = {'inbounds': [{'tag': 'ffi-not-started',
                                'listen': {'address': '127.0.0.1', 'port': port},
                                'protocol': {'type': 'socks5'}}],
                  'outbounds': [], 'route': {'rules': [], 'final': {'type': 'direct'}}}
        path = root / 'config.json'
        path.write_text(json.dumps(config))
        handle = api.zero_start(str(path).encode())
        assert handle, consume(api.zero_last_error())
        try:
            with socket.socket() as listener:
                listener.bind(('127.0.0.1', port))
                records['zero_start_does_not_start_proxy_listener'] = True
            for name in ['capabilities', 'health', 'runtime', 'stats']:
                request = {name: {}}
                response = consume(api.zero_query(handle, json.dumps(request).encode()))
                assert response is not None
                records['calls'].append({'query': request, 'response': json.loads(response)})
            command = {'method': 'mode.set', 'params': {'mode': 'direct'}}
            response = consume(api.zero_execute(handle, json.dumps(command).encode()))
            assert response is not None
            records['calls'].append({'command': command, 'response': json.loads(response)})
            invalid = api.zero_query(handle, bytes([255]))
            assert invalid is None
            records['invalid_utf8_error'] = consume(api.zero_last_error())
            assert records['invalid_utf8_error']
            assert consume(api.zero_last_error()) is None
            records['last_error_is_consumed_and_freed'] = True
        finally:
            api.zero_shutdown(handle)
        api.zero_shutdown(None)
        api.zero_free_string(None)
        records['documented_null_shutdown_and_free_are_noops'] = True
        invalid_path = root / 'missing.json'
        result = []
        thread = threading.Thread(target=lambda: result.append(api.zero_start(str(invalid_path).encode())))
        thread.start()
        thread.join(timeout=15)
        assert not thread.is_alive() and result == [None]
        error = consume(api.zero_last_error())
        assert error
        records['error_from_other_thread_visible_on_caller_thread'] = error.replace(str(root), '${TEMP}')
    output.write_text(json.dumps(records, indent=2) + '\n')
    print('Captured six C exports, Engine-only startup, queries/command, release and actual error ownership')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--library', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    capture(args.library.resolve(), args.output.resolve())
