"""Loopback-only echo and DNS peers. One native selector thread; no Tokio tasks."""
import ipaddress
import selectors
import socket
import struct
import threading
import time


class Peers:
    def __init__(self):
        self.selector = selectors.DefaultSelector()
        self.echoes, self.packets, self.queries = [], [], []
        self.ipv4 = '127.0.0.1'
        self.stop = threading.Event()
        for host in ('127.0.0.1', '127.0.0.2', '::1'):
            family = socket.AF_INET6 if ':' in host else socket.AF_INET
            stream = socket.socket(family, socket.SOCK_DGRAM)
            if family == socket.AF_INET6:
                stream.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
            stream.bind((host, 0 if not self.echoes else self.port))
            if not self.echoes:
                self.port = stream.getsockname()[1]
            self.echoes.append(stream)
            self.selector.register(stream, selectors.EVENT_READ, 'echo')
        self.dns = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.dns.bind(('127.0.0.1', 0))
        self.selector.register(self.dns, selectors.EVENT_READ, 'dns')
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        while not self.stop.is_set():
            for key, _ in self.selector.select(.05):
                stream = key.fileobj
                data, sender = stream.recvfrom(65535)
                if key.data == 'echo':
                    self.packets.append({'monotonic_ns': time.monotonic_ns(),
                                         'target': stream.getsockname(), 'sender': sender,
                                         'size': len(data), 'payload_hex': data[:32].hex()})
                    stream.sendto(data, sender)
                else:
                    self.answer(data, sender)

    def answer(self, data, sender):
        offset = 12
        labels = []
        while offset < len(data) and data[offset]:
            length = data[offset]
            labels.append(data[offset + 1:offset + 1 + length].decode('ascii'))
            offset += length + 1
        offset += 1
        qtype, qclass = struct.unpack('!HH', data[offset:offset + 4])
        name = '.'.join(labels)
        assert name == 'budget.invalid', name
        assert qclass == 1
        address = self.ipv4 if qtype == 1 else '::1'
        self.queries.append({'monotonic_ns': time.monotonic_ns(), 'name': name,
                             'qtype': qtype, 'answer': address if qtype in (1, 28) else None})
        question = data[12:offset + 4]
        answer = b''
        if qtype in (1, 28):
            packed = ipaddress.ip_address(address).packed
            answer = b'\xc0\x0c' + struct.pack('!HHIH', qtype, 1, 0, len(packed)) + packed
        header = data[:2] + struct.pack('!HHHHH', 0x8180, 1, int(bool(answer)), 0, 0)
        self.dns.sendto(header + question + answer, sender)

    def close(self):
        self.stop.set()
        self.thread.join(timeout=3)
        for stream in [*self.echoes, self.dns]:
            stream.close()
        self.selector.close()


def config(peers, domain):
    return {
        'schema_version': 1,
        'inbounds': [{'tag': 'budget-combined', 'listen': {'address': '127.0.0.1', 'port': 0},
                      'udp': {'enabled': True},
                      'protocol': {'type': 'direct',
                                   'target': 'budget.invalid' if domain else '127.0.0.1',
                                   'port': peers.port}}],
        'outbounds': [{'tag': 'budget-direct', 'protocol': {'type': 'direct'}}],
        'route': {'rules': [], 'final': {'type': 'route', 'outbound': 'budget-direct'}},
        'runtime': {'dns': {'servers': {'loopback': {'type': 'udp', 'host': '127.0.0.1',
                                                   'port': peers.dns.getsockname()[1]}},
                            'default_server': 'loopback', 'cache': None,
                            'policy': {'address_family': 'prefer_ipv4'}}}}
