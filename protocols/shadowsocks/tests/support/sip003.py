#!/usr/bin/env python3
"""Transparent test carrier that implements the SIP003 environment contract."""
import asyncio
import json
import os
import sys

async def main():
    role, mode, record = sys.argv[1:]
    assert os.environ['SS_PLUGIN_OPTIONS'] == 'test=options;escaped=value'
    remote = (os.environ['SS_REMOTE_HOST'], int(os.environ['SS_REMOTE_PORT']))
    local = (os.environ['SS_LOCAL_HOST'], int(os.environ['SS_LOCAL_PORT']))
    listen, target = (remote, local) if role == 'server' else (local, remote)
    with open(record, 'w') as output:
        json.dump({'pid': os.getpid(), 'remote': remote, 'local': local}, output)
    async def connected(reader, writer):
        try:
            other_reader, other_writer = await asyncio.open_connection(*target)
            async def copy(source, destination):
                while True:
                    data = await source.read(65535)
                    if not data:
                        if destination.can_write_eof(): destination.write_eof()
                        return
                    destination.write(data)
                    await destination.drain()
            await asyncio.gather(copy(reader, other_writer), copy(other_reader, writer))
            other_writer.close()
            await other_writer.wait_closed()
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()
    loop = asyncio.get_running_loop()
    servers = []
    if mode != 'udp_only':
        servers.append(await asyncio.start_server(connected, *listen))
    if mode != 'tcp_only':
        class Relay(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                self.transport = transport
                self.clients = {}
            def datagram_received(self, data, peer):
                async def forward():
                    if peer not in self.clients:
                        response_transport = self.transport
                        class Response(asyncio.DatagramProtocol):
                            def datagram_received(self, packet, address):
                                response_transport.sendto(packet, peer)
                        transport, _ = await loop.create_datagram_endpoint(Response, remote_addr=target)
                        self.clients[peer] = transport
                    self.clients[peer].sendto(data)
                asyncio.create_task(forward())
        await loop.create_datagram_endpoint(Relay, local_addr=listen)
    await asyncio.Future()

asyncio.run(main())
