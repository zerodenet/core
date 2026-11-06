import selectors, socket, sys
host, port = sys.argv[1], int(sys.argv[2])
selector = selectors.DefaultSelector()
tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcp.bind((host, port))
tcp.listen(8)
tcp.setblocking(False)
udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udp.bind((host, port))
udp.setblocking(False)
selector.register(tcp, selectors.EVENT_READ)
selector.register(udp, selectors.EVENT_READ)
while True:
    for key, _ in selector.select():
        original = key.fileobj
        if original is tcp:
            child, _ = tcp.accept()
            child.setblocking(False)
            selector.register(child, selectors.EVENT_READ)
        elif original is udp:
            data, peer = udp.recvfrom(65535)
            udp.sendto(data, peer)
        else:
            data = original.recv(65535)
            if data:
                original.sendall(data)
            else:
                selector.unregister(original)
                original.close()
