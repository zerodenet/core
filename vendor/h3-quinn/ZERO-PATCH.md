# HTTP/3 carrier extension

Upstream h3-quinn 0.0.10, MIT license retained. Adds a neutral externally
classified bidirectional stream source and replay prefix. Ordinary clients
and servers retain upstream behavior. The source must terminate by yielding
a QUIC connection error, like the upstream accept stream. Hysteria2 frame
recognition, authentication and web policy stay in the protocol crate.
