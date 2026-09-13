# HTTP carrier Early Data

The fixed interoperability reference is Xray-core v26.3.27.

`ws.path` and `http_upgrade.path` accept the `ed` query option, for example
`/tunnel?ed=2048`. The option is removed from the request path. WebSocket uses
an unpadded base64url `Sec-WebSocket-Protocol` value for a first write no larger
than the configured limit. A larger first write is sent in normal WebSocket frames.
The receiver preserves the early bytes ahead of subsequent frames.

HTTPUpgrade uses any positive `ed` value to enable uploading immediately after the
HTTP request. The first read validates the 101 response and preserves bytes after
its headers. Cancelling a partial read does not discard response parsing state.
Both ordinary and early handshakes require the Upgrade/Connection contract.

`ws.host` selects the HTTP Host value independently of the destination address.
Inbound Host validation compares the hostname case-insensitively and allows an
optional port in the request. `ws.headers` and `http_upgrade.headers` supply ordinary
custom headers; handshake-managed headers remain owned by the transport.

These features belong to shared transports and carry no VLESS parsing or routing.
For implementation and qualification status see [parity](parity.md).

WebSocket `heartbeat_period_secs` sends empty Ping frames on an idle active stream;
zero disables it. The transport owns the persistent deadline and control writes;
read cancellation does not postpone the next heartbeat and dropping the stream
retains no background worker.

The fixed Xray HTTPUpgrade inbound discards the `bufio.Reader` after parsing the
upgrade request (`transport/internet/httpupgrade/hub.go::upgrade`). On a plaintext
connection, pipelined VLESS bytes coalesced with the HTTP header can be lost by that
peer. This was reproduced with v26.3.27/d2758a0: the official log reads only the
16-byte business payload and rejects its request version. Zero preserves this
prefix. The official Early Data matrix separately exercises TLS, whose record
boundary keeps the request header distinct; plaintext Early Data is not claimed
as a passing official interop combination. No payload retry or timing delay masks
this upstream failure.

Inbound `ws.accept_proxy_protocol` and `http_upgrade.accept_proxy_protocol` require
PROXY v1/v2 before TLS or HTTP parsing. The original client address is used for
session observation and device policy. Enable this only on a listener reached by
the intended upstream proxy; with the option disabled, peer-supplied headers do
not change the connection identity. The option has no outbound wire effect.
