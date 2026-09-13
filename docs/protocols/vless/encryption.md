# VLESS Encryption

Reference: Xray-core v26.3.27 (`d2758a023cd7f4174a5a5fa4ff66e487d4342ba0`).
Implementation and verification status is tracked in [parity](parity.md).

The outbound protocol accepts `encryption`; the inbound protocol accepts
`decryption`. Omission retains the existing `none` behavior. Parsing and key
materialization belong to VLESS. Encryption wraps each established logical carrier
before the VLESS request, for TCP, stream UDP, MUX/XUDP and relay final hops.
It does not introduce a management endpoint.

Client form:
`mlkem768x25519plus.<native|xorpub|random>.<1rtt|0rtt>.[padding.]<public-key>[.<public-key>...]`.
Server form:
`mlkem768x25519plus.<native|xorpub|random>.<seconds>s.[padding.]<secret-key>[.<secret-key>...]`.
Server lifetime may also be a range such as `300-600s`. Lifetimes fit the wire's
unsigned 16-bit seconds. Public keys decode to 32-byte X25519 keys or 1184-byte
ML-KEM-768 keys; server keys decode to 32-byte X25519 secrets or 64-byte ML-KEM
seeds. Encoding is unpadded base64url. Both ends must use matching key chains and
mask modes. Inbound decryption and fallback cannot be configured together.

Padding consists of period-separated `probability-min-max` triples, alternating
byte lengths and millisecond gaps. The first length has probability at least 100
and both endpoints at least 35 bytes; total maximum padding is at most 65553 bytes.
Omitting padding uses the reference defaults. Authentication and handshake work is
bounded by a 30-second timeout.

`0rtt` reuses a server-issued ticket and performs a fresh non-forward-secret key
exchange for each connection. Ticket state belongs to the configured client profile;
the server retains bounded ticket and replay state and rejects duplicate resumed
handshakes. Exhausted capacity rejects admission without evicting live replay
records. An expired ticket invalidates the client's cached ticket; business payload
is never replayed automatically. Reconfiguration retires encrypted MUX pools.

Vision can use the encryption layer as its switchable stream. Its authenticated
transition preserves the underlying carrier; random masking continues to process
record headers across arbitrary I/O fragments.
