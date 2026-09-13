# REALITY development status

Baseline: Xray-core v26.3.27 (`d2758a023cd7f4174a5a5fa4ff66e487d4342ba0`),
XTLS/REALITY `9234c772ba8f`, uTLS `aa6edf4b11af`.

Native and pinned official bidirectional TCP/UDP tests pass for target mirroring,
ML-DSA-65 certificate proof, hybrid ML-KEM/X25519 exchange, authenticated version,
SNI and clock policy, and unauthenticated forwarding with PROXY metadata. Fragmented
encrypted handshake messages resume at their incomplete header/body; both Finished
proofs are validated. The complete Certificate vector and CertificateVerify lengths
are checked rather than accepting a valid first certificate with a malformed tail.

The client distinguishes REALITY proof from ordinary PKI authentication. A real
target certificate must pass chain/name/validity and handshake signature checks,
but never yields a usable VLESS connection. The client instead starts bounded
HTTP/2 navigation over that same TLS connection and returns an authentication error.
Tests verify the HTTP/2 preface and absence of proxy admission, browser navigation
headers, Referer, path discovery, and request counts.

The native outbound REALITY field `spider_x` accepts a path, for example
`/entry?p=100-200&c=2&t=3&i=100-200&r=500`. Its control query parameters are
removed before the HTTP request: `p` sets padding bytes, `c` concurrent workers,
`t` requests per worker, `i` request interval milliseconds, and `r` caller return
delay milliseconds. One initial request precedes the workers. Empty configuration
starts at `/`, with zero values for these ranges. Other query parameters remain.

Navigation owns its carrier independently of the caller's return delay. Limits
are explicit: 64 active navigation connections, one-minute lifetime, 64 workers,
4096 requests per connection, 32 active HTTP requests across navigation connections,
4 MiB per response, and 256 discovered paths per
host. The host cache holds at most 1024 entries with a five-minute lifetime.
Invalid or oversized settings fail native configuration validation.

This is not a full parity declaration. The [ClientHello catalog](fingerprints.md)
now contains 22 pinned TLS 1.3 browser profiles, including iOS and QQ, with
random selection and randomized modes. Target post-handshake record/CCS probes
and target-specific post-handshake shaping are implemented under `reality/target`;
they use bounded on-demand caches and runtime-owned cancellation. Tickets are bounded and validated when discarded. KeyUpdate
now rotates read/write secrets and resets record sequence numbers in both roles; an
update request is acknowledged under the old write key before new application records.
Native tests cover all three TLS 1.3 suites, repeated updates and stale-epoch rejection;
a Rustls peer verifies response progress when the REALITY client only reads.
Brotli and Zstd compressed certificates are decoded within a 256 KiB certificate cap,
while CertificateVerify and Finished use the original compressed handshake transcript.
The Rustls real-certificate path and malformed compression/length tests pass.
Unsupported post-handshake messages return errors rather than panicking. The bounded navigation lifecycle also differs
from the reference's indefinitely retained connections and unbounded path cache.
Production qualification is a separate final step, not implementation debt.
