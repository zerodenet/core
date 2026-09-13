# XHTTP contract

Implementation reference: official Xray-core **v26.3.27**, immutable commit
`d2758a023cd7f4174a5a5fa4ff66e487d4342ba0`. The VLESS package version and lockfile
track this selected reference. VMess is a separate package and is not upgraded
by this change. A matching package number describes the comparison baseline,
not a claim that every Xray feature is implemented.

- `packet-up`: normalized `/path/session/sequence` POST uploads and a GET
  download. POST acknowledgements are checked; application bytes are never
  automatically replayed after an ambiguous failure.
- `stream-up`: one streaming POST `/path/session` plus a GET download.
- `stream-one`: one bidirectional HTTP request without a session suffix.
- Outbound `auto`: packet-up normally; stream-one when REALITY is configured.
  With REALITY and `download_settings`, auto selects stream-up.
- Inbound `auto`: accepts all three modes. HTTP/1.1 and HTTP/2 are served by the
  same request executor; each logical download is independently handed to the
  runtime for VLESS authentication and TCP/UDP/MUX routing.
- Direct outbound carriers use HTTP/1.1 for cleartext, H2 for TLS/REALITY, and H3 when `quic` is combined with `split_http`. HTTP/1 streaming requests close their connection; packet uploads may reuse it after the response completes. Explicit legacy H2C helpers remain available.

This replaces the former `X-Session-Id` private pairing contract. Existing Zero
paired peers using that contract must be upgraded together. `auto` no longer
means stream-one unconditionally: configure `stream-one` explicitly when a
single-stream relay prefix is used. Two-stream UDP relay machinery remains
available for paired modes.

Pending sessions expire after 30 seconds; downloads own active session lifetime.
Upload queues have separate bounded ingress and reassembly buffers, a shared listener byte budget,
and bounded request concurrency. Duplicate or invalid sequence input rejects
that session without terminating unrelated HTTP/2 streams. In-order packet and stream uploads use
backpressure. Connection and logical-stream cancellation release request tasks.

Run the fixed reference tests:

```sh
cargo generate-lockfile # This repository does not track Cargo.lock.
python3 scripts/prepare-xhttp-interop.py --output /tmp/zero-xray
XRAY_BIN=/tmp/zero-xray/xray RUST_MIN_STACK=16777216 cargo test --workspace --all-features --test vless_xhttp_modes --test vless_xhttp_options --test vless_xhttp_h3 -- --ignored --nocapture
```

Transport regression tests cover reordering, duplicate isolation, simultaneous
HTTP/2 downloads and bounded I/O. These are controlled implementation checks;
production verification is a separate final rollout step and is not part of
implementation completeness.

## Extended HTTP options

Config fields use Zero snake_case. `session_placement` and `seq_placement` accept
`path`, `query`, `header`, or `cookie`; the corresponding `*_key` is optional.
`uplink_data_placement` accepts `auto`, `body`, `header`, or `cookie`. Header and
cookie uploads require `packet-up`; data is URL-safe unpadded base64, split into
`<key>-<index>` headers or `<key>_<index>` cookies. `auto` accepts concatenated
header, cookie, then body payloads. `uplink_http_method` controls packet and stream uploads.
`headers` carries application headers; it cannot override Host or HTTP framing.

`x_padding_bytes`, `uplink_chunk_size`, `sc_max_each_post_bytes`,
`sc_min_posts_interval_ms`, and `sc_stream_up_server_secs` accept an integer or
an inclusive `{ "from": n, "to": m }` range. Default padding is 100–1000 bytes;
`x_padding_obfs_mode` enables configurable `x_padding_placement` (`header`,
`cookie`, `query`, `queryInHeader`), `x_padding_key`, `x_padding_header`, and
`x_padding_method` (`repeat-x` or `tokenish`). Tokenish validation measures the
HPACK/QPACK encoded length with the reference's two-byte tolerance. Missing or
out-of-range request padding is rejected before allocating a session.

`no_grpc_header` and `no_sse_header` omit the respective MIME headers.
`sc_max_buffered_posts` bounds the ingress and reassembly queues (default 30),
while the listener byte budget remains bounded. `server_max_header_bytes`
controls HTTP parser limits. Stream-up responses send periodic padding when a
Referer is present. CORS responses reflect Origin and preflight fields, and
allow credentials for cookie placements. HTTP details remain in shared
transport; protocol parsing and route execution are unchanged.

## Client groups and independent downloads

`xmux` accepts inclusive ranges for `max_concurrency`, `max_connections`,
`c_max_reuse_times`, `h_max_request_times`, and `h_max_reusable_secs`, plus signed
`h_keep_alive_period` seconds. Concurrency and connection-count limits are
mutually exclusive. All-zero options select the reference defaults: concurrency
1, request limit 600–900, reusable age 1800–3000 seconds. A client group may own
multiple HTTP/1 connections; H2/H3 streams share a driver. Exhausted groups stop
accepting new use; existing downloads keep their connection. Upload request
rotation keeps its session and sequence without replaying acknowledged bytes.
Configuration reload retires the registered pools.

`download_settings` contains `server`, `port`, one optional `tls`, `reality`, or
`quic` profile, and a `split_http` profile. It has independent Host/path/options
and XMUX policy. Upload and download dial through the same native network
services and share the XHTTP session identifier. It is outbound-only and cannot
be used with stream-one or recursively nested. Direct TCP/UDP wiring is under
verification; relay-chain combinations remain part of the open closure work.

XMUX core regressions and direct independent-download bidirectional TCP/UDP
interop pass against the pinned official binary. The expanded HTTP/1 stress
matrix and relay combinations remain in progress. Browser request headers have
Go-derived golden coverage; TLS fingerprints and overall VLESS
closure are still incomplete.

## Browser request headers

XHTTP uses the pinned reference's fetch defaults. WebSocket and HTTPUpgrade use
its WebSocket defaults. An omitted `User-Agent` selects Chrome; `chrome`, `edge`
and `firefox` select the reference profiles; `golang` selects the Go HTTP client
user agent. An explicitly empty or custom user agent is preserved and disables
browser defaults. Explicit Accept, Cache-Control, Pragma and Priority values are
preserved where the reference preserves them.

Chrome's major version follows the reference's CPU-seeded Go PRNG schedule from
2026-01-13 (version 144); it is stable for the process lifetime. CPU facts come
from the platform layer. No machine identifier is persisted. Browser headers are
separate from TLS ClientHello fingerprint support, which remains on the parity
checklist. Golden vectors are pinned to the reference source, not the host date.

HTTP/1 packet replies are acknowledgements of independently delivered uploads.
When the peer has closed its reply side, the carrier continues consuming complete
packet requests already received on that connection. It retains the same request,
byte, queue and idle limits. Download/stream response failures still cancel their
logical stream. This covers the pinned reference's raw upload pool, whose pooled
connections can be closed by Go GC without waiting for their HTTP replies.

## Packet acknowledgement lifetime

A fully received packet retains its request permit and shared byte budget while
waiting for FIFO admission, even if its HTTP waiter is cancelled. Partial or
malformed requests release their budget. Waiting producers are included in the
64 MiB listener budget; dequeued payload retains its permit until consumed.

The pinned official HTTP/1 client can close a pooled socket without reading packet
acknowledgements. Packet-only acknowledgement write failures therefore preserve
processing of already received requests. Stream and download response failures
still terminate their owning exchange. HTTP/1 packet handlers yield after
admission so buffered requests on one connection do not monopolize execution.
These lifecycle and bounded-admission regressions have focused pass records.
The later implementation promotes fully received missing packets and bounds
reassembly waiting to 30 seconds; the dated parity ledger records the corrected
pinned HTTP/1 stress result. This deliberately differs from immediate overflow
failure and does not establish a complete current workspace gate.

## Stream relay carriers

HTTP/1 and HTTP/2 carrier factories can reopen the prepared relay prefix for
packet uploads, stream uploads, XMUX rotation, and an independent download
endpoint. Intermediate hops use the same mechanism recursively with a strictly
shorter prefix. They do not dial directly around the configured relay chain.

Native TCP/UDP matrices pass for packet-up, stream-up, stream-one and auto over
plain HTTP and TLS, including separated downloads, client rotation and nested
XHTTP intermediate hops. Pool access from prepared leaves is weak; a flow from
an earlier reload generation cannot repopulate the current pool registry.
Official relay interoperability remains under verification. QUIC/H3 over a relay
still needs a datagram carrier and must not silently use a direct UDP socket.
