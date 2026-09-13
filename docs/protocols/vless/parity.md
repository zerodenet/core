# VLESS implementation closure

Reference: Xray-core v26.3.27, commit
`d2758a023cd7f4174a5a5fa4ff66e487d4342ba0`.

Scope confirmed by the user: implement the existing defects and all newly identified
protocol/carrier capabilities in this task. Management remains on Zero's native
control plane. Production qualification is a separate last step and is not counted
as implementation debt. This is a live development checklist, not a completion claim.

| Area | Owning layer | Current work |
| --- | --- | --- |
| VLESS `testpre` | VLESS transport runtime | Shared pre-handshake carrier pools, two-minute expiry, 200 ms replenishment after consumption and owner/reload retirement implemented; seven lifecycle regressions pass. Relay paths do not consume direct preconnections. |
| Vision `testseed` and traffic filter | VLESS | Config/default parameter handling and shared runtime propagation implemented. The expanded 12-test wire regression set passes, including a shared filter window, complete-record transitions, CCM-8 exclusion and short unframed responses. |
| Reverse sniffing | config + neutral sniff/routing runtime | HTTP/TLS/QUIC, destination override/exclusion, metadata-only and route-only policies are wired. Config checks pass; 27 focused sniff/MUX/tuple-flow lifecycle tests pass on the preceding snapshot. Current MUX Send contracts compile; native and pinned-official bidirectional TCP/UDP/reconnect regressions pass (9.74 seconds). |
| Browser Dialer | shared transport + VLESS transport preparation | Native loopback page/data channel, token/origin checks, WS and packet-up routing and runtime retirement are wired. Config checks and the actual VLESS WS control-channel simulation pass. Transport-level WS/error/auth tests (four), XHTTP tests (two), and the shipped-page real-browser WS/Fetch/POST local acceptance all pass. Acknowledgement failures are surfaced without replaying an assigned task. |
| CA dynamic issuance | shared TLS | Per-SNI issuance, bounded certificate cache, issuer validity clipping, chain construction and generation refresh implemented. Three actual TLS issuance/refresh/last-good regressions pass. |
| Actual ECH | config/traits + DNS preparation + shared TLS/QUIC | Static and DNS/DoH client configuration, force/cache policy, nine HPKE combinations, OpenSSL server keys and the QUIC listener adapter are implemented. Native TCP/UDP passes all nine combinations; native and pinned-official bidirectional ECH/H3 TCP/UDP pass. Five runtime force/cache tests and trusted CNAME-chain lookup pass. The standard Go TLS path passes all nine combinations in both official directions; the pinned uTLS path passes its three SHA-256 KDF/AEAD combinations in both directions. Auto selects the ECH-capable Rustls client even when legacy minimum versions or CBC suites are configured; ECH negotiates TLS 1.3. Explicit OpenSsl client selection currently rejects ECH instead of ignoring it. ECH GREASE in fingerprint presets does not implement this capability. |
| Legacy TLS and remaining reference cipher suites | shared TLS | The OpenSSL 4.0.2 backend, reference legacy suite selection, certificate/authority refresh, OCSP, isolated session caches and directional Vision handoff are implemented. Native VLESS TCP/UDP passes TLS 1.0 and 1.1. Actual certificate-pin, trust-root, issuance/refresh, session isolation/resumption, ECH, QUIC key-update and directional Vision tests pass. The corrected OCSP fixture passes exact-staple handshakes in TLS 1.2 and TLS 1.3. The pinned-official raw-TLS matrix also passes TLS 1.0/1.1 in both directions. The workspace final gate is still open. |
| Custom string IDs and equivalent-identity duplicate detection | VLESS + config delegation | Implemented; regression verification in progress |
| Ordinary TLS 1.3 Vision | shared TLS + VLESS | Native and official bidirectional inner TLS TCP/XUDP passed; TLS 1.2 handoff rejected |
| Vision UDP/XUDP and outbound udp443 policy | VLESS + neutral recorded-stream handoff | Implemented; kernel and official verification in progress |
| QUIC custom CA, relative paths, TCP/UDP | protocol profile + shared QUIC transport | Shared trust/name rejection tests passed; native raw QUIC and H3 TCP/UDP pass with CA/cert/key paths relative to loaded config files |
| REALITY short ID length/padding | VLESS validation/runtime | Implemented; verification in progress |
| HTTPUpgrade coalesced header/payload preservation | shared transport | Implemented; verification in progress |
| XHTTP base modes and auto selection | shared transport + VLESS preparation | Existing uncommitted implementation retained |
| VLESS Encryption (1-RTT, 0-RTT, all key/masking/padding modes) | VLESS | Three masks, single ML-KEM and mixed key chains pass native TCP/XUDP and pinned official bidirectional matrices; remaining hardening/combination tests in progress |
| Reverse/Rvs and lifecycle | VLESS + neutral routing/execution | Command-aware admission, portal TCP/UDP, bridge workers, heartbeat/DRAIN and reconnect are wired. Native and pinned official bidirectional TCP/UDP, burst/unsolicited UDP and forced reconnect pass. Source/local propagation, reload/cancellation and final boundary gates remain under verification. |
| XHTTP H3, downloadSettings, XMUX and metadata/padding options | shared transport | H3 all modes and metadata pass pinned official bidirectional TCP/UDP. Metadata/payload placements, padding, pacing and response headers implemented; remaining verification in progress. XMUX core regressions passed; independent download direct TCP/UDP passes pinned official bidirectional tests. Packet cancellation/FIFO/shared-budget regressions pass. HTTP/1 metadata/payload stress now passes the fixed official binary in both directions (four simultaneous 131073-byte TCP flows plus UDP, 4.93 seconds); the matching pinned official-to-official baseline also passes. Cooperative-budget FIFO registration and promotion of fully received missing packets are implemented. Reassembly stops consuming at capacity and waits at most 30 seconds for a missing request; this deliberately differs from the reference's immediate overflow failure. Delayed-arrival/deadline regressions pass, with unchanged reassembly capacity and shared byte reservations. H3 heartbeat now follows the pinned ten-second default and nonzero HTTP-option suppression, with real QUIC timing coverage. Native HTTP/1+HTTP/2 TCP/UDP relay mode/download/XMUX and nested-hop matrices pass; pinned official HTTP/1+HTTP/2 relay TCP/UDP, download and XMUX matrix also passes; SOCKS5-prefix raw QUIC/H3/mKCP/Hysteria native TCP/UDP and pinned official H3/mKCP/Hysteria server matrices pass. Codec-based multi-hop datagram prefixes are now composed by the generic runtime; SS packet-path plans allocate a fresh association for each opened carrier. Independent-association and nested-codec/bounded-buffer regressions pass. Eleven three/four-hop native and pinned-official TCP/UDP combinations pass, including balanced association accounting and reclamation after shutdown (26.33 seconds). VLESS stream/association packet-path prefixes are now wired through strictly shorter prepared relay prefixes; native mKCP and double-VLESS H3 combinations pass. The Mieru domain-versus-resolved-IP response defect is fixed: plain VLESS UDP delegates peer validation to runtime and encodes only length/payload. All three associated-prefix regressions now pass (6.59 seconds), as do four datagram-prefix matrices (63.14 seconds) and the focused domain-response/unknown-peer/scoped-peer tests |
| Browser HTTP request defaults | shared transport + platform CPU facts | Go-derived Chrome/Edge version/header vectors and Firefox/custom-policy regressions passed; separate from TLS fingerprint work |
| REALITY full handshake/target behavior, ML-DSA, hybrid KEX, fingerprints, spider | protocol + TLS transport | ML-DSA-65 key generation matches the pinned official executable; certificate proof, authenticated fragmented ClientHello, target random/record padding and low-order ECDH rejection tests pass. Version/SNI/millisecond-clock policy, target mirroring, runtime-owned forwarding with PROXY metadata, and server-side hybrid KEX are wired. Native and pinned official bidirectional target/ML-DSA TCP/UDP kernel interoperability pass. Client hybrid KEX and fragmented encrypted-flight/Finished validation are wired and pass focused tests. The PKI-verified real-certificate branch now completes TLS without granting proxy admission; bounded HTTP/2 spider navigation on the same carrier and native spider_x parsing pass focused tests. KeyUpdate works in both roles with bounded non-advancing messages and read-only acknowledgement progress; Brotli/Zstd certificate decoding and original-transcript authentication pass focused tests. The custom TLS/REALITY ClientHello catalog now covers 22 pinned versioned presets and random modes; see fingerprints.md for TLS 1.3 and randomized-sequence boundaries. Target post-handshake record shaping and same-connection CCS probing are implemented with runtime-owned bounded caches/cancellation; nine focused probe/handshake regressions pass. On-demand probing and resource bounds remain explicit policies; see reality.md. |
| Fallback rule selection, Unix/PROXY handoff | config + protocol decision + neutral transport/runtime | Native and pinned official SNI/ALPN/path selection, TCP/Unix replay and PROXY v1/v2 wire tests passed |
| WS/HTTPUpgrade Early Data and extended carrier behavior | shared transport | WS Early Data, HTTPUpgrade TLS Early Data and plaintext regular upgrade pass bidirectional official interop; heartbeat passes cancellation/idle tests; required PROXY listener prelude passes shared and kernel tests for WS/HTTPUpgrade before TLS |
| mKCP carrier | shared transport | Native and pinned official bidirectional TCP/UDP pass; loss/reorder/ACK and bounded teardown state tests pass. TLS plus FinalMask passes native and pinned official bidirectional TCP/UDP; final configuration/boundary gates remain open. |
| Hysteria carrier | shared transport | Authentication, 0x401 byte streams, client pooling, inbound H3/stream demultiplexing and negotiated BBR/Brutal/Reno policies are wired. Native and pinned official bidirectional TCP/UDP pass. Auth gating, bandwidth negotiation and string/file masquerade tests pass. Streaming reverse proxy passes a full-duplex 3 MiB upload/response test without buffering the entire request. Conditional file requests, multipart ranges and redirects pass focused tests. Port hopping passes bounded two-socket retention/cancellation tests and native/official kernel interoperability, including port override and socket rotation. Content sniffing now matches 889 Go 1.26.1 vectors and file serving uses its 64 built-in extension types with case-insensitive lookup; all five static/file response regressions pass. Host MIME databases, Go-style URL path escaping and HTTP/2 origin negotiation are implemented. The HTTP/CA focused batch passes 18 tests; the Windows registry MIME branch is not runtime-verified here. Other HTTP appearance semantics and advanced TLS combinations still need their own evidence. |
| FinalMask | shared transport + neutral packet I/O | TCP custom/Fragment/Sudoku and UDP header/crypto/Sudoku/Noise stages are wired. Nine native and pinned official bidirectional TCP/UDP carrier combinations pass. Sudoku samples from pinned Go source decode in entropy/ASCII/custom/rotation modes. Noise order/reset/capacity/cancellation tests pass. XDNS native logical-peer/unsolicited-downlink, pinned Go wire samples and native/official bidirectional mKCP TCP/UDP pass. Sudoku per-byte custom-layout recomputation was removed and shared prepared TCP profiles are reused. XICMP raw I/O, framing, sequence window and polling are implemented; raw-socket execution requires OS permission unavailable in the current unprivileged session. Final configuration/boundary gates remain open. |
| gRPC method, metadata and connection behavior | shared transport + neutral inbound multiplexer | Tun/TunMulti and custom paths, authority, browser/custom User-Agent, keepalive, receive-window settings, bounded flow-controlled writes and connection-level inbound multiplexing implemented. Native and pinned official bidirectional TCP/UDP pass. Direct outbound pooling and retire-with-active-leases unit tests pass; HTTP status, grpc-status, truncation and protobuf overflow errors are surfaced and regression-tested. Pre-payload stale-connection retry and relay-scoped pooling are implemented. Eight simultaneous reconnects reuse one new carrier and all carry data (0.02 seconds). Native and pinned-official SOCKS5-prefix TCP/UDP reuse one gRPC connection (0.85 seconds). |
| Fallback carrier metadata | shared transport + VLESS | Corrected TLS ClientHello ALPN list offset, preserved gRPC SNI, actual REALITY SNI/ALPN and peer addresses, and H3/mKCP TLS metadata. Unit checks pass; raw QUIC local endpoint propagation and acknowledged-FIN regression pass; remaining carrier combination checks are pending. |

## Verification boundary

The capability table above is the primary implementation ledger. Evidence in this
section is tied to the pinned Xray-core v26.3.27 commit
`d2758a023cd7f4174a5a5fa4ff66e487d4342ba0`; focused regressions prove the named
paths only and do not imply complete protocol or production qualification.

`zero_core::InboundRecording` retains ownership of the protocol codec while
handshake recording is removed and pre-route byte counters are handed to the
runtime. Recorded MUX dispatch consumes this neutral contract. Runtime-owned
pools, caches and workers retire on reload or final-owner drop. DNS detour
callbacks retain only weak inventory access, and carrier preparation receives
narrow network services rather than a protocol registry, preventing carrier/DNS
ownership cycles.

Shared TLS policy and its remaining work are tracked in [tls.md](tls.md). The TLS
fingerprint catalog is tracked separately in [fingerprints.md](fingerprints.md)
and is outside this verification ledger.

No commit, push, deployment or production verification is included here. Existing
unrelated VMess work remains outside this VLESS closure.

## Evidence ledger

| Evidence set | Result | What it establishes | Limit |
| --- | --- | --- | --- |
| Historical full workspace batch | 246 test executables completed: 1896 passed, 4 failed across 3 binaries, 126 ignored; workspace doctests passed | A complete result for the earlier snapshot | It is not a clean result for the current tree. Later focused fixes do not retroactively change these four failures. |
| Historical static gates after focused corrections | Workspace Clippy completed without blocking errors; all 26 workspace architecture checks and 188 proxy boundary checks passed; `cargo check --workspace --all-features` passed after the weak-inventory lifecycle fix | Static compilation, lint and dependency-boundary closure at that correction point | No full workspace test rerun followed those corrections. |
| Shared TLS policy (`vless-wave5e-tls-options-run.log`) | 7 passed, 0 failed | Dynamic authority issuance and refresh, last-good retention, pin/name separation, opt-in resumption, TLS version/group policy and SNI certificate selection | Focused shared-TLS coverage only. |
| OpenSSL transport snapshot (`vless-wave5e-tls-openssl-run.log`) | 21 passed, 1 failed | Trust roots, live certificate refresh, TLS 1.0, ECH acceptance, directional handoff, pin validation, sessions and QUIC/key-update paths passed | The one failure was the OCSP fixture with identical issuer and leaf subject names. This snapshot remains failed. |
| Corrected OCSP (`vless-wave5g-ocsp-run.log`) | 1 passed, 0 failed | Exact OCSP staples were observed in actual TLS 1.2 and TLS 1.3 handshakes, and refresh shutdown completed | Supersedes only the wave 5e OCSP fixture failure. |
| ECH runtime and DNS (`vless-wave5f-proxy-ech-run.log`, `vless-wave5f-dns-ech-chain-run.log`) | 5 force/cache tests and 1 trusted CNAME-chain test passed | Query failures surface without plaintext downgrade; successful empty answers follow force policy; trusted CNAME traversal and bounded cache behavior work | These focused tests do not replace transport interoperability or workspace gates. |
| ECH over H3 (`vless-wave5f-ech-h3-run.log`) | 2 passed, 0 failed | Native and pinned-official bidirectional ECH/H3 TCP/UDP paths passed | Covers the H3 matrix represented by those two tests. |
| VLESS TLS profile (`vless-wave5f-vless-tls-profile-run.log`) | 1 passed, 0 failed | VLESS ALPN/default TLS profile projection passed | Configuration/profile scope only. |
| ECH and legacy raw TLS discovery snapshot (`vless-wave5f-ech-legacy-run.log`) | 2 passed, 1 failed | Native VLESS TCP/UDP passed all nine HPKE combinations plus TLS 1.0 and 1.1. The logged pinned-official run passed the three SHA-256 KDF/AEAD cases before its default uTLS client rejected the next KDF. | This remains a failed snapshot that identified a client-selection boundary; its failure is superseded only by the corrected matrix below. |
| Corrected pinned-official ECH and legacy matrix (`vless-final-official-ech-legacy-run.log`) | 2 passed, 0 failed in 14.67 seconds | The standard Go TLS path passed nine ECH combinations plus TLS 1.0 and 1.1 in both client/server directions: 22 cases. The browser-client path passed all three supported AEADs in native and pinned-official client/server directions: 9 cases. | Focused raw-TLS interoperability evidence; it does not replace the final workspace gate. |
| TLS configuration (`vless-wave5f-config-tls-run.log`, `vless-wave5g-config-tls-run.log`) | Wave 5f: 5 passed, 1 failed; wave 5g: 6 passed, 0 failed | The later run verifies authority-only certificates, OCSP limits, policy rejection, ECH/legacy capability selection, round-trip serialization and unsupported backend rejection | The wave 5f canonical `open_ssl` expectation failure remains part of that older snapshot; wave 5g is the focused corrected result. |

Focused post-batch regressions also verify split VLESS UDP send/receive progress,
packet-path association accounting, gRPC reconnect sharing, multihop association
reclamation and weak DNS-inventory shutdown. These results support the corresponding
capability-table rows; they are not a substitute for a current full-suite run.

## Current gate status

The 2026-09-13 workspace build produced 258 test executables. Its execution
was stopped at the user's request: 131 executables exited successfully, two
failed, and four recorded termination. There is no completed whole-workspace
result or completed doctest gate for that batch. The registry failure is an
outdated VLESS packet-path expectation; the SOCKS5-to-Mieru pooled UDP timeout
was independently reproduced before implementation resumed.

Implementation has resumed with tests explicitly deferred. The packet-path
expectation and carrier metadata are updated. TLS 1.3 HelloRetryRequest,
authenticated ALPN validation and bounded asynchronous handshake progress are
implemented. Explicit TLS-1.3-only ordinary TLS fingerprint profiles now use
the full ClientHello engine with the existing trust/pin verifier and directional
Vision handoff. ECH, resumption, custom curve policies and legacy negotiation
retain their established backends; those combinations are not claimed as full
browser wire emulation. Mieru UDP worker shutdown now closes response waiters
and exposes stale connections to the runtime. This does not establish that the
previous first-packet timeout is fixed.

These new changes are untested. The previous static/focused results remain
historical evidence only. See the dated implementation review for the stopped
batch and the implementation follow-up for the current unverified changes.
