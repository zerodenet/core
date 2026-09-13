# Versioned ClientHello profiles

The custom TLS 1.3 engine and REALITY client use uTLS
`aa6edf4b11af82e110eea845bb2983d30138d651`, the dependency selected by
Xray-core v26.3.27 (`d2758a023cd7f4174a5a5fa4ff66e487d4342ba0`). This does
not advance the protocol's upstream baseline.

| Family | Explicit versions | Short alias |
| --- | --- | --- |
| Chrome | 70, 72, 83, 87, 96, 100, 102, 106, 120, 131, 133 | `chrome` -> `chrome-133` |
| Firefox | 63, 65, 99, 102, 105, 120, 148 | `firefox` -> `firefox-148` |
| Safari | 16.0, 26.3 | `safari` -> `safari-26.3` |
| iOS | 13, 14 | `ios` -> `ios-14` |
| Edge | 85, 106 | `edge` -> `edge-85` |
| QQ | 11.1 | `qq` -> `qq-11.1` |
| 360 | 11.0 | use `360-11.0` |

Names use `family-version`, for example `firefox-120`. The corresponding
`HelloFirefox_120` spelling is also accepted. The historical Zero `edge-120`
name remains a compatibility alias for Chrome 120; upstream never defined an
Edge 120 preset. New configurations should use an actual upstream version.
The old four family aliases now select the pinned official defaults above;
select an explicit version to retain the old browser version.

```json
{"reality":{"client_fingerprint":"ios-14"}}
```

The complete preset cipher vector and extension bodies are retained, including
legacy cipher advertisements, signature algorithms, supported versions/groups,
renegotiation, status/SCT, certificate compression, delegated credentials, ALPS,
record-size limit, GREASE and ECH where the selected version includes them.
Chrome's shuffled extension order and BoringSSL's conditional padding rules
are materialized at connection construction. Every ClientHello receives fresh
GREASE, ECH config ID/encapsulation/payload and real ephemeral keys. Firefox 148
reuses the hybrid X25519 component; Chrome and Safari retain independent keys.
The first handshake record uses legacy record version 0x0301.

`random` selects one of the fixed reference's 19 `ModernFingerprints` once per process.
`randomized` and `randomizednoalpn` construct separate process-stable randomized
shapes: TLS 1.3 cipher preference/subsets, signature lists, groups, optional
extensions and extension order vary. Keys and other handshake entropy remain
fresh per connection. The randomized shapes follow the pinned uTLS weights
with Xray's TLS 1.3 restriction, use Rust randomness, and do not reproduce Go's
seed-to-byte sequence. A hybrid share is always included in supported groups.
The no-ALPN variant emits neither ALPN nor ALPS. Adding explicit historical
profiles does not expand the `random` candidate set.

Captures and metadata live in `crates/ztls/src/fingerprint/presets/`. Regenerate
with `scripts/prepare-clienthello-vectors.go` from the pinned Xray module,
then run `scripts/compile-clienthello-presets.py` and `cargo fmt --all`.
The Go tool rejects a different uTLS module version. Generated captures have
random bytes; regression comparisons normalize only ephemeral fields and
permitted Chrome ordering, while checking ECH envelope lengths separately.
The upstream BSD license is preserved beside the catalog.

This is a TLS 1.3 ClientHello implementation, not a TLS 1.2 engine. Legacy
cipher/version advertisements remain in the source templates; the generic TLS
1.3 client filters them to its supported version. TLS 1.2 negotiation,
historical TLS-1.2-only presets (including Android/360 7.5),
Dedicated PSK-named presets and old Kyber draft presets are outside this table.
Ordinary TLS session resumption with these browser presentations is implemented
through the shared rustls state machine, as described below. TLS 1.3
X25519, secondary P-256 and X25519MLKEM768 shares retain live private state;
zlib, Brotli and Zstd certificate decoding is bounded. Explicit custom cipher
or ALPN settings and disabling hybrid KEX intentionally modify the preset.
Ordinary TCP TLS now installs the presentation on rustls for default TLS 1.2/1.3
negotiation, opt-in session resumption and ECH. QUIC and explicit OpenSSL paths
retain their established backends.
The generic TLS 1.3 client now implements HelloRetryRequest; REALITY retains its
protocol-specific handshake. New paths have not been tested.

Validation commands:

```sh
cargo test -p ztls --all-features
XRAY_BIN=/path/to/pinned/xray RUST_MIN_STACK=16777216 cargo test -p zero-proxy --all-features --test vless_reality_extensions
RUST_MIN_STACK=16777216 cargo test --workspace --all-features
```

The ztls tests enumerate the current 26 profiles against a certificate-verified Rustls
server and exchange application records in both directions. Separate cases force
P-256 and ML-KEM selection. The proxy test attempts each profile through native
REALITY, then through the pinned official server when `XRAY_BIN` is provided.
A run without that variable is not evidence of official interoperability.

Local verification on 2026-09-13: ztls has 14 passing tests (plus two ignored
pre-existing doc examples) and clean focused Clippy; native configuration has
four passing tests; the REALITY library has 13 passing filtered tests, including
all 22 versioned profiles authenticating and exchanging application records.
Whole-workspace compilation was interrupted by concurrent changes to unrelated
MUX and carrier interfaces. This is not a whole-workspace or release acceptance
statement.

The final pinned-Xray proxy interop attempt did not reach execution: concurrent
inbound MUX/service changes failed Rust's `Send` lifetime checks in the proxy
runtime. No official-Xray interop pass is claimed for this change. Earlier
whole-workspace attempts also stopped during compilation. Rerun the commands
above after the shared workspace is stable.

## Implementation follow-up (2026-09-13)

The later stopped workspace batch did execute `vless_reality_extensions` with
`XRAY_BIN` set: all three tests completed, including the 22-profile native and
pinned-official-server matrix. That evidence supersedes the earlier compile-blocked
attempt above only for the logged paths and preceding code snapshot.

New untested work adds HelloRetryRequest to the generic TLS 1.3 handshake,
authenticated ALPN checking, record-boundary directional bypass and ordinary
TLS fingerprint integration when the configured minimum is TLS 1.3. ECH,
resumption and legacy negotiation continue to use the existing backend.
Disabled SNI and explicit curves now stay on the custom path: certificate
verification keeps its configured name, and real shares use the transport
group provider, including across HRR. ServerHello/HRR may span multiple records,
and ALPN validation uses the actual wire offer. These changes remain untested. This does not add TLS-1.2-only, PSK or old Kyber presets,
or full QUIC browser fingerprint emulation. Tests are deferred by user request.

Four additional explicit profiles (Chrome 70/72, Firefox 63/65) were generated
from the same pinned uTLS source. Existing capture files were retained. Their
handshakes have not been executed in this implementation-only follow-up.


## Ordinary TLS integration follow-up

The shared TCP TLS connectors (direct and relay) now use the same mature rustls
state machine for the default TLS 1.2/1.3 range, TLS 1.3 PSK resumption and real
ECH with the selected browser presentation. There is no TLS-1.3-only selection
gate or silent provider-only fallback for those three combinations.

A small, licensed patch to rustls 0.23.40 applies connection-local presentation
before PSK binder construction and ECH sealing. The same layout and GREASE values
survive HRR; fresh connections receive fresh presentation data. Live keys,
tickets, binders, SNI, ALPN and ECH ciphertext remain state-machine-owned. The
existing complete-policy cache is retained for resumption and is still opt-in.
The existing CA, pin, alternative-name and disabled-SNI policies remain in use.
Certificate compression reuses bounded zlib/Brotli/Zstd implementations.

This is complete ClientHello presentation plumbing, not a claim of identical
browser negotiation for unsupported algorithms: rustls-unavailable CBC and RSA
key-exchange cipher suites are omitted. Static SCT/ALPS offers are inherited
from the catalog; HTTP/2 ALPS application-settings consumption is not added by
this patch. Legacy TLS-1.2-only presets, old Kyber, explicit OpenSSL presentation
and QUIC browser emulation remain separate gaps.

New regression sources cover TLS 1.2/1.3 negotiation, session resumption,
configuration policies and combined ECH+PSK. Tests were not executed at the
user's request. Earlier interoperability evidence does not validate this patch.
See `vendor/rustls/ZERO-PATCH.md` for the exact source baseline and patch surface.
