# TLS policy

The implementation baseline remains Xray-core v26.3.27, commit
`d2758a023cd7f4174a5a5fa4ff66e487d4342ba0`.

VLESS TCP/TLS uses `tls.options`. Raw QUIC, XHTTP/H3 and the Hysteria carrier
use `quic.client_options` and `quic.server_options`. The same settings apply to
XHTTP's independently configured download carrier. These are Zero native
configuration fields, not a second Xray configuration/control interface.

Client example:

```json
{
  "server_name": "front.example",
  "ca_cert_path": "certs/root.pem",
  "options": {
    "disable_system_roots": true,
    "verify_peer_names": ["certificate.example"],
    "parameters": {
      "min_version": "1.3",
      "max_version": "1.3",
      "curve_preferences": ["X25519MLKEM768", "X25519"],
      "enable_session_resumption": true
    }
  }
}
```

`pinned_peer_cert_sha256` is a list of base64-encoded SHA-256 hashes of DER
certificates. A matching leaf pin authenticates that leaf directly. A matching
CA pin replaces the trust roots and still requires chain, validity and name
verification. `verify_peer_names` tries the configured names independently of
SNI. Pins and explicit verification names remain enforced with `insecure`;
skipping PKI checks never skips the TLS handshake signature.

`disable_system_roots` removes the bundled public roots. `ca_cert_path` adds
explicit trust anchors and resolves relative to the configuration directory.
Session resumption is disabled by default. When enabled, bounded client caches
are isolated by the entire TLS policy, carrier kind, and actual CA file contents.

Server example (`tls.options`, or `quic.server_options`):

```json
{
  "reject_unknown_sni": true,
  "certificates": [
    {
      "cert_path": "certs/second.pem",
      "key_path": "certs/second.key",
      "ocsp_path": "certs/second.ocsp"
    }
  ],
  "parameters": {
    "min_version": "1.2",
    "max_version": "1.3",
    "enable_session_resumption": true
  }
}
```

The existing top-level certificate/key is the first certificate. Additional
certificates participate in case-insensitive exact and single-label wildcard
SNI matching. With `reject_unknown_sni` disabled, unmatched names select the
first certificate. Certificate and key matching is checked during preparation.
OCSP response files are passed as TLS staples. The resolver refreshes certificate/key and OCSP files hourly by default.
`reload_interval_secs` changes the interval; `one_time_loading` disables periodic
refresh. A failed refresh keeps the last valid certificate/key pair. Dropping the
resolver cancels its refresh task; applying configuration reload rebuilds it.

`ocsp_stapling_secs` enables automatic OCSP for the primary certificate. Each
additional `certificates` entry can set its own `ocsp_stapling_secs`; zero disables
automatic retrieval for that certificate. A nonzero value also selects that
certificate's refresh interval. The first OCSP attempt runs when the resolver
starts in a Tokio runtime. `one_time_loading` disables all background refresh,
and an explicit `ocsp_path` takes precedence over automatic retrieval.

The shared TLS carrier reads the certificate's first OCSP responder URL, obtains
the issuer from the chain or its first issuer URL, and sends a Go-compatible
SHA-1 CertID request over verified HTTP/HTTPS. Redirects are bounded to ten and
the complete operation to twenty seconds; issuer and OCSP bodies are limited to
1 MiB and 64 KiB respectively. The response is passed as an opaque TLS staple (the OpenSSL backend first
requires a syntactically valid DER OCSP response),
matching the reference's retrieval behavior; fetching does not itself establish
certificate status or validate an OCSP signature. Failed refresh retains a prior
staple only for the same certificate chain. A replacement certificate never
inherits the previous leaf's staple. Resolver disposal cancels pending refresh.
These resource bounds are deliberate operational limits relative to the reference.
The request matches a pinned Go 1.26.1 / x/crypto v0.49.0 vector. Focused tests
pass for missing issuer retrieval, POST-preserving redirects, response limits,
failed refresh, certificate replacement and resolver cancellation. Native
configuration roundtrip and interval overflow rejection also pass.

`parameters.cipher_suites` selects pre-TLS-1.3 suites; TLS 1.3 cipher selection
remains automatic, matching Go's CipherSuites semantics. The OpenSSL backend
adds the reference legacy suites that Rustls cannot negotiate.
Key exchange preferences support X25519, P-256, P-384, P-521, X25519MLKEM768,
SecP256r1MLKEM768 and SecP384r1MLKEM1024, including the pinned upstream
`curvep256`, `curvep384`, `curvep521` names. The hybrid groups require TLS 1.3.
P-521 and both NIST-curve hybrids pass actual shared TLS handshakes and malformed
peer-share rejection tests.

The default `options.backend: "auto"` uses Rustls for ordinary TLS and
client ECH, and OpenSSL 4.0.2 for explicitly enabled legacy TLS/suites or server
ECH keys. TLS 1.0/1.1 require explicit configuration; the ordinary minimum remains
TLS 1.2. QUIC and ECH always negotiate TLS 1.3. An ECH client can retain a legacy
minimum version and CBC suite list because neither constrains the ECH handshake.
Explicit `"open_ssl"` client selection currently rejects ECH; use `"auto"` or
`"rustls"`. Unsupported version, cipher, curve, pin and certificate settings fail
validation/materialization. Native and pinned-official bidirectional TCP/UDP pass TLS 1.0/1.1 and all
nine ECH combinations through the standard TLS path; ECH/H3 also passes.
The current workspace gate remains open, as recorded in [parity.md](parity.md).

Additional certificates may set `usage: "issue"` (also accepted as
`"authority_issue"`) to use their certificate and private key as a signing
authority. `build_chain: true` appends the configured authority chain to the
issued leaf. A configuration containing only an issuing authority does not need
a separate static `cert_path`/`key_path` pair. Issuance uses the requested SNI,
caches the result, and replaces matching static leaves that expire within two
minutes. Issued validity remains inside the authority's validity interval.
The issuance cache is bounded to 1024 entries. Certificate-file refresh retains
the last usable generation on errors. Three actual TLS handshake tests cover
per-SNI issuance/cache/chain behavior, expiring static leaf replacement, and
authority refresh including invalid replacement files.

This surface does not yet close all upstream TLS differences. Legacy TLS
1.0/1.1, remaining cipher suites and actual ECH are implemented in the working
tree. Real handshakes and the pinned-official ECH/legacy matrices pass; these
results cover the named paths rather than every possible TLS configuration.
Client `ech_config_list` accepts a base64 ECHConfigList or a DNS/DoH source;
`ech_force_query` controls required-query failure behavior. Server
`ech_server_keys` accepts the reference serialized private-key/config entries.
ECH configuration resolution is prepared through Zero DNS and runtime services;
TLS carriers consume the material without owning a separate resolver/control plane.
ClientHello preset work is tracked separately in [fingerprints](./fingerprints.md);
ECH-shaped GREASE does not implement ECH encryption. A passing interoperability
case must not be reported as full TLS parity.

The pinned Xray client defaults an empty fingerprint to Chrome/uTLS. That uTLS
version accepts only HKDF-SHA256 for ECH (with AES-128-GCM, AES-256-GCM and
ChaCha20-Poly1305); the pinned Go 1.26.1 standard TLS path supports all nine
KDF/AEAD combinations. Interoperability matrices select these paths explicitly
instead of treating the default-client limitation as a Zero failure.

OpenSSL uses the same bundled Mozilla trust-root set as the existing Rustls
backend; it does not depend on a nonexistent vendored OpenSSL CA directory.
CA pins establish a trust anchor and still require a valid server certificate
chain, time and usage; a pinned leaf follows the reference's direct identity
contract. Explicit verification names still require a valid trust path even
with the generic insecure switch. Actual positive and rejection handshakes pass.
Legacy TLS enables only the protocol/signature exceptions required for TLS 1.0
and 1.1; it does not disable certificate security checks for the whole context.
OpenSSL server context refresh replaces its session cache and ticket keys, so
sessions do not survive a context generation change. This remains a lifecycle
policy difference from the reference's certificate-only refresh.


### Fingerprint, default versions, resumption and ECH

Ordinary TCP TLS (including relay TLS) applies the browser presentation inside
the pinned rustls 0.23.40 ClientHello builder. The default minimum remains TLS
1.2. Opt-in session resumption and ECH no longer bypass the presentation. PSK
binders and ECH AAD/transcripts include the actual encoded extensions; live
credentials and key shares never come from the template. Existing verification
and policy-isolated caching remain authoritative. QUIC and explicitly selected
OpenSSL do not install this hook.

This implementation follow-up has compilation evidence only; the preceding
handshake results belong to the earlier code. Exact browser equivalence is
limited by available cipher suites and application extension handling; see
[fingerprint scope](./fingerprints.md#ordinary-tls-integration-follow-up).
