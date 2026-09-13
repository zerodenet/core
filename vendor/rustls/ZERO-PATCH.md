# Zero ClientHello presentation hook

Base: rustls 0.23.40, upstream commit `b44c09fbca5172b3f5e5ed6ba2ffe6fcd934e07a`
(package path `rustls`), copied from the published crate. Original licenses and
source remain included. The workspace pins this exact package version.

The optional `client::hello_profile::ClientHelloProfileProvider` creates a fresh
presentation per connection. `client/hs.rs` applies it before ECH sealing and PSK
binder construction; the same presentation is retained for HelloRetryRequest.
No VLESS names, browser catalog or external wire protocol is implemented here.
Zero's transport supplies the pinned uTLS-derived presentation.

The hook orders supported suites, groups, signatures and extensions, adds GREASE
and static presentation extensions, and preserves live SNI/ALPN, key shares,
cookies, tickets and PSK binders. Unsupported negotiable suites are filtered out.
The extension encoder carries presentation through ECH inner compression and
outer AAD construction. PSK remains the final extension. Verification, traffic
keys, TLS 1.2/1.3 negotiation, record processing, ticket storage and ECH state
transitions continue to use upstream rustls code.

The connection template may contain opaque SCT/ALPS offers inherited from the
existing browser catalog; this patch does not add HTTP/2 application-settings
consumption. Likewise CBC/RSA-key-exchange suites absent from rustls are not
advertised, and QUIC does not install this presentation hook. These are explicit
scope limits, not proof of complete browser equivalence.

Regression sources cover default TLS 1.2/1.3 negotiation, resumption, explicit
curves and ECH+PSK. Execution is deferred at the user's request; compilation is
not interoperability evidence.
