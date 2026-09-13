# Zero TLS compatibility extensions

Base: rustls 0.23.40, upstream commit `b44c09fbca5172b3f5e5ed6ba2ffe6fcd934e07a`
(package path `rustls`), copied from the published crate. Original licenses and
source remain included. The workspace pins this exact package version.

## ClientHello presentation

`client::hello_profile::ClientHelloProfileProvider` supplies fresh per-connection
presentation before PSK binders and ECH sealing. HRR reuses the presentation.
Live keys, tickets, binders, SNI, ALPN, cookies and ECH remain state-machine owned.
The presentation survives ECH inner compression and outer AAD construction.
No browser catalog or VLESS-specific behavior is embedded in this crate.

## TLS 1.2 client compatibility

The opt-in `legacy-client` feature adds 19 TLS 1.2 suites covering the CBC, static
RSA, DHE and 3DES IDs in the pinned presentation catalog. They are exported
separately and are not added to the default provider. Zero installs them only
for browser profiles or explicit client suite configuration.

The record key block includes separate MAC secrets before encryption keys.
`rustls-legacy-crypto` wraps AWS-LC's TLS CBC AEADs; AES-256-CBC/SHA256 uses its
bounded constant-work fallback described in that crate's README. Explicit IVs
are unpredictable and every MAC/padding failure maps to the same record error.
The 3DES confidentiality limit is 512 maximum-size records per key (8 MiB).
CBC key extraction for kTLS is rejected, since rustls exposes no CBC secret type.

Static RSA validates the server certificate using the configured verifier before
PKCS#1 encryption of the fresh premaster secret. It handles the absent
ServerKeyExchange, optional OCSP/client authentication, EMS, Finished and session
resumption. RSA key exchange is not labeled FIPS approved. DHE uses AWS-LC's
built-in RFC 7919 groups and validates the peer public key. Inbound legacy suite
configuration continues to select Zero's existing OpenSSL server.

## ALPS

Both browser codepoints (17513 and 17613) are negotiated for configured ALPN
protocols, only with TLS 1.3. Server settings require a matching offered protocol;
dual codepoints and unsolicited settings are rejected. The client sends its
EncryptedExtensions before client authentication/Finished, in the authenticated
transcript. The public settings getter exposes data only after handshake.

Tickets retain both peers' settings. Normal PSK resumption renegotiates ALPS;
accepted early data requires unchanged local settings/protocol/offer and restores
cached peer settings without another ALPS exchange. These extensions also run
inside the existing ECH handshake. Zero exposes authenticated settings on the
carrier; patched h2 consumes them before streams open, and hyper forwards them
for XHTTP. ALPS settings never generate an HTTP/2 SETTINGS ACK.

## Validation boundary

Regression sources cover suite preservation, verified TLS 1.2 full/resumed
handshakes, record rejection, ALPS transcript/state rules and HTTP/2 settings/ACK
handling. Tests were not executed at the user's request; compilation is not
interoperability or production evidence. Original upstream rustls unit tests
also require the upstream `test-ca` fixtures, absent from the published crate.
