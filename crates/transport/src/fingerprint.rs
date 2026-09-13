//! TLS client fingerprint presets.
//!
//! Each preset defines browser-compatible cipher-suite and key-exchange
//! preferences. Client connections use a modern AWS-LC provider shape; these
//! presets are compatibility profiles, not byte-for-byte browser emulation.
//!
//! Presets: `"chrome"`, `"firefox"`, `"safari"`, `"ios"`, `"edge"`,
//! `"randomized"`, `"none"`.

use rustls::crypto::ring as ring_provider;
use rustls::crypto::CryptoProvider;
use rustls::SupportedCipherSuite;

/// Preset configuration for a TLS client fingerprint.
#[derive(Debug, Clone)]
pub struct TlsFingerprint {
    pub cipher_suites: Vec<SupportedCipherSuite>,
    pub kx_groups: Vec<&'static dyn rustls::crypto::SupportedKxGroup>,
    pub client_hello_profile: ztls::fingerprint::ClientHelloProfile,
}

// 鈹€鈹€ Cipher suite aliases 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

fn cs(name: &str) -> SupportedCipherSuite {
    let suites = ring_provider::default_provider().cipher_suites;
    for s in &suites {
        if s.suite().as_str() == Some(name) {
            return *s;
        }
    }
    // Fallback to first available
    suites[0]
}

fn tls13_aes128() -> SupportedCipherSuite {
    cs("TLS13_AES_128_GCM_SHA256")
}
fn tls13_aes256() -> SupportedCipherSuite {
    cs("TLS13_AES_256_GCM_SHA384")
}
fn tls13_chacha() -> SupportedCipherSuite {
    cs("TLS13_CHACHA20_POLY1305_SHA256")
}
fn tls12_ecdsa_aes128() -> SupportedCipherSuite {
    cs("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256")
}
fn tls12_rsa_aes128() -> SupportedCipherSuite {
    cs("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256")
}
fn tls12_ecdsa_aes256() -> SupportedCipherSuite {
    cs("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384")
}
fn tls12_rsa_aes256() -> SupportedCipherSuite {
    cs("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384")
}
fn tls12_ecdsa_chacha() -> SupportedCipherSuite {
    cs("TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256")
}
fn tls12_rsa_chacha() -> SupportedCipherSuite {
    cs("TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256")
}

// 鈹€鈹€ Kx groups 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

fn kx_x25519() -> &'static dyn rustls::crypto::SupportedKxGroup {
    ring_provider::kx_group::X25519
}
fn kx_p256() -> &'static dyn rustls::crypto::SupportedKxGroup {
    ring_provider::kx_group::SECP256R1
}
fn kx_p384() -> &'static dyn rustls::crypto::SupportedKxGroup {
    ring_provider::kx_group::SECP384R1
}

// 鈹€鈹€ Lookup 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

/// Look up a fingerprint preset by name.
pub fn lookup_fingerprint(name: &str) -> Option<TlsFingerprint> {
    use ztls::fingerprint::ClientHelloProfile as P;
    let profile = name.parse::<P>().ok()?;
    let mut fingerprint = match profile {
        P::Firefox63
        | P::Firefox65
        | P::Firefox99
        | P::Firefox102
        | P::Firefox105
        | P::Firefox120
        | P::Firefox148 => firefox(),
        P::Safari160 | P::Safari263 => safari(),
        P::Ios13 | P::Ios14 => ios(),
        _ => chrome(),
    };
    fingerprint.client_hello_profile = profile;
    Some(fingerprint)
}

/// Build a `CryptoProvider` with the fingerprint's cipher suites and
/// kx groups. Falls back to ring defaults for anything not overridden.
pub fn build_provider(fp: &TlsFingerprint) -> CryptoProvider {
    let base = ring_provider::default_provider();
    CryptoProvider {
        cipher_suites: fp.cipher_suites.clone(),
        kx_groups: fp.kx_groups.clone(),
        ..base
    }
}

/// Build the client-side provider for a browser-shaped ClientHello.
///
/// AWS-LC supplies the hybrid X25519/ML-KEM key share used by current browser
/// TLS stacks. Besides post-quantum negotiation, that key share keeps the
/// ClientHello in the modern browser size class expected by some TLS edges.
pub fn build_client_provider(fp: &TlsFingerprint) -> CryptoProvider {
    let mut provider = rustls::crypto::aws_lc_rs::default_provider();
    let preferred = fp
        .cipher_suites
        .iter()
        .filter_map(|suite| {
            provider
                .cipher_suites
                .iter()
                .find(|candidate| candidate.suite() == suite.suite())
                .copied()
        })
        .collect::<Vec<_>>();
    if !preferred.is_empty() {
        provider.cipher_suites = preferred;
    }
    provider
        .kx_groups
        .retain(|group| group.name() != rustls::NamedGroup::X25519MLKEM768);
    provider
        .kx_groups
        .insert(0, rustls::crypto::aws_lc_rs::kx_group::X25519MLKEM768);
    provider
}

// 鈹€鈹€ Presets 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

fn chrome() -> TlsFingerprint {
    TlsFingerprint {
        cipher_suites: vec![
            tls13_aes128(),
            tls13_aes256(),
            tls13_chacha(),
            tls12_ecdsa_aes128(),
            tls12_rsa_aes128(),
            tls12_ecdsa_aes256(),
            tls12_rsa_aes256(),
        ],
        kx_groups: vec![kx_x25519(), kx_p256(), kx_p384()],
        client_hello_profile: ztls::fingerprint::ClientHelloProfile::Chrome120,
    }
}

fn firefox() -> TlsFingerprint {
    TlsFingerprint {
        cipher_suites: vec![
            tls13_aes128(),
            tls13_chacha(),
            tls13_aes256(),
            tls12_ecdsa_aes128(),
            tls12_rsa_aes128(),
            tls12_ecdsa_chacha(),
            tls12_rsa_chacha(),
            tls12_ecdsa_aes256(),
            tls12_rsa_aes256(),
        ],
        kx_groups: vec![kx_x25519(), kx_p256(), kx_p384()],
        client_hello_profile: ztls::fingerprint::ClientHelloProfile::Firefox120,
    }
}

fn safari() -> TlsFingerprint {
    TlsFingerprint {
        cipher_suites: vec![
            tls13_aes128(),
            tls13_aes256(),
            tls13_chacha(),
            tls12_ecdsa_aes128(),
            tls12_rsa_aes128(),
            tls12_ecdsa_aes256(),
            tls12_rsa_aes256(),
        ],
        kx_groups: vec![kx_p256(), kx_x25519(), kx_p384()],
        client_hello_profile: ztls::fingerprint::ClientHelloProfile::Safari160,
    }
}

fn ios() -> TlsFingerprint {
    TlsFingerprint {
        cipher_suites: vec![
            tls13_aes128(),
            tls13_aes256(),
            tls13_chacha(),
            tls12_ecdsa_aes128(),
            tls12_rsa_aes128(),
            tls12_ecdsa_aes256(),
            tls12_rsa_aes256(),
        ],
        kx_groups: vec![kx_p256(), kx_x25519(), kx_p384()],
        client_hello_profile: ztls::fingerprint::ClientHelloProfile::Safari160,
    }
}
