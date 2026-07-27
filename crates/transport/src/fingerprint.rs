//! TLS client fingerprint presets.
//!
//! Each preset defines cipher suites and key exchange groups that
//! match a known browser fingerprint.  Applied via a custom
//! `CryptoProvider` at `ClientConfig` build time.
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
    match name.to_lowercase().as_str() {
        "chrome" => Some(chrome()),
        "firefox" => Some(firefox()),
        "safari" => Some(safari()),
        "ios" => Some(ios()),
        "edge" => Some(chrome()),
        "randomized" => Some(randomized()),
        "none" | "" => None,
        _ => None,
    }
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

fn randomized() -> TlsFingerprint {
    use std::time::{SystemTime, UNIX_EPOCH};
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / 60;
    let s13 = (seed % 3) as usize;
    let s12 = (seed % 4) as usize;

    let mut t13 = vec![tls13_aes128(), tls13_aes256(), tls13_chacha()];
    t13.rotate_left(s13);
    let mut t12 = vec![
        tls12_ecdsa_aes128(),
        tls12_rsa_aes128(),
        tls12_ecdsa_aes256(),
        tls12_rsa_aes256(),
    ];
    t12.rotate_left(s12);

    let mut all = Vec::new();
    all.append(&mut t13);
    all.append(&mut t12);

    TlsFingerprint {
        cipher_suites: all,
        kx_groups: vec![kx_x25519(), kx_p256(), kx_p384()],
        client_hello_profile: ztls::fingerprint::ClientHelloProfile::Chrome120,
    }
}
