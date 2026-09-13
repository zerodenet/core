use super::options::{
    VlessQuicBindOptionsRef, VlessQuicClientOptionsRef, VlessRealityClientOptionsRef,
    VlessRealityServerOptionsRef,
};

const VLESS_QUIC_ALPN: &[u8] = b"h3";

pub(super) fn client_tls(
    profile: &(impl zero_traits::ClientTlsProfile + ?Sized),
) -> zero_transport::profile::OwnedClientTlsProfile {
    let mut profile = zero_transport::profile::OwnedClientTlsProfile::from_profile(profile);
    default_tls_alpn(&mut profile.alpn);
    profile
}

pub(super) fn server_tls(
    profile: &(impl zero_traits::ServerTlsProfile + ?Sized),
) -> zero_transport::profile::OwnedServerTlsProfile {
    let mut profile = zero_transport::profile::OwnedServerTlsProfile::from_profile(profile);
    default_tls_alpn(&mut profile.alpn);
    profile
}

fn default_tls_alpn(alpn: &mut Vec<String>) {
    // The pinned reference supplies these protocols when TLS NextProtos is empty.
    if alpn.is_empty() {
        alpn.extend(["h2".to_owned(), "http/1.1".to_owned()]);
    }
}

#[cfg(test)]
#[path = "../../tests/transport/tls_profile.rs"]
mod tls_tests;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VlessRealityClientProfile {
    pub spider_x: String,
    pub hybrid_key_exchange: bool,
    pub mldsa65_verify: Option<String>,
    pub public_key: String,
    pub short_id: String,
    pub server_name: Option<String>,
    pub cipher_suites: Vec<String>,
    pub client_fingerprint: String,
}

impl VlessRealityClientProfile {
    pub fn with_hybrid_key_exchange(mut self, enabled: bool) -> Self {
        self.hybrid_key_exchange = enabled;
        self
    }

    pub fn with_mldsa65_verify(mut self, key: Option<&str>) -> Self {
        self.mldsa65_verify = key.map(str::to_owned);
        self
    }
    pub fn new(
        public_key: impl Into<String>,
        short_id: impl Into<String>,
        server_name: Option<String>,
        cipher_suites: Vec<String>,
        client_fingerprint: impl Into<String>,
    ) -> Self {
        Self {
            spider_x: String::new(),
            hybrid_key_exchange: true,
            public_key: public_key.into(),
            mldsa65_verify: None,
            short_id: short_id.into(),
            server_name,
            cipher_suites,
            client_fingerprint: client_fingerprint.into(),
        }
    }
}

impl From<VlessRealityClientOptionsRef<'_>> for VlessRealityClientProfile {
    fn from(options: VlessRealityClientOptionsRef<'_>) -> Self {
        let mut profile = Self::new(
            options.public_key,
            options.short_id,
            options.server_name.map(str::to_owned),
            options.cipher_suites.to_vec(),
            options.client_fingerprint,
        )
        .with_mldsa65_verify(options.mldsa65_verify)
        .with_hybrid_key_exchange(options.hybrid_key_exchange);
        profile.spider_x = options.spider_x.to_owned();
        profile
    }
}

impl TryFrom<VlessRealityServerOptionsRef<'_>> for crate::reality::VlessRealityServerProfile {
    type Error = std::io::Error;
    fn try_from(options: VlessRealityServerOptionsRef<'_>) -> Result<Self, Self::Error> {
        Self::new(
            options.private_key,
            options.short_ids.to_vec(),
            options.server_name,
            options.cipher_suites.to_vec(),
        )
        .with_target(options.target)
        .with_mldsa65_seed(options.mldsa65_seed)
        .with_policy(options.policy)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VlessQuicClientProfile {
    pub tls_options: zero_traits::ClientTlsOptions,
    pub server_name: Option<String>,
    pub insecure: bool,
    pub ca_cert_path: Option<String>,
}

impl VlessQuicClientProfile {
    pub fn new(server_name: Option<String>, insecure: bool, ca_cert_path: Option<String>) -> Self {
        Self {
            tls_options: Default::default(),
            server_name,
            insecure,
            ca_cert_path,
        }
    }

    pub fn alpn_protocols(&self) -> Vec<Vec<u8>> {
        vec![VLESS_QUIC_ALPN.to_vec()]
    }
}

impl From<VlessQuicClientOptionsRef<'_>> for VlessQuicClientProfile {
    fn from(options: VlessQuicClientOptionsRef<'_>) -> Self {
        let mut profile = Self::new(
            options.server_name.map(str::to_owned),
            options.insecure,
            options.ca_cert_path.map(str::to_owned),
        );
        profile.tls_options = options.tls.tls_options();
        profile
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VlessQuicBindProfile {
    pub tls_options: zero_traits::ServerTlsOptions,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

impl VlessQuicBindProfile {
    pub fn new(cert_path: Option<String>, key_path: Option<String>) -> Self {
        Self {
            tls_options: Default::default(),
            cert_path,
            key_path,
        }
    }

    pub fn alpn_protocols(&self) -> Vec<Vec<u8>> {
        vec![VLESS_QUIC_ALPN.to_vec()]
    }
}

impl From<VlessQuicBindOptionsRef<'_>> for VlessQuicBindProfile {
    fn from(options: VlessQuicBindOptionsRef<'_>) -> Self {
        let mut profile = Self::new(
            options.cert_path.map(str::to_owned),
            options.key_path.map(str::to_owned),
        );
        profile.tls_options = options.tls.tls_options();
        profile
    }
}
