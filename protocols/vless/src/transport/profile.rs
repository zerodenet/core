use super::options::{
    VlessQuicBindOptionsRef, VlessQuicClientOptionsRef, VlessRealityClientOptionsRef,
    VlessRealityServerOptionsRef,
};

const VLESS_QUIC_ALPN: &[u8] = b"h3";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VlessRealityClientProfile {
    pub public_key: String,
    pub short_id: String,
    pub server_name: Option<String>,
    pub cipher_suites: Vec<String>,
    pub client_fingerprint: String,
}

impl VlessRealityClientProfile {
    pub fn new(
        public_key: impl Into<String>,
        short_id: impl Into<String>,
        server_name: Option<String>,
        cipher_suites: Vec<String>,
        client_fingerprint: impl Into<String>,
    ) -> Self {
        Self {
            public_key: public_key.into(),
            short_id: short_id.into(),
            server_name,
            cipher_suites,
            client_fingerprint: client_fingerprint.into(),
        }
    }
}

impl From<VlessRealityClientOptionsRef<'_>> for VlessRealityClientProfile {
    fn from(options: VlessRealityClientOptionsRef<'_>) -> Self {
        Self::new(
            options.public_key,
            options.short_id,
            options.server_name.map(str::to_owned),
            options.cipher_suites.to_vec(),
            options.client_fingerprint,
        )
    }
}

impl From<VlessRealityServerOptionsRef<'_>> for crate::reality::VlessRealityServerProfile {
    fn from(options: VlessRealityServerOptionsRef<'_>) -> Self {
        Self::new(
            options.private_key,
            options.short_ids.to_vec(),
            options.server_name,
            options.cipher_suites.to_vec(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VlessQuicClientProfile {
    pub server_name: Option<String>,
    pub insecure: bool,
    pub ca_cert_path: Option<String>,
}

impl VlessQuicClientProfile {
    pub fn new(server_name: Option<String>, insecure: bool, ca_cert_path: Option<String>) -> Self {
        Self {
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
        Self::new(
            options.server_name.map(str::to_owned),
            options.insecure,
            options.ca_cert_path.map(str::to_owned),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VlessQuicBindProfile {
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

impl VlessQuicBindProfile {
    pub fn new(cert_path: Option<String>, key_path: Option<String>) -> Self {
        Self {
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
        Self::new(
            options.cert_path.map(str::to_owned),
            options.key_path.map(str::to_owned),
        )
    }
}
