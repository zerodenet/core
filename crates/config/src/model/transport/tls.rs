use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TlsBackendConfig {
    #[default]
    Auto,
    Rustls,
    #[serde(alias = "openssl")]
    OpenSsl,
}
impl TlsBackendConfig {
    fn to_options(self) -> zero_traits::TlsBackend {
        match self {
            Self::Auto => zero_traits::TlsBackend::Auto,
            Self::Rustls => zero_traits::TlsBackend::Rustls,
            Self::OpenSsl => zero_traits::TlsBackend::OpenSsl,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsParametersConfig {
    pub min_version: String,
    pub max_version: String,
    pub cipher_suites: Vec<String>,
    pub curve_preferences: Vec<String>,
    pub enable_session_resumption: bool,
}
impl TlsParametersConfig {
    pub fn to_options(&self) -> zero_traits::TlsParameters {
        zero_traits::TlsParameters {
            min_version: self.min_version.clone(),
            max_version: self.max_version.clone(),
            cipher_suites: self.cipher_suites.clone(),
            curve_preferences: self.curve_preferences.clone(),
            enable_session_resumption: self.enable_session_resumption,
        }
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClientTlsOptionsConfig {
    pub backend: TlsBackendConfig,
    pub parameters: TlsParametersConfig,
    pub disable_system_roots: bool,
    pub pinned_peer_cert_sha256: Vec<String>,
    pub verify_peer_names: Vec<String>,
    pub ech_config_list: String,
    pub ech_force_query: EchForceQueryConfig,
}
impl ClientTlsOptionsConfig {
    pub fn to_options(&self) -> zero_traits::ClientTlsOptions {
        zero_traits::ClientTlsOptions {
            backend: self.backend.to_options(),
            parameters: self.parameters.to_options(),
            disable_system_roots: self.disable_system_roots,
            pinned_peer_cert_sha256: self.pinned_peer_cert_sha256.clone(),
            verify_peer_names: self.verify_peer_names.clone(),
            ech: zero_traits::EchClientOptions {
                config_list: self.ech_config_list.clone(),
                force_query: match self.ech_force_query {
                    EchForceQueryConfig::None => zero_traits::EchForceQuery::None,
                    EchForceQueryConfig::Half => zero_traits::EchForceQuery::Half,
                    EchForceQueryConfig::Full => zero_traits::EchForceQuery::Full,
                },
                prepared_config_list: None,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EchForceQueryConfig {
    None,
    Half,
    #[default]
    Full,
}

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerTlsOptionsConfig {
    pub backend: TlsBackendConfig,
    pub ech_server_keys: String,
    pub one_time_loading: bool,
    pub reload_interval_secs: u64,
    pub ocsp_stapling_secs: u64,
    pub parameters: TlsParametersConfig,
    pub certificates: Vec<TlsCertificateFilesConfig>,
    pub reject_unknown_sni: bool,
}

impl core::fmt::Debug for ServerTlsOptionsConfig {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("ServerTlsOptionsConfig")
            .field("backend", &self.backend)
            .field(
                "ech_server_keys",
                &if self.ech_server_keys.is_empty() {
                    ""
                } else {
                    "[redacted]"
                },
            )
            .field("one_time_loading", &self.one_time_loading)
            .field("reload_interval_secs", &self.reload_interval_secs)
            .field("ocsp_stapling_secs", &self.ocsp_stapling_secs)
            .field("parameters", &self.parameters)
            .field("certificates", &self.certificates)
            .field("reject_unknown_sni", &self.reject_unknown_sni)
            .finish()
    }
}
impl ServerTlsOptionsConfig {
    pub fn to_options(&self) -> zero_traits::ServerTlsOptions {
        zero_traits::ServerTlsOptions {
            backend: self.backend.to_options(),
            ech_server_keys: self.ech_server_keys.clone(),
            one_time_loading: self.one_time_loading,
            reload_interval_secs: self.reload_interval_secs,
            ocsp_stapling_secs: self.ocsp_stapling_secs,
            parameters: self.parameters.to_options(),
            certificates: self
                .certificates
                .iter()
                .map(|cert| zero_traits::TlsCertificateFiles {
                    cert_path: cert.cert_path.clone(),
                    key_path: cert.key_path.clone(),
                    ocsp_path: cert.ocsp_path.clone(),
                    ocsp_stapling_secs: cert.ocsp_stapling_secs,
                    usage: match cert.usage {
                        TlsCertificateUsageConfig::Encipherment => {
                            zero_traits::TlsCertificateUsage::Encipherment
                        }
                        TlsCertificateUsageConfig::Issue => {
                            zero_traits::TlsCertificateUsage::AuthorityIssue
                        }
                    },
                    build_chain: cert.build_chain,
                })
                .collect(),
            reject_unknown_sni: self.reject_unknown_sni,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsCertificateFilesConfig {
    pub cert_path: String,
    pub key_path: String,
    #[serde(default)]
    pub ocsp_path: Option<String>,
    #[serde(default)]
    pub ocsp_stapling_secs: u64,
    #[serde(default)]
    pub usage: TlsCertificateUsageConfig,
    #[serde(default)]
    pub build_chain: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TlsCertificateUsageConfig {
    #[default]
    Encipherment,
    #[serde(alias = "authority_issue")]
    Issue,
}
