//! Runtime-neutral TLS policy passed by prepared transport profiles.
use alloc::{string::String, vec::Vec};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TlsBackend {
    #[default]
    Auto,
    Rustls,
    OpenSsl,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsParameters {
    pub min_version: String,
    pub max_version: String,
    pub cipher_suites: Vec<String>,
    pub curve_preferences: Vec<String>,
    pub enable_session_resumption: bool,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClientTlsOptions {
    pub backend: TlsBackend,
    pub parameters: TlsParameters,
    pub disable_system_roots: bool,
    pub pinned_peer_cert_sha256: Vec<String>,
    pub verify_peer_names: Vec<String>,
    pub ech: EchClientOptions,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EchClientOptions {
    pub config_list: String,
    pub force_query: EchForceQuery,
    /// DNS-backed ECH material resolved by the async carrier preparation path.
    /// Config deserialization never populates this field.
    pub prepared_config_list: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EchForceQuery {
    None,
    Half,
    #[default]
    Full,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EchConfigSource<'a> {
    Static(&'a str),
    Dns {
        query_name: Option<&'a str>,
        server: &'a str,
    },
}

impl EchClientOptions {
    pub fn source(&self) -> Result<Option<EchConfigSource<'_>>, &'static str> {
        let value = self.config_list.trim();
        if value.is_empty() {
            return Ok(None);
        }
        if !value.contains("://") {
            return Ok(Some(EchConfigSource::Static(value)));
        }
        let mut parts = value.split('+');
        let first = parts.next().unwrap_or_default();
        let second = parts.next();
        if parts.next().is_some() {
            return Err("ECH DNS source must contain at most one query-name prefix");
        }
        let (query_name, server) = match second {
            Some(server) => (Some(first), server),
            None => (None, first),
        };
        if query_name.is_some_and(str::is_empty) || server.is_empty() {
            return Err("ECH DNS source has an empty query name or server");
        }
        if !server.starts_with("udp://")
            && !server.starts_with("https://")
            && !server.starts_with("h2c://")
        {
            return Err("ECH DNS source must use udp, https, or h2c");
        }
        Ok(Some(EchConfigSource::Dns { query_name, server }))
    }
}
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ServerTlsOptions {
    pub backend: TlsBackend,
    pub ech_server_keys: String,
    pub one_time_loading: bool,
    pub reload_interval_secs: u64,
    pub ocsp_stapling_secs: u64,
    pub parameters: TlsParameters,
    pub certificates: Vec<TlsCertificateFiles>,
    pub reject_unknown_sni: bool,
}

impl core::fmt::Debug for ServerTlsOptions {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("ServerTlsOptions")
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
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsCertificateFiles {
    pub cert_path: String,
    pub key_path: String,
    pub ocsp_path: Option<String>,
    pub ocsp_stapling_secs: u64,
    pub usage: TlsCertificateUsage,
    pub build_chain: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TlsCertificateUsage {
    #[default]
    Encipherment,
    AuthorityIssue,
}
