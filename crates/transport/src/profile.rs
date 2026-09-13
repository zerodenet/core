use zero_traits::{
    ClientTlsProfile, GrpcTransportProfile, H2TransportProfile, HttpUpgradeTransportProfile,
    ServerTlsProfile, SplitHttpTransportProfile, WebSocketTransportProfile,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedServerTlsProfile {
    pub options: zero_traits::ServerTlsOptions,
    pub cert_path: String,
    pub key_path: String,
    pub alpn: Vec<String>,
    pub server_fingerprint: Option<String>,
}

impl OwnedServerTlsProfile {
    pub fn from_profile(profile: &(impl ServerTlsProfile + ?Sized)) -> Self {
        Self {
            options: profile.tls_options(),
            cert_path: profile.cert_path().to_owned(),
            key_path: profile.key_path().to_owned(),
            alpn: profile.alpn().to_vec(),
            server_fingerprint: profile.server_fingerprint().map(str::to_owned),
        }
    }
}

impl ServerTlsProfile for OwnedServerTlsProfile {
    fn tls_options(&self) -> zero_traits::ServerTlsOptions {
        self.options.clone()
    }
    fn cert_path(&self) -> &str {
        &self.cert_path
    }

    fn key_path(&self) -> &str {
        &self.key_path
    }

    fn alpn(&self) -> &[String] {
        self.alpn.as_slice()
    }

    fn server_fingerprint(&self) -> Option<&str> {
        self.server_fingerprint.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedClientTlsProfile {
    pub options: zero_traits::ClientTlsOptions,
    pub server_name: Option<String>,
    pub disable_sni: bool,
    pub ca_cert_path: Option<String>,
    pub insecure: bool,
    pub alpn: Vec<String>,
    pub client_fingerprint: Option<String>,
}

impl OwnedClientTlsProfile {
    pub fn from_profile(profile: &(impl ClientTlsProfile + ?Sized)) -> Self {
        Self {
            options: profile.tls_options(),
            server_name: profile.server_name().map(str::to_owned),
            disable_sni: profile.disable_sni(),
            ca_cert_path: profile.ca_cert_path().map(str::to_owned),
            insecure: profile.insecure(),
            alpn: profile.alpn().to_vec(),
            client_fingerprint: profile.client_fingerprint().map(str::to_owned),
        }
    }
}

impl ClientTlsProfile for OwnedClientTlsProfile {
    fn tls_options(&self) -> zero_traits::ClientTlsOptions {
        self.options.clone()
    }
    fn server_name(&self) -> Option<&str> {
        self.server_name.as_deref()
    }

    fn disable_sni(&self) -> bool {
        self.disable_sni
    }

    fn ca_cert_path(&self) -> Option<&str> {
        self.ca_cert_path.as_deref()
    }

    fn insecure(&self) -> bool {
        self.insecure
    }

    fn alpn(&self) -> &[String] {
        self.alpn.as_slice()
    }

    fn client_fingerprint(&self) -> Option<&str> {
        self.client_fingerprint.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedWebSocketProfile {
    pub accept_proxy_protocol: bool,
    pub heartbeat_period_secs: u32,
    pub host: Option<String>,
    pub path: String,
    pub headers: Vec<(String, String)>,
}

impl OwnedWebSocketProfile {
    pub fn from_profile(profile: &(impl WebSocketTransportProfile + ?Sized)) -> Self {
        Self {
            accept_proxy_protocol: profile.accept_proxy_protocol(),
            heartbeat_period_secs: profile.heartbeat_period_secs(),
            host: profile.host().map(str::to_owned),
            path: profile.path().to_owned(),
            headers: profile.header_pairs(),
        }
    }
}

impl WebSocketTransportProfile for OwnedWebSocketProfile {
    fn accept_proxy_protocol(&self) -> bool {
        self.accept_proxy_protocol
    }
    fn heartbeat_period_secs(&self) -> u32 {
        self.heartbeat_period_secs
    }
    fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }
    fn path(&self) -> &str {
        &self.path
    }

    fn header_pairs(&self) -> Vec<(String, String)> {
        self.headers.clone()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OwnedGrpcProfile {
    pub service_names: Vec<String>,
    pub authority: Option<String>,
    pub multi_mode: bool,
    pub idle_timeout_secs: u32,
    pub health_check_timeout_secs: u32,
    pub permit_without_stream: bool,
    pub initial_window_size: u32,
    pub user_agent: Option<String>,
}

impl OwnedGrpcProfile {
    pub fn from_profile(profile: &(impl GrpcTransportProfile + ?Sized)) -> Self {
        Self {
            service_names: profile.service_names().to_vec(),
            authority: profile.authority().map(str::to_owned),
            multi_mode: profile.multi_mode(),
            idle_timeout_secs: profile.idle_timeout_secs(),
            health_check_timeout_secs: profile.health_check_timeout_secs(),
            permit_without_stream: profile.permit_without_stream(),
            initial_window_size: profile.initial_window_size(),
            user_agent: profile.user_agent().map(str::to_owned),
        }
    }
}

impl GrpcTransportProfile for OwnedGrpcProfile {
    fn service_names(&self) -> &[String] {
        self.service_names.as_slice()
    }
    fn authority(&self) -> Option<&str> {
        self.authority.as_deref()
    }
    fn multi_mode(&self) -> bool {
        self.multi_mode
    }
    fn idle_timeout_secs(&self) -> u32 {
        self.idle_timeout_secs
    }
    fn health_check_timeout_secs(&self) -> u32 {
        self.health_check_timeout_secs
    }
    fn permit_without_stream(&self) -> bool {
        self.permit_without_stream
    }
    fn initial_window_size(&self) -> u32 {
        self.initial_window_size
    }
    fn user_agent(&self) -> Option<&str> {
        self.user_agent.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedH2Profile {
    pub host: Option<String>,
    pub path: String,
}

impl OwnedH2Profile {
    pub fn from_profile(profile: &(impl H2TransportProfile + ?Sized)) -> Self {
        Self {
            host: profile.host().map(str::to_owned),
            path: profile.path().to_owned(),
        }
    }
}

impl H2TransportProfile for OwnedH2Profile {
    fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    fn path(&self) -> &str {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedHttpUpgradeProfile {
    pub accept_proxy_protocol: bool,
    pub headers: Vec<(String, String)>,
    pub host: Option<String>,
    pub path: String,
}

impl OwnedHttpUpgradeProfile {
    pub fn from_profile(profile: &(impl HttpUpgradeTransportProfile + ?Sized)) -> Self {
        Self {
            accept_proxy_protocol: profile.accept_proxy_protocol(),
            headers: profile.header_pairs(),
            host: profile.host().map(str::to_owned),
            path: profile.path().to_owned(),
        }
    }
}

impl HttpUpgradeTransportProfile for OwnedHttpUpgradeProfile {
    fn accept_proxy_protocol(&self) -> bool {
        self.accept_proxy_protocol
    }
    fn header_pairs(&self) -> Vec<(String, String)> {
        self.headers.clone()
    }
    fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    fn path(&self) -> &str {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedSplitHttpProfile {
    pub options: zero_traits::SplitHttpOptions,
    pub host: Option<String>,
    pub path: String,
    pub mode: String,
}

impl OwnedSplitHttpProfile {
    pub fn from_profile(profile: &(impl SplitHttpTransportProfile + ?Sized)) -> Self {
        Self {
            options: profile.options(),
            host: profile.host().map(str::to_owned),
            path: profile.path().to_owned(),
            mode: profile.mode().to_owned(),
        }
    }
}

impl SplitHttpTransportProfile for OwnedSplitHttpProfile {
    fn options(&self) -> zero_traits::SplitHttpOptions {
        self.options.clone()
    }
    fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn mode(&self) -> &str {
        &self.mode
    }
}
