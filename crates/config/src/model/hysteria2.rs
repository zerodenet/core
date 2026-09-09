use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Hysteria2CongestionConfig {
    #[serde(rename = "type")]
    pub kind: String,
    pub bbr_initial_window: u64,
    pub bbr_profile: String,
    pub disable_loss_compensation: bool,
}
impl Default for Hysteria2CongestionConfig {
    fn default() -> Self {
        Self {
            kind: "bbr".into(),
            bbr_initial_window: 38_400,
            bbr_profile: "standard".into(),
            disable_loss_compensation: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Hysteria2QuicConfig {
    pub stream_receive_window: u64,
    pub connection_receive_window: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_stream_receive_window: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_connection_receive_window: Option<u64>,
    pub send_window: u64,
    pub max_idle_timeout_secs: u64,
    pub keep_alive_interval_secs: u64,
    pub max_incoming_streams: u32,
    pub disable_path_mtu_discovery: bool,
}
impl Default for Hysteria2QuicConfig {
    fn default() -> Self {
        let q = hysteria2::settings::QuicSettings::default();
        Self {
            stream_receive_window: q.stream_receive_window,
            connection_receive_window: q.connection_receive_window,
            max_stream_receive_window: q.max_stream_receive_window,
            max_connection_receive_window: q.max_connection_receive_window,
            send_window: q.send_window,
            max_idle_timeout_secs: q.max_idle_timeout_secs,
            keep_alive_interval_secs: q.keep_alive_interval_secs,
            max_incoming_streams: q.max_incoming_streams,
            disable_path_mtu_discovery: q.disable_path_mtu_discovery,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Hysteria2TransportConfig {
    pub congestion: Hysteria2CongestionConfig,
    pub quic: Hysteria2QuicConfig,
    pub ignore_client_bandwidth: bool,
}
impl Hysteria2TransportConfig {
    /// Materialize protocol settings from normalized local send/receive rates.
    /// The owning adapter maps Zero upload/download directions at the boundary.
    pub fn validated(
        &self,
        send_bps: Option<u64>,
        receive_bps: Option<u64>,
    ) -> Result<hysteria2::settings::Settings, &'static str> {
        use hysteria2::settings::{Congestion, QuicSettings, Settings};
        let congestion = Congestion::parse(&self.congestion.kind)?;
        let q = &self.quic;
        let settings = Settings {
            upload: send_bps.unwrap_or(0),
            download: receive_bps.unwrap_or(0),
            ignore_client_bandwidth: self.ignore_client_bandwidth,
            disable_loss_compensation: self.congestion.disable_loss_compensation,
            congestion,
            bbr_initial_window: self.congestion.bbr_initial_window,
            bbr_profile: hysteria2::settings::BbrProfile::parse(&self.congestion.bbr_profile)?,
            quic: QuicSettings {
                stream_receive_window: q.stream_receive_window,
                connection_receive_window: q.connection_receive_window,
                max_stream_receive_window: q.max_stream_receive_window,
                max_connection_receive_window: q.max_connection_receive_window,
                send_window: q.send_window,
                max_idle_timeout_secs: q.max_idle_timeout_secs,
                keep_alive_interval_secs: q.keep_alive_interval_secs,
                max_incoming_streams: q.max_incoming_streams,
                disable_path_mtu_discovery: q.disable_path_mtu_discovery,
            },
        };
        settings.validate()?;
        Ok(settings)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Hysteria2MasqueradeResponseConfig {
    NotFound {},
    File {
        dir: String,
    },
    Proxy {
        url: String,
        #[serde(default)]
        rewrite_host: bool,
        #[serde(default)]
        insecure: bool,
        #[serde(default)]
        x_forwarded: bool,
    },
    String {
        content: String,
        #[serde(default = "default_status")]
        status: u16,
        #[serde(default = "default_content_type")]
        content_type: String,
    },
}
impl Default for Hysteria2MasqueradeResponseConfig {
    fn default() -> Self {
        Self::NotFound {}
    }
}
fn default_status() -> u16 {
    200
}
fn default_content_type() -> String {
    "text/html; charset=utf-8".into()
}

impl Hysteria2MasqueradeResponseConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        match self {
            Self::NotFound {} => Ok(()),
            Self::File { dir } => {
                if dir.trim().is_empty() {
                    Err("masquerade file directory is empty")
                } else {
                    Ok(())
                }
            }
            Self::Proxy { url, .. } => hysteria2::settings::validate_proxy_url(url).map(|_| ()),
            Self::String {
                status,
                content_type,
                ..
            } => hysteria2::settings::validate_masquerade_response(*status, content_type),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hysteria2MasqueradeConfig {
    #[serde(flatten)]
    pub response: Hysteria2MasqueradeResponseConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http: Option<super::ListenConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub https: Option<super::ListenConfig>,
    #[serde(default)]
    pub force_https: bool,
}
impl Hysteria2MasqueradeConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.response.validate()?;
        if (self.http.is_some() || self.force_https) && self.https.is_none() {
            return Err("masquerade HTTP and force_https require an HTTPS listener");
        }
        for listen in self.http.iter().chain(self.https.iter()) {
            if listen.address.trim().is_empty() || listen.port == 0 {
                return Err("masquerade listener requires an address and nonzero port");
            }
        }
        Ok(())
    }
}
