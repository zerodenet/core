use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Hysteria2BandwidthConfig {
    pub up: Option<Hysteria2BandwidthValue>,
    pub down: Option<Hysteria2BandwidthValue>,
    pub disable_loss_compensation: bool,
}

/// String values are bit rates; integers are raw bytes per second.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Hysteria2BandwidthValue {
    Rate(String),
    BytesPerSecond(u64),
}
impl Hysteria2BandwidthValue {
    fn bytes_per_second(&self) -> Result<u64, &'static str> {
        match self {
            Self::Rate(value) => hysteria2::settings::parse_bandwidth(Some(value)),
            Self::BytesPerSecond(value) => Ok(*value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Hysteria2CongestionConfig {
    #[serde(rename = "type")]
    pub kind: String,
    pub bbr_initial_window: u64,
}
impl Default for Hysteria2CongestionConfig {
    fn default() -> Self {
        Self {
            kind: "bbr".into(),
            bbr_initial_window: 38_400,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Hysteria2QuicConfig {
    pub stream_receive_window: u64,
    pub connection_receive_window: u64,
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
    pub bandwidth: Hysteria2BandwidthConfig,
    pub congestion: Hysteria2CongestionConfig,
    pub quic: Hysteria2QuicConfig,
    pub ignore_client_bandwidth: bool,
}
impl Hysteria2TransportConfig {
    pub fn validated(&self) -> Result<hysteria2::settings::Settings, &'static str> {
        use hysteria2::settings::{Congestion, QuicSettings, Settings};
        let congestion = Congestion::parse(&self.congestion.kind)?;
        let q = &self.quic;
        let settings = Settings {
            upload: self
                .bandwidth
                .up
                .as_ref()
                .map(Hysteria2BandwidthValue::bytes_per_second)
                .transpose()?
                .unwrap_or(0),
            download: self
                .bandwidth
                .down
                .as_ref()
                .map(Hysteria2BandwidthValue::bytes_per_second)
                .transpose()?
                .unwrap_or(0),
            ignore_client_bandwidth: self.ignore_client_bandwidth,
            disable_loss_compensation: self.bandwidth.disable_loss_compensation,
            congestion,
            bbr_initial_window: self.congestion.bbr_initial_window,
            quic: QuicSettings {
                stream_receive_window: q.stream_receive_window,
                connection_receive_window: q.connection_receive_window,
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Hysteria2MasqueradeConfig {
    #[default]
    NotFound,
    File {
        dir: String,
    },
    Proxy {
        url: String,
        #[serde(default)]
        rewrite_host: bool,
    },
    String {
        content: String,
        #[serde(default = "default_status")]
        status: u16,
        #[serde(default = "default_content_type")]
        content_type: String,
    },
}
fn default_status() -> u16 {
    200
}
fn default_content_type() -> String {
    "text/html; charset=utf-8".into()
}

impl Hysteria2MasqueradeConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        match self {
            Self::NotFound => Ok(()),
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
