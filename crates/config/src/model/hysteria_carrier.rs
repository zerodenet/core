use serde::{Deserialize, Serialize};
/// Hysteria v2 authenticated byte-stream carrier. TLS identity uses `quic`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HysteriaTransportConfig {
    pub auth: String,
    #[serde(default)]
    pub masquerade: HysteriaCarrierMasqueradeConfig,
    #[serde(default)]
    pub congestion: String,
    #[serde(default)]
    pub uplink_bytes_per_sec: u64,
    #[serde(default)]
    pub downlink_bytes_per_sec: u64,
    #[serde(default)]
    pub quic_parameters: QuicParametersConfig,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QuicParametersConfig {
    pub initial_stream_receive_window: u64,
    pub max_stream_receive_window: u64,
    pub initial_connection_receive_window: u64,
    pub max_connection_receive_window: u64,
    pub max_idle_timeout_secs: u64,
    pub keep_alive_secs: u64,
    pub max_incoming_streams: u32,
    pub disable_path_mtu_discovery: bool,
    pub udp_hop: Option<UdpHopConfig>,
}
impl HysteriaTransportConfig {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.auth.is_empty() || self.auth.bytes().any(|b| b < 32 || b == 127) {
            return Err("invalid Hysteria authentication header");
        }
        if !matches!(
            self.congestion.as_str(),
            "" | "reno" | "bbr" | "brutal" | "force-brutal"
        ) || (self.congestion == "force-brutal" && self.uplink_bytes_per_sec == 0)
        {
            return Err("invalid Hysteria congestion or bandwidth settings");
        }
        let p = &self.quic_parameters;
        if let Some(hop) = &p.udp_hop {
            hop.validate()?;
        }
        if (p.max_idle_timeout_secs != 0 && !(4..=120).contains(&p.max_idle_timeout_secs))
            || (p.keep_alive_secs != 0 && !(2..=60).contains(&p.keep_alive_secs))
            || (p.max_incoming_streams != 0 && p.max_incoming_streams < 8)
        {
            return Err("invalid QUIC idle, keepalive or incoming-stream limits");
        }
        for (initial, max, fallback) in [
            (
                p.initial_stream_receive_window,
                p.max_stream_receive_window,
                8_388_608,
            ),
            (
                p.initial_connection_receive_window,
                p.max_connection_receive_window,
                20_971_520,
            ),
        ] {
            let initial = if initial == 0 { fallback } else { initial };
            let max = if max == 0 { fallback } else { max };
            if initial < 16384 || max < 16384 || initial > max || max > 1 << 60 {
                return Err("invalid QUIC receive-window bounds");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HysteriaCarrierMasqueradeConfig {
    #[default]
    NotFound,
    String {
        content: String,
        #[serde(default)]
        status: u16,
        #[serde(default)]
        headers: std::collections::BTreeMap<String, String>,
    },
    File {
        directory: String,
    },
    Proxy {
        url: String,
        #[serde(default)]
        rewrite_host: bool,
        #[serde(default)]
        insecure: bool,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UdpHopConfig {
    pub ports: Vec<u16>,
    pub interval_min_secs: u64,
    pub interval_max_secs: u64,
}

impl UdpHopConfig {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.ports.is_empty()
            || self.ports.len() > 65535
            || self.ports.contains(&0)
            || [self.interval_min_secs, self.interval_max_secs]
                .iter()
                .any(|value| *value != 0 && *value < 5)
            || (self.interval_min_secs != 0
                && self.interval_max_secs != 0
                && (self.interval_min_secs.min(self.interval_max_secs) < 5
                    || self.interval_min_secs.max(self.interval_max_secs)
                        > i64::MAX as u64 / 1_000_000_000))
        {
            return Err("invalid UDP hopping ports or interval");
        }
        Ok(())
    }
}
