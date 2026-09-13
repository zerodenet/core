use serde::{Deserialize, Serialize};
use zero_traits::{SplitHttpOptions, SplitHttpRange, SplitHttpTransportProfile};
/// Inclusive range, or a fixed value in the serialized configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum SplitHttpRangeConfig {
    Fixed(u32),
    Range { from: u32, to: u32 },
}
impl SplitHttpRangeConfig {
    pub fn range(self) -> SplitHttpRange {
        match self {
            Self::Fixed(n) => SplitHttpRange::new(n, n),
            Self::Range { from, to } => SplitHttpRange::new(from, to),
        }
    }
}
impl From<SplitHttpRange> for SplitHttpRangeConfig {
    fn from(r: SplitHttpRange) -> Self {
        Self::Range {
            from: r.from,
            to: r.to,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SplitHttpConfig {
    pub browser_dialer: Option<super::BrowserDialerConfig>,
    pub download_settings: Option<Box<SplitHttpDownloadConfig>>,
    pub xmux: SplitHttpXmuxConfig,
    pub host: Option<String>,
    pub path: String,
    pub mode: String,
    pub headers: std::collections::HashMap<String, String>,
    pub x_padding_bytes: SplitHttpRangeConfig,
    pub x_padding_obfs_mode: bool,
    pub x_padding_key: String,
    pub x_padding_header: String,
    pub x_padding_placement: String,
    pub x_padding_method: String,
    pub no_grpc_header: bool,
    pub no_sse_header: bool,
    pub session_placement: String,
    pub session_key: String,
    pub seq_placement: String,
    pub seq_key: String,
    pub uplink_http_method: String,
    pub uplink_data_placement: String,
    pub uplink_data_key: String,
    pub uplink_chunk_size: SplitHttpRangeConfig,
    pub sc_max_each_post_bytes: SplitHttpRangeConfig,
    pub sc_min_posts_interval_ms: SplitHttpRangeConfig,
    pub sc_max_buffered_posts: u32,
    pub sc_stream_up_server_secs: SplitHttpRangeConfig,
    pub server_max_header_bytes: u32,
}
impl Default for SplitHttpConfig {
    fn default() -> Self {
        let defaults = SplitHttpOptions::default();
        Self {
            browser_dialer: None,
            download_settings: None,
            xmux: SplitHttpXmuxConfig::default(),
            host: None,
            path: "/".into(),
            mode: "auto".into(),
            headers: defaults.headers.into_iter().collect(),
            x_padding_bytes: defaults.x_padding_bytes.into(),
            x_padding_obfs_mode: defaults.x_padding_obfs_mode,
            x_padding_key: defaults.x_padding_key,
            x_padding_header: defaults.x_padding_header,
            x_padding_placement: defaults.x_padding_placement,
            x_padding_method: defaults.x_padding_method,
            no_grpc_header: defaults.no_grpc_header,
            no_sse_header: defaults.no_sse_header,
            session_placement: defaults.session_placement,
            session_key: defaults.session_key,
            seq_placement: defaults.seq_placement,
            seq_key: defaults.seq_key,
            uplink_http_method: defaults.uplink_http_method,
            uplink_data_placement: defaults.uplink_data_placement,
            uplink_data_key: defaults.uplink_data_key,
            uplink_chunk_size: defaults.uplink_chunk_size.into(),
            sc_max_each_post_bytes: defaults.sc_max_each_post_bytes.into(),
            sc_min_posts_interval_ms: defaults.sc_min_posts_interval_ms.into(),
            sc_max_buffered_posts: defaults.sc_max_buffered_posts,
            sc_stream_up_server_secs: defaults.sc_stream_up_server_secs.into(),
            server_max_header_bytes: defaults.server_max_header_bytes,
        }
    }
}
impl SplitHttpTransportProfile for SplitHttpConfig {
    fn browser_dialer(&self) -> Option<zero_traits::BrowserDialerSettings> {
        self.browser_dialer
            .as_ref()
            .map(super::BrowserDialerConfig::settings)
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
    fn options(&self) -> SplitHttpOptions {
        SplitHttpOptions {
            xmux: self.xmux.options(),
            headers: self
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            x_padding_bytes: self.x_padding_bytes.range(),
            x_padding_obfs_mode: self.x_padding_obfs_mode,
            x_padding_key: self.x_padding_key.clone(),
            x_padding_header: self.x_padding_header.clone(),
            x_padding_placement: self.x_padding_placement.clone(),
            x_padding_method: self.x_padding_method.clone(),
            no_grpc_header: self.no_grpc_header,
            no_sse_header: self.no_sse_header,
            session_placement: self.session_placement.clone(),
            session_key: self.session_key.clone(),
            seq_placement: self.seq_placement.clone(),
            seq_key: self.seq_key.clone(),
            uplink_http_method: self.uplink_http_method.clone(),
            uplink_data_placement: self.uplink_data_placement.clone(),
            uplink_data_key: self.uplink_data_key.clone(),
            uplink_chunk_size: self.uplink_chunk_size.range(),
            sc_max_each_post_bytes: self.sc_max_each_post_bytes.range(),
            sc_min_posts_interval_ms: self.sc_min_posts_interval_ms.range(),
            sc_max_buffered_posts: self.sc_max_buffered_posts,
            sc_stream_up_server_secs: self.sc_stream_up_server_secs.range(),
            server_max_header_bytes: self.server_max_header_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SplitHttpXmuxConfig {
    pub max_concurrency: SplitHttpRangeConfig,
    pub max_connections: SplitHttpRangeConfig,
    pub c_max_reuse_times: SplitHttpRangeConfig,
    pub h_max_request_times: SplitHttpRangeConfig,
    pub h_max_reusable_secs: SplitHttpRangeConfig,
    pub h_keep_alive_period: i64,
}
impl Default for SplitHttpXmuxConfig {
    fn default() -> Self {
        Self {
            max_concurrency: SplitHttpRangeConfig::Fixed(0),
            max_connections: SplitHttpRangeConfig::Fixed(0),
            c_max_reuse_times: SplitHttpRangeConfig::Fixed(0),
            h_max_request_times: SplitHttpRangeConfig::Fixed(0),
            h_max_reusable_secs: SplitHttpRangeConfig::Fixed(0),
            h_keep_alive_period: 0,
        }
    }
}
impl SplitHttpXmuxConfig {
    fn options(&self) -> zero_traits::SplitHttpXmux {
        zero_traits::SplitHttpXmux {
            max_concurrency: self.max_concurrency.range(),
            max_connections: self.max_connections.range(),
            c_max_reuse_times: self.c_max_reuse_times.range(),
            h_max_request_times: self.h_max_request_times.range(),
            h_max_reusable_secs: self.h_max_reusable_secs.range(),
            h_keep_alive_period: self.h_keep_alive_period,
        }
    }
}

/// Independently dialed XHTTP download endpoint. It uses the same native
/// security configuration types as other Zero outbound carriers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitHttpDownloadConfig {
    pub server: String,
    pub port: u16,
    #[serde(default)]
    pub tls: Option<super::ClientTlsConfig>,
    #[serde(default)]
    pub reality: Option<super::RealityConfig>,
    #[serde(default)]
    pub quic: Option<super::QuicConfig>,
    #[serde(default)]
    pub split_http: SplitHttpConfig,
}
