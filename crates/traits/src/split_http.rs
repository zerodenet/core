//! Runtime-neutral HTTP carrier options. Serialization belongs to zero-config.
use alloc::{string::String, vec, vec::Vec};
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SplitHttpRange {
    pub from: u32,
    pub to: u32,
}
impl SplitHttpRange {
    pub const fn new(from: u32, to: u32) -> Self {
        Self { from, to }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitHttpOptions {
    pub xmux: SplitHttpXmux,
    pub headers: Vec<(String, String)>,
    pub x_padding_bytes: SplitHttpRange,
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
    pub uplink_chunk_size: SplitHttpRange,
    pub sc_max_each_post_bytes: SplitHttpRange,
    pub sc_min_posts_interval_ms: SplitHttpRange,
    pub sc_max_buffered_posts: u32,
    pub sc_stream_up_server_secs: SplitHttpRange,
    pub server_max_header_bytes: u32,
}
impl Default for SplitHttpOptions {
    fn default() -> Self {
        Self {
            xmux: SplitHttpXmux::default(),
            headers: vec![],
            x_padding_bytes: SplitHttpRange::new(100, 1000),
            x_padding_obfs_mode: false,
            x_padding_key: "x_padding".into(),
            x_padding_header: "x-padding".into(),
            x_padding_placement: "queryInHeader".into(),
            x_padding_method: "repeat-x".into(),
            no_grpc_header: false,
            no_sse_header: false,
            session_placement: "path".into(),
            session_key: String::new(),
            seq_placement: "path".into(),
            seq_key: String::new(),
            uplink_http_method: "POST".into(),
            uplink_data_placement: "auto".into(),
            uplink_data_key: String::new(),
            uplink_chunk_size: SplitHttpRange::new(0, 0),
            sc_max_each_post_bytes: SplitHttpRange::new(1000000, 1000000),
            sc_min_posts_interval_ms: SplitHttpRange::new(30, 30),
            sc_max_buffered_posts: 30,
            sc_stream_up_server_secs: SplitHttpRange::new(20, 80),
            server_max_header_bytes: 8192,
        }
    }
}

/// HTTP client-group reuse policy; zero ranges mean unlimited. An all-zero
/// policy selects the pinned reference defaults at the transport boundary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SplitHttpXmux {
    pub max_concurrency: SplitHttpRange,
    pub max_connections: SplitHttpRange,
    pub c_max_reuse_times: SplitHttpRange,
    pub h_max_request_times: SplitHttpRange,
    pub h_max_reusable_secs: SplitHttpRange,
    pub h_keep_alive_period: i64,
}
