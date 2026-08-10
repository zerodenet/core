pub use crate::inbound::TrojanInboundUserRef;

pub struct TrojanInboundOptionsRef<I> {
    pub users: I,
    pub mux_response_backlog_frames: Option<u32>,
    pub mux_response_backlog_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub struct TrojanOutboundOptionsRef<'a> {
    pub password: &'a str,
    pub sni: Option<&'a str>,
    pub insecure: bool,
    pub client_fingerprint: Option<&'a str>,
    pub mux_concurrency: Option<u32>,
    pub mux_idle_timeout_secs: Option<u64>,
    pub mux_response_backlog_frames: Option<u32>,
    pub mux_response_backlog_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub struct TrojanOutboundBuildOptionsRef<'a> {
    pub tag: &'a str,
    pub server: &'a str,
    pub port: u16,
    pub protocol: TrojanOutboundOptionsRef<'a>,
}
