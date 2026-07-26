#[derive(Debug, Clone, Copy)]
pub struct VmessInboundUserRef<'a> {
    pub id: &'a str,
    pub cipher: &'a str,
    pub principal_key: Option<&'a str>,
    pub up_bps: Option<u64>,
    pub down_bps: Option<u64>,
    pub device_limit: Option<u32>,
    pub quota_remaining_bytes: Option<u64>,
    pub policy_revision: Option<u64>,
}

pub struct VmessInboundOptionsRef<'a, I, TTls: ?Sized, TWs: ?Sized, TGrpc: ?Sized> {
    pub users: I,
    pub tls: Option<&'a TTls>,
    pub ws: Option<&'a TWs>,
    pub grpc: Option<&'a TGrpc>,
    pub mux_response_backlog_frames: Option<u32>,
    pub mux_response_backlog_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub struct VmessOutboundOptionsRef<'a> {
    pub id: &'a str,
    pub cipher: &'a str,
    pub mux_concurrency: Option<u32>,
    pub mux_idle_timeout_secs: Option<u64>,
    pub mux_response_backlog_frames: Option<u32>,
    pub mux_response_backlog_bytes: Option<u64>,
}

pub struct VmessOutboundBuildOptionsRef<'a, TTls: ?Sized, TWs: ?Sized, TGrpc: ?Sized> {
    pub tag: &'a str,
    pub server: &'a str,
    pub port: u16,
    pub protocol: VmessOutboundOptionsRef<'a>,
    pub tls: Option<&'a TTls>,
    pub ws: Option<&'a TWs>,
    pub grpc: Option<&'a TGrpc>,
}

impl<'a, TTls: ?Sized, TWs: ?Sized, TGrpc: ?Sized> Copy
    for VmessOutboundBuildOptionsRef<'a, TTls, TWs, TGrpc>
{
}

impl<'a, TTls: ?Sized, TWs: ?Sized, TGrpc: ?Sized> Clone
    for VmessOutboundBuildOptionsRef<'a, TTls, TWs, TGrpc>
{
    fn clone(&self) -> Self {
        *self
    }
}
