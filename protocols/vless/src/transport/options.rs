#[derive(Debug, Clone, Copy)]
pub struct VlessInboundUserRef<'a> {
    pub id: &'a str,
    pub flow: Option<&'a str>,
    pub principal_key: Option<&'a str>,
    pub up_bps: Option<u64>,
    pub down_bps: Option<u64>,
    pub device_limit: Option<u32>,
    pub quota_remaining_bytes: Option<u64>,
    pub policy_revision: Option<u64>,
}

pub struct VlessInboundOptionsRef<
    'a,
    I,
    TTls: ?Sized,
    TWs: ?Sized,
    TGrpc: ?Sized,
    TH2: ?Sized,
    THttp: ?Sized,
    TSplit: ?Sized,
    TFallback: ?Sized,
> {
    pub users: I,
    pub reality: Option<VlessRealityServerOptionsRef<'a>>,
    pub tls: Option<&'a TTls>,
    pub ws: Option<&'a TWs>,
    pub grpc: Option<&'a TGrpc>,
    pub h2: Option<&'a TH2>,
    pub http_upgrade: Option<&'a THttp>,
    pub split_http: Option<&'a TSplit>,
    pub fallback: Option<&'a TFallback>,
    pub mux_response_backlog_frames: Option<u32>,
    pub mux_response_backlog_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub struct VlessOutboundOptionsRef<'a> {
    pub id: &'a str,
    pub flow: Option<&'a str>,
    pub mux_concurrency: Option<u32>,
    pub xudp_concurrency: Option<u32>,
    pub mux_idle_timeout_secs: Option<u64>,
    pub mux_response_backlog_frames: Option<u32>,
    pub mux_response_backlog_bytes: Option<u64>,
    pub reality: Option<VlessRealityClientOptionsRef<'a>>,
    pub quic: Option<VlessQuicClientOptionsRef<'a>>,
}

pub struct VlessOutboundBuildOptionsRef<
    'a,
    TTls: ?Sized,
    TWs: ?Sized,
    TGrpc: ?Sized,
    TH2: ?Sized,
    THttp: ?Sized,
    TSplit: ?Sized,
> {
    pub tag: &'a str,
    pub server: &'a str,
    pub port: u16,
    pub protocol: VlessOutboundOptionsRef<'a>,
    pub tls: Option<&'a TTls>,
    pub ws: Option<&'a TWs>,
    pub grpc: Option<&'a TGrpc>,
    pub h2: Option<&'a TH2>,
    pub http_upgrade: Option<&'a THttp>,
    pub split_http: Option<&'a TSplit>,
}

impl<'a, TTls: ?Sized, TWs: ?Sized, TGrpc: ?Sized, TH2: ?Sized, THttp: ?Sized, TSplit: ?Sized> Copy
    for VlessOutboundBuildOptionsRef<'a, TTls, TWs, TGrpc, TH2, THttp, TSplit>
{
}

impl<'a, TTls: ?Sized, TWs: ?Sized, TGrpc: ?Sized, TH2: ?Sized, THttp: ?Sized, TSplit: ?Sized> Clone
    for VlessOutboundBuildOptionsRef<'a, TTls, TWs, TGrpc, TH2, THttp, TSplit>
{
    fn clone(&self) -> Self {
        *self
    }
}

#[derive(Debug, Clone, Copy)]
pub struct VlessRealityClientOptionsRef<'a> {
    pub public_key: &'a str,
    pub short_id: &'a str,
    pub server_name: Option<&'a str>,
    pub cipher_suites: &'a [String],
    pub client_fingerprint: &'a str,
}

#[derive(Debug, Clone, Copy)]
pub struct VlessRealityServerOptionsRef<'a> {
    pub private_key: &'a str,
    pub short_ids: &'a [String],
    pub server_name: Option<&'a str>,
    pub cipher_suites: &'a [String],
}

#[derive(Debug, Clone, Copy)]
pub struct VlessQuicClientOptionsRef<'a> {
    pub server_name: Option<&'a str>,
    pub insecure: bool,
    pub ca_cert_path: Option<&'a str>,
}

#[derive(Debug, Clone, Copy)]
pub struct VlessQuicBindOptionsRef<'a> {
    pub cert_path: Option<&'a str>,
    pub key_path: Option<&'a str>,
}
