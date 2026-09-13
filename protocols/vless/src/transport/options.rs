#[derive(Debug, Clone, Copy)]
pub struct VlessInboundUserRef<'a> {
    pub reverse_tag: Option<&'a str>,
    pub id: &'a str,
    pub flow: Option<&'a str>,
    pub testseed: &'a [u32],
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
    pub decryption: Option<&'a str>,
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
    pub encryption: Option<&'a str>,
    pub id: &'a str,
    pub flow: Option<&'a str>,
    pub testpre: u32,
    pub testseed: &'a [u32],
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
    pub final_mask: Option<zero_transport::finalmask::Settings>,
    pub mkcp: Option<zero_transport::mkcp::Settings>,
    pub hysteria: Option<zero_transport::hysteria::OptionsRef<'a>>,
    pub download: Option<VlessXhttpDownloadOptionsRef<'a, TTls, TSplit>>,
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

impl<TTls: ?Sized, TWs: ?Sized, TGrpc: ?Sized, TH2: ?Sized, THttp: ?Sized, TSplit: ?Sized> Clone
    for VlessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc, TH2, THttp, TSplit>
{
    fn clone(&self) -> Self {
        Self {
            final_mask: self.final_mask.clone(),
            mkcp: self.mkcp,
            hysteria: self.hysteria,
            download: self.download,
            tag: self.tag,
            server: self.server,
            port: self.port,
            protocol: self.protocol,
            tls: self.tls,
            ws: self.ws,
            grpc: self.grpc,
            h2: self.h2,
            http_upgrade: self.http_upgrade,
            split_http: self.split_http,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct VlessRealityClientOptionsRef<'a> {
    pub spider_x: &'a str,
    pub hybrid_key_exchange: bool,
    pub mldsa65_verify: Option<&'a str>,
    pub public_key: &'a str,
    pub short_id: &'a str,
    pub server_name: Option<&'a str>,
    pub cipher_suites: &'a [String],
    pub client_fingerprint: &'a str,
}

#[derive(Debug, Clone)]
pub struct VlessRealityServerOptionsRef<'a> {
    pub target: Option<crate::reality::target::Profile>,
    pub policy: crate::reality_policy::PolicyRef<'a>,
    pub mldsa65_seed: Option<&'a str>,
    pub private_key: &'a str,
    pub short_ids: &'a [String],
    pub server_name: Option<&'a str>,
    pub cipher_suites: &'a [String],
}

#[derive(Clone, Copy)]
pub struct VlessQuicClientOptionsRef<'a> {
    pub tls: &'a (dyn zero_traits::ClientTlsProfile + Send + Sync),
    pub server_name: Option<&'a str>,
    pub insecure: bool,
    pub ca_cert_path: Option<&'a str>,
}

#[derive(Clone, Copy)]
pub struct VlessQuicBindOptionsRef<'a> {
    pub tls: &'a (dyn zero_traits::ServerTlsProfile + Send + Sync),
    pub cert_path: Option<&'a str>,
    pub key_path: Option<&'a str>,
}

pub struct VlessXhttpDownloadOptionsRef<'a, TTls: ?Sized, TSplit: ?Sized> {
    pub server: &'a str,
    pub port: u16,
    pub tls: Option<&'a TTls>,
    pub reality: Option<VlessRealityClientOptionsRef<'a>>,
    pub quic: Option<VlessQuicClientOptionsRef<'a>>,
    pub split_http: &'a TSplit,
}
impl<TTls: ?Sized, TSplit: ?Sized> Copy for VlessXhttpDownloadOptionsRef<'_, TTls, TSplit> {}
impl<TTls: ?Sized, TSplit: ?Sized> Clone for VlessXhttpDownloadOptionsRef<'_, TTls, TSplit> {
    fn clone(&self) -> Self {
        *self
    }
}

impl std::fmt::Debug for VlessQuicClientOptionsRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VlessQuicClientOptionsRef")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for VlessQuicBindOptionsRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VlessQuicBindOptionsRef")
            .finish_non_exhaustive()
    }
}
