pub struct QuicConnectionOptions<'a> {
    pub server: &'a str,
    pub port: u16,
    pub alpn: Vec<Vec<u8>>,
    pub quic_profile: Hysteria2QuicProfile,
    pub datagram_receive_buffer_size: Option<usize>,
    pub socket_factory: &'a zero_transport::OutboundDatagramSocketFactory,
}

#[derive(Debug, Clone)]
pub struct Hysteria2ManagedDatagramFlowResume {
    pub(super) protocol: crate::udp::Hysteria2UdpFlowResume,
    pub(super) cache_scope: u64,
    pub(super) lifetime: std::sync::Arc<tokio::sync::watch::Sender<()>>,
    pub(super) pool: super::pool::Hysteria2ConnectionPool,
    pub(super) tag: String,
    pub(super) node: Hysteria2NodeOptions,
}

#[derive(Debug, Clone)]
pub struct Hysteria2AuthenticatedInboundProfile {
    pub(super) masquerade: super::http3::Masquerade,
    pub(super) settings: crate::settings::Settings,
    pub(super) protocol: crate::inbound::Hysteria2InboundProfile,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Hysteria2InboundTcpResponseProtocol {
    pub(super) protocol: crate::inbound::Hysteria2InboundTcpAcceptor,
}

pub struct Hysteria2AuthenticatedQuicConnection {
    pub(super) protocol: crate::inbound::Hysteria2AcceptedQuicConnection,
    pub(super) _http3: Option<super::http3::Hysteria2Http3ServerGuard>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hysteria2ManagedUdpPacketPathCarrierDescriptor {
    pub(super) protocol: crate::udp::Hysteria2UdpPacketPathCarrierDescriptor,
    pub(super) node_identity: String,
}

#[derive(Debug, Clone)]
pub struct Hysteria2ManagedUdpPacketPathCarrierBuild {
    pub(super) protocol: crate::udp::Hysteria2UdpPacketPathCarrierBuild,
    pub(super) pool: super::pool::Hysteria2ConnectionPool,
    pub(super) tag: String,
    pub(super) node: Hysteria2NodeOptions,
}

#[derive(Debug, Clone)]
pub struct Hysteria2ManagedUdpFlowPlan {
    pub(super) tag: String,
    pub(super) server: String,
    pub(super) port: u16,
    pub(super) resume: Hysteria2ManagedDatagramFlowResume,
}

#[derive(Debug, Clone)]
pub struct Hysteria2ManagedUdpPacketPathPlan {
    pub(super) carrier_descriptor: Hysteria2ManagedUdpPacketPathCarrierDescriptor,
    pub(super) carrier_build: Hysteria2ManagedUdpPacketPathCarrierBuild,
}

#[derive(Debug, Clone)]
pub struct Hysteria2ManagedUdpFlowConfig<'a> {
    pub(super) pool: Option<&'a super::pool::Hysteria2ConnectionPool>,
    pub(super) tag: &'a str,
    pub(super) server: &'a str,
    pub(super) port: u16,
    pub(super) password: &'a str,
    pub(super) insecure: bool,
    pub(super) client_fingerprint: Option<&'a str>,
    pub(super) server_name: Option<&'a str>,
    pub(super) settings: crate::settings::Settings,
    pub(super) node: Hysteria2NodeOptions,
}

#[derive(Debug, Clone)]
pub struct Hysteria2TransportLeaf {
    pub(super) pool: super::pool::Hysteria2ConnectionPool,
    pub(super) tag: String,
    pub(super) server: String,
    pub(super) port: u16,
    pub(super) password: String,
    pub(super) insecure: bool,
    pub(super) client_fingerprint: Option<String>,
    pub(super) server_name: Option<String>,
    pub(super) settings: crate::settings::Settings,
    pub(super) node: Hysteria2NodeOptions,
}

#[derive(Debug, Clone)]
pub struct Hysteria2QuicProfile {
    pub(super) insecure: bool,
    pub(super) client_fingerprint: Option<String>,
    pub(super) server_name: Option<String>,
    pub(super) settings: crate::settings::Settings,
    pub(super) node: Hysteria2NodeOptions,
}

#[derive(Debug, Clone, Default)]
pub(super) struct Hysteria2NodeOptions {
    pub(super) ca_cert_path: Option<String>,
    pub(super) source_dir: Option<std::path::PathBuf>,
    pub(super) tls_options: zero_traits::ClientTlsOptions,
    pub(super) salamander_password: Option<String>,
    pub(super) udp_hop: Option<zero_transport::datagram_hop::Profile>,
}

impl Hysteria2NodeOptions {
    pub(super) fn identity(&self) -> String {
        format!("{self:?}")
    }

    pub(super) fn ca_cert_path(&self) -> Option<std::path::PathBuf> {
        self.ca_cert_path.as_ref().map(|path| {
            let path = std::path::PathBuf::from(path);
            if path.is_absolute() {
                path
            } else if let Some(source_dir) = &self.source_dir {
                source_dir.join(path)
            } else {
                path
            }
        })
    }
}
