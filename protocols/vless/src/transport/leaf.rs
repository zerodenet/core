mod relay;
mod reverse;
pub use reverse::VlessReverseBridge;
use std::future::Future;
use std::path::Path;

use zero_core::Session;
use zero_platform_tokio::{TcpRelayStream, TokioSocket};
use zero_traits::{
    ClientTlsProfile, GrpcTransportProfile, H2TransportProfile, HttpUpgradeTransportProfile,
    ProtocolOutboundLeaf, ProtocolRelayTwoStreamUdpFlowLeaf, ProtocolUdpFlowLeaf,
    SplitHttpTransportProfile, WebSocketTransportProfile,
};
use zero_transport::RuntimeError;

use super::managed_udp::VlessManagedUdpFlowResume;
use super::outbound::OwnedVlessOutboundTransportPlan;
use super::profile::{VlessQuicClientProfile, VlessRealityClientProfile};
use super::runtime::VlessTransportRuntime;

#[derive(Clone)]
pub struct VlessOutboundLeaf {
    tag: String,
    server: String,
    port: u16,
    transport: OwnedVlessOutboundTransportPlan,
    relay_pools: super::runtime::xhttp::PoolAccess,
    protocol: crate::outbound::PreparedVlessOutboundRequestBundle,
    mux_pool: crate::mux_pool::MuxConnectionPool,
    preconnect: Option<super::runtime::preconnect::Access>,
}

impl VlessOutboundLeaf {
    pub fn from_options_refs<TTls, TWs, TGrpc, TH2, THttp, TSplit>(
        source_dir: Option<&Path>,
        options: super::options::VlessOutboundBuildOptionsRef<
            '_,
            TTls,
            TWs,
            TGrpc,
            TH2,
            THttp,
            TSplit,
        >,
        runtime: &VlessTransportRuntime,
    ) -> Result<Self, zero_core::Error>
    where
        TTls: ClientTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
        TH2: H2TransportProfile + ?Sized,
        THttp: HttpUpgradeTransportProfile + ?Sized,
        TSplit: SplitHttpTransportProfile + ?Sized,
    {
        let super::options::VlessOutboundBuildOptionsRef {
            final_mask,
            mkcp,
            hysteria,
            download,
            tag,
            server,
            port,
            protocol,
            tls,
            ws,
            grpc,
            h2,
            http_upgrade,
            split_http,
        } = options;
        let reality = protocol.reality.map(VlessRealityClientProfile::from);
        let quic = protocol.quic.map(VlessQuicClientProfile::from);
        let mut leaf = Self::from_profile_refs(
            source_dir,
            tag,
            server,
            port,
            protocol.id,
            protocol.flow,
            protocol.testseed,
            protocol.mux_concurrency,
            protocol.xudp_concurrency,
            protocol.mux_idle_timeout_secs,
            protocol.mux_response_backlog_frames,
            protocol.mux_response_backlog_bytes,
            tls,
            reality.as_ref(),
            ws,
            grpc,
            h2,
            http_upgrade,
            split_http,
            quic.as_ref(),
            runtime.mux_pool(),
        )?;
        leaf.transport.final_mask =
            zero_transport::finalmask::Profile::new(final_mask.unwrap_or_default())
                .map_err(|_| zero_core::Error::Config("invalid FinalMask settings"))?;
        leaf.relay_pools = runtime.xhttp_pool_access();
        leaf.transport.share_xhttp_pool(runtime, tag);
        leaf.transport.share_browser_dialer(runtime);
        leaf.transport.set_download(download, runtime, tag);
        leaf.transport.set_encryption(protocol.encryption)?;
        leaf.transport.mkcp = mkcp;
        leaf.transport.hysteria = hysteria
            .map(zero_transport::hysteria::Profile::from_options)
            .transpose()
            .map_err(|_| zero_core::Error::Config("invalid Hysteria carrier authentication"))?;
        leaf.transport.share_hysteria_pool(runtime, tag);
        leaf.transport.validate_browser_dialer()?;
        let preconnect_identity = leaf
            .transport
            .preconnect_identity(protocol.encryption, protocol.testpre);
        leaf.preconnect = runtime.preconnect_pool(tag, preconnect_identity, protocol.testpre);
        if !leaf.transport.final_mask.udp().is_empty()
            || !leaf.transport.final_mask.tcp().is_empty()
            || protocol.encryption.is_some_and(|value| value != "none")
        {
            let identity = format!(
                "{:?}:{:?}",
                protocol.encryption,
                leaf.transport.final_mask.identity()
            );
            leaf.mux_pool = runtime.encryption_mux_pool(tag, &identity);
        }
        Ok(leaf)
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::transport) fn from_profile_refs<TTls, TWs, TGrpc, TH2, THttp, TSplit>(
        source_dir: Option<&Path>,
        tag: &str,
        server: &str,
        port: u16,
        id: &str,
        flow: Option<&str>,
        testseed: &[u32],
        mux_concurrency: Option<u32>,
        xudp_concurrency: Option<u32>,
        mux_idle_timeout_secs: Option<u64>,
        mux_response_backlog_frames: Option<u32>,
        mux_response_backlog_bytes: Option<u64>,
        tls: Option<&TTls>,
        reality: Option<&VlessRealityClientProfile>,
        ws: Option<&TWs>,
        grpc: Option<&TGrpc>,
        h2: Option<&TH2>,
        http_upgrade: Option<&THttp>,
        split_http: Option<&TSplit>,
        quic: Option<&VlessQuicClientProfile>,
        mux_pool: crate::mux_pool::MuxConnectionPool,
    ) -> Result<Self, zero_core::Error>
    where
        TTls: ClientTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
        TH2: H2TransportProfile + ?Sized,
        THttp: HttpUpgradeTransportProfile + ?Sized,
        TSplit: SplitHttpTransportProfile + ?Sized,
    {
        let transport = OwnedVlessOutboundTransportPlan::from_profile_refs(
            source_dir,
            server,
            port,
            tls,
            reality,
            ws,
            grpc,
            h2,
            http_upgrade,
            split_http,
            quic,
        );
        let protocol =
            crate::outbound::PreparedVlessOutboundRequestBundle::from_config_with_transport_hints_mux_policy_and_testseed(
                id,
                flow,
                testseed,
                mux_concurrency,
                xudp_concurrency,
                mux_idle_timeout_secs,
                mux_response_backlog_frames,
                mux_response_backlog_bytes,
                transport.mux_transport_hints(),
            )?;
        Ok(Self::new(tag, server, port, transport, protocol, mux_pool))
    }

    pub(super) fn new(
        tag: &str,
        server: &str,
        port: u16,
        transport: OwnedVlessOutboundTransportPlan,
        protocol: crate::outbound::PreparedVlessOutboundRequestBundle,
        mux_pool: crate::mux_pool::MuxConnectionPool,
    ) -> Self {
        Self {
            tag: tag.to_owned(),
            server: server.to_owned(),
            port,
            protocol,
            transport,
            mux_pool,
            relay_pools: Default::default(),
            preconnect: None,
        }
    }

    pub fn tag(&self) -> &str {
        &self.tag
    }

    pub fn server(&self) -> &str {
        &self.server
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    fn udp_relay_final_hop_error(&self) -> Option<&'static str> {
        None
    }

    pub fn relay_needs_two_streams(&self) -> bool {
        self.transport.relay_needs_two_streams()
    }

    fn uses_deferred_tcp_response(&self) -> bool {
        self.transport.uses_deferred_tcp_response()
    }

    fn owned_transport_plan(&self) -> OwnedVlessOutboundTransportPlan {
        self.transport.clone()
    }

    pub async fn build_relay_two_stream_udp_transport(
        &self,
        post_stream: TcpRelayStream,
        get_stream: TcpRelayStream,
        ech_resolver: std::sync::Arc<dyn zero_transport::tls::ech::EchConfigResolver>,
    ) -> Result<TcpRelayStream, RuntimeError> {
        let mut transport = self.transport.clone();
        transport.prepare_ech(ech_resolver.as_ref()).await?;
        transport
            .build_relay_two_stream_udp_transport(post_stream, get_stream)
            .await
    }

    pub async fn open_tcp_stream<OpenSocket, OpenSocketFut>(
        &self,
        session: &Session,
        open_socket: OpenSocket,
        socket_factory: zero_transport::OutboundDatagramSocketFactory,
        ech_resolver: std::sync::Arc<dyn zero_transport::tls::ech::EchConfigResolver>,
    ) -> Result<crate::outbound::VlessTcpStreamOpen, RuntimeError>
    where
        OpenSocket: Clone + Fn(&str, u16) -> OpenSocketFut + Send + Sync + 'static,
        OpenSocketFut: Future<Output = Result<TokioSocket, RuntimeError>> + Send + 'static,
    {
        let protocol = self.protocol.clone();
        let transport = self.owned_transport_plan();
        let preconnect = self.preconnect.clone();
        let direct_transport = || {
            let finish = transport.clone();
            let open = move || {
                let open_socket = open_socket.clone();
                let mut transport = transport.clone();
                let socket_factory = socket_factory.clone();
                let ech_resolver = ech_resolver.clone();
                async move {
                    transport.prepare_ech(ech_resolver.as_ref()).await?;
                    transport
                        .open_direct_unencrypted(
                            move |server, port| open_socket.clone()(server, port),
                            socket_factory,
                        )
                        .await
                }
            };
            async move {
                let stream = match preconnect {
                    Some(pool) => pool.take(open).await,
                    None => open().await,
                }?;
                finish.encrypt(stream).await
            }
        };
        protocol
            .open_tcp_stream_with_transport_or_mux(
                session,
                &self.server,
                self.port,
                self.uses_deferred_tcp_response(),
                &self.mux_pool,
                direct_transport,
            )
            .await
    }

    pub(super) fn direct_udp_resume(&self) -> VlessManagedUdpFlowResume {
        VlessManagedUdpFlowResume::new(
            self.mux_pool.clone(),
            self.protocol.udp_direct_flow_plan(),
            self.owned_transport_plan(),
            self.preconnect.clone(),
        )
    }

    pub(super) fn relay_two_stream_udp_resume(&self) -> VlessManagedUdpFlowResume {
        VlessManagedUdpFlowResume::new(
            self.mux_pool.clone(),
            self.protocol.udp_relay_paired_transport_plan(),
            self.owned_transport_plan(),
            None,
        )
    }

    pub(super) fn relay_final_hop_udp_resume(&self) -> VlessManagedUdpFlowResume {
        VlessManagedUdpFlowResume::new(
            self.mux_pool.clone(),
            self.protocol.udp_relay_final_hop_plan(),
            self.owned_transport_plan(),
            None,
        )
    }
}

impl ProtocolOutboundLeaf for VlessOutboundLeaf {
    fn tag(&self) -> &str {
        VlessOutboundLeaf::tag(self)
    }

    fn server(&self) -> &str {
        VlessOutboundLeaf::server(self)
    }

    fn port(&self) -> u16 {
        VlessOutboundLeaf::port(self)
    }

    fn udp_relay_final_hop_error(&self) -> Option<&'static str> {
        VlessOutboundLeaf::udp_relay_final_hop_error(self)
    }
}

impl ProtocolUdpFlowLeaf for VlessOutboundLeaf {
    type Resume = VlessManagedUdpFlowResume;

    fn direct_udp_resume(&self) -> Self::Resume {
        VlessOutboundLeaf::direct_udp_resume(self)
    }

    fn relay_final_hop_udp_resume(&self) -> Self::Resume {
        VlessOutboundLeaf::relay_final_hop_udp_resume(self)
    }
}

impl ProtocolRelayTwoStreamUdpFlowLeaf for VlessOutboundLeaf {
    fn udp_relay_needs_two_streams(&self) -> bool {
        VlessOutboundLeaf::relay_needs_two_streams(self)
    }

    fn relay_two_stream_udp_resume(&self) -> Self::Resume {
        VlessOutboundLeaf::relay_two_stream_udp_resume(self)
    }
}
