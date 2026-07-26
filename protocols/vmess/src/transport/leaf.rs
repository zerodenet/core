use std::future::Future;
use std::path::Path;

use zero_core::Session;
use zero_platform_tokio::{TcpRelayStream, TokioSocket};
use zero_traits::{
    ClientTlsProfile, GrpcTransportProfile, ProtocolOutboundLeaf, ProtocolUdpFlowLeaf,
    WebSocketTransportProfile,
};
use zero_transport::RuntimeError;

use super::managed_udp::VmessManagedUdpFlowResume;
use super::outbound::OwnedVmessOutboundTransportPlan;
use super::runtime::VmessTransportRuntime;

#[derive(Clone)]
pub struct VmessOutboundLeaf {
    tag: String,
    server: String,
    port: u16,
    transport: OwnedVmessOutboundTransportPlan,
    protocol: crate::outbound::PreparedVmessOutboundRequestBundle,
    mux_pool: crate::mux::VmessMuxConnectionPool,
}

impl VmessOutboundLeaf {
    pub fn from_options_refs<TTls, TWs, TGrpc>(
        source_dir: Option<&Path>,
        options: super::options::VmessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc>,
        runtime: &VmessTransportRuntime,
    ) -> Result<Self, zero_core::Error>
    where
        TTls: ClientTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
    {
        let super::options::VmessOutboundBuildOptionsRef {
            tag,
            server,
            port,
            protocol,
            tls,
            ws,
            grpc,
        } = options;
        Self::from_profile_refs(
            source_dir,
            tag,
            server,
            port,
            protocol.id,
            protocol.cipher,
            protocol.mux_concurrency,
            protocol.mux_idle_timeout_secs,
            protocol.mux_response_backlog_frames,
            protocol.mux_response_backlog_bytes,
            tls,
            ws,
            grpc,
            runtime.mux_pool(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::transport) fn from_profile_refs<TTls, TWs, TGrpc>(
        source_dir: Option<&Path>,
        tag: &str,
        server: &str,
        port: u16,
        id: &str,
        cipher: &str,
        mux_concurrency: Option<u32>,
        mux_idle_timeout_secs: Option<u64>,
        mux_response_backlog_frames: Option<u32>,
        mux_response_backlog_bytes: Option<u64>,
        tls: Option<&TTls>,
        ws: Option<&TWs>,
        grpc: Option<&TGrpc>,
        mux_pool: crate::mux::VmessMuxConnectionPool,
    ) -> Result<Self, zero_core::Error>
    where
        TTls: ClientTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
    {
        let transport = OwnedVmessOutboundTransportPlan::from_profile_refs(
            source_dir, server, port, tls, ws, grpc,
        );
        let protocol =
            crate::outbound::PreparedVmessOutboundRequestBundle::from_config_with_transport_hints_and_mux_policy(
                id,
                cipher,
                mux_concurrency,
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
        transport: OwnedVmessOutboundTransportPlan,
        protocol: crate::outbound::PreparedVmessOutboundRequestBundle,
        mux_pool: crate::mux::VmessMuxConnectionPool,
    ) -> Self {
        Self {
            tag: tag.to_owned(),
            server: server.to_owned(),
            port,
            protocol,
            transport,
            mux_pool,
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

    fn owned_transport_plan(&self) -> OwnedVmessOutboundTransportPlan {
        self.transport.clone()
    }

    pub async fn open_tcp_stream<OpenSocket, OpenSocketFut>(
        &self,
        session: &Session,
        open_socket: OpenSocket,
    ) -> Result<crate::outbound::VmessTcpStreamOpen, RuntimeError>
    where
        OpenSocket: Clone + Fn(&str, u16) -> OpenSocketFut + Send + Sync,
        OpenSocketFut: Future<Output = Result<TokioSocket, RuntimeError>> + Send,
    {
        let protocol = self.protocol.clone();
        let transport = self.owned_transport_plan();
        let direct_transport =
            || transport.open_direct(move |server, port| open_socket.clone()(server, port));
        protocol
            .open_tcp_stream_with_transport_or_mux(
                session,
                &self.server,
                self.port,
                &self.mux_pool,
                direct_transport,
            )
            .await
    }

    pub async fn open_tcp_relay_hop(
        &self,
        stream: TcpRelayStream,
        session: &Session,
    ) -> Result<TcpRelayStream, RuntimeError> {
        let protocol = self.protocol.clone();
        let transport = self.owned_transport_plan();
        protocol
            .open_tcp_relay_hop_with_transport(session, || transport.open_relay(stream))
            .await
            .map(TcpRelayStream::new)
    }

    pub(super) fn direct_udp_resume(&self) -> VmessManagedUdpFlowResume {
        VmessManagedUdpFlowResume::new(
            self.mux_pool.clone(),
            self.protocol.udp_direct_flow_plan(),
            self.owned_transport_plan(),
        )
    }

    pub(super) fn relay_final_hop_udp_resume(&self) -> VmessManagedUdpFlowResume {
        VmessManagedUdpFlowResume::new(
            self.mux_pool.clone(),
            self.protocol.udp_relay_flow_plan(),
            self.owned_transport_plan(),
        )
    }
}

impl ProtocolOutboundLeaf for VmessOutboundLeaf {
    fn tag(&self) -> &str {
        VmessOutboundLeaf::tag(self)
    }

    fn server(&self) -> &str {
        VmessOutboundLeaf::server(self)
    }

    fn port(&self) -> u16 {
        VmessOutboundLeaf::port(self)
    }
}

impl ProtocolUdpFlowLeaf for VmessOutboundLeaf {
    type Resume = VmessManagedUdpFlowResume;

    fn direct_udp_resume(&self) -> Self::Resume {
        VmessOutboundLeaf::direct_udp_resume(self)
    }

    fn relay_final_hop_udp_resume(&self) -> Self::Resume {
        VmessOutboundLeaf::relay_final_hop_udp_resume(self)
    }
}
