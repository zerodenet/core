//! Same actual Source or admitted Kernel packet-call inputs; no new provider.
use super::*;
/// Registry-free services retained by prepared packet-path carrier builders.
#[derive(Clone)]
pub(crate) struct PacketPathExecutionServices {
    pub(super) packet_target: Option<zero_core::request::Destination>,
    pub(super) driver: Option<
        std::sync::Arc<zero_platform_tokio::contract::component::GrantedPacketServiceCaller>,
    >,
    pub(super) tcp: TcpExecutionServices,
    pub(super) network: UdpNetworkServices,
    pub(super) receive:
        Option<std::sync::Arc<[crate::protocol_registry::packet_memory::PacketCodecReceiveMemory]>>,
}

pub(crate) struct PacketPathDriverCall {
    pub(crate) pending: zero_platform_tokio::contract::network::PendingCarrierObserver,
    pub(crate) call: zero_platform_tokio::contract::network::CarrierContext,
    pub(crate) grant: zero_platform_tokio::contract::component::PacketDriverCallGrant,
}
impl PacketPathExecutionServices {
    /// Actual next wire destination already selected by this original caller.
    /// This is request data, never a resolver, native grant or replacement caller.
    pub(crate) fn with_packet_target(mut self, target: zero_core::request::Destination) -> Self {
        self.packet_target = Some(target);
        self
    }
    pub(crate) fn packet_target(&self) -> Option<&zero_core::request::Destination> {
        self.packet_target.as_ref()
    }
    /// Business identity must still be owned by its actual accepted/Raw caller.
    /// A real Service invocation is not relabelled Business just because it has
    /// no Source handoff; that existing caller keeps its original API duty.
    pub(crate) fn has_business_invocation(&self) -> bool {
        self.tcp.invocation.as_ref().is_some_and(|call| {
            matches!(
                &call.identity,
                zero_platform_tokio::contract::context::RequestIdentity::Business(_)
            )
        })
    }
    /// Presence of the actual Source/wire caller takes priority. A Kernel
    /// Business borrower instead needs its original admitted Invocation; None
    /// alone never identifies an independent library caller or grants IO.
    pub(crate) fn has_packet_driver_caller(&self) -> bool {
        self.driver.is_some()
    }
    pub(crate) fn with_packet_driver_receive(
        mut self,
        receive: std::sync::Arc<
            [crate::protocol_registry::packet_memory::PacketCodecReceiveMemory],
        >,
    ) -> Self {
        self.receive = Some(receive);
        self
    }
    pub(crate) fn packet_codec_receive(
        &self,
        index: usize,
    ) -> Result<
        Option<crate::protocol_registry::packet_memory::PacketCodecReceiveMemory>,
        zero_engine::EngineError,
    > {
        if self.driver.is_none() {
            return Ok(None);
        }
        self.receive
            .as_ref()
            .and_then(|groups| groups.get(index))
            .cloned()
            .map(Some)
            .ok_or_else(|| {
                std::io::Error::other("actual Packet Source has no prepaid codec receive workspace")
                    .into()
            })
    }
    pub(crate) fn with_packet_driver(
        mut self,
        actual: std::sync::Arc<
            zero_platform_tokio::contract::component::GrantedPacketServiceCaller,
        >,
    ) -> Self {
        self.driver = Some(actual);
        self
    }
    pub(crate) fn packet_driver_call(
        &self,
        target: zero_core::component::ComponentInstanceId,
        binding: &zero_platform_tokio::contract::network::UpstreamBinding,
    ) -> Result<Option<PacketPathDriverCall>, zero_platform_tokio::contract::ChannelError> {
        self.driver
            .as_ref()
            .map(|source| {
                let grant = source.for_target(target, binding)?;
                Ok(PacketPathDriverCall {
                    pending: grant.pending_observer(),
                    call: grant.call(),
                    grant,
                })
            })
            .transpose()
    }

    pub(crate) fn with_outbound_io(mut self, tag: &str) -> Self {
        let observer = self.tcp.outbound_io(tag, zero_api::TrafficPlane::Outer);
        self.network.upstream = self.network.upstream.with_observer(observer.clone());
        self.tcp.upstream = self.tcp.upstream.with_observer(observer);
        self
    }

    pub(crate) fn from_tcp_execution(services: &TcpExecutionServices) -> Self {
        Self {
            driver: None,
            packet_target: None,
            receive: None,
            tcp: services.clone(),
            network: UdpNetworkServices::from_tcp_execution(services),
        }
    }

    pub(crate) fn tcp(&self) -> TcpExecutionServices {
        self.tcp.clone()
    }

    pub(crate) fn network(&self) -> UdpNetworkServices {
        self.network.clone()
    }

    #[cfg(feature = "tls-ech-runtime")]
    pub(crate) fn ech_resolver(
        &self,
    ) -> std::sync::Arc<dyn zero_transport::tls::ech::EchConfigResolver> {
        self.tcp.upstream().ech_resolver()
    }

    pub(crate) fn prepare_lazy_tcp_relay_prefix<'a>(
        &self,
        prefix: crate::inventory::PreparedTcpRelayPrefix,
    ) -> crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier<'a> {
        crate::runtime::tcp_dispatch::relay::prepare_lazy_tcp_relay_prefix(self.tcp(), prefix)
    }
}
