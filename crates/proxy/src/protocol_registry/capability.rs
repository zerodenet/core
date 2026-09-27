use async_trait::async_trait;

use zero_config::{
    InboundConfig, InboundProtocolConfig, OutboundConfig, OutboundProtocolConfig, RuntimeConfig,
};
use zero_engine::EngineError;
use zero_traits::ProtocolMetadata;

use super::BoundInbound;
use crate::runtime::path::TcpPathCategory;
use crate::runtime::tcp_dispatch::operation::{
    PreparedTcpConnectOperation, PreparedTcpRelayOperation,
};
#[cfg(feature = "udp-runtime")]
use crate::runtime::udp_dispatch::operation::PreparedUdpFlowOperation;
#[cfg(feature = "udp-runtime")]
use crate::runtime::udp_dispatch::relay::PreparedUdpRelayOperation;
#[cfg(feature = "managed-datagram-runtime")]
use crate::runtime::udp_flow::managed::model::ManagedDatagramFlowHandler;
#[cfg(feature = "managed-stream-runtime")]
use crate::runtime::udp_flow::managed::ManagedStreamHandlerPair;
#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_flow::registered::UpstreamAssociationHandler;
use crate::transport::TcpOutboundFailure;

pub(crate) trait ClaimedTcpOutboundLeaf<'a>: Send + Sync {
    fn prepare_tcp_connect(
        &self,
        source_dir: Option<&std::path::Path>,
    ) -> Result<Box<dyn PreparedTcpConnectOperation>, TcpOutboundFailure>;

    fn prepare_tcp_relay_hop(
        &self,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<Box<dyn PreparedTcpRelayOperation>, EngineError> {
        Err(super::defaults::relay_hop_unsupported())
    }
}

#[cfg(feature = "udp-runtime")]
pub(crate) trait ClaimedUdpFlowLeaf<'a>: Send + Sync {
    fn prepare_udp_flow(
        &self,
        source_dir: Option<&std::path::Path>,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, crate::runtime::udp_dispatch::FlowFailure>;

    fn prepare_udp_relay(
        &self,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<
        Box<dyn PreparedUdpRelayOperation<'a> + 'a>,
        crate::runtime::udp_dispatch::FlowFailure,
    > {
        Err(super::defaults::udp_relay_final_hop_unsupported())
    }
}

#[cfg(feature = "raw-ip-runtime")]
pub(crate) trait ClaimedPacketLeaf: Send + Sync {
    fn prepare_packet_route(
        &self,
    ) -> Box<dyn crate::runtime::packet_route::PreparedPacketRouteOperation>;

    fn prepare_datagram_exchange(
        &self,
    ) -> Option<Box<dyn crate::runtime::packet_route::PreparedDatagramExchangeOperation>> {
        None
    }

    /// A PacketSink may expose a shared user-space stack for L4 callers.
    /// The adapter owns local addressing; generic routing only selects this edge.
    fn prepare_tcp_flow(&self) -> Option<Box<dyn PreparedTcpConnectOperation>> {
        None
    }

    #[cfg(feature = "udp-runtime")]
    fn prepare_udp_flow(&self) -> Option<Box<dyn PreparedUdpFlowOperation>> {
        None
    }
}

#[cfg(feature = "udp-runtime")]
pub(crate) trait ClaimedUdpPacketPathLeaf<'a>: Send + Sync {
    fn prepare_udp_packet_path(
        &self,
        source_dir: Option<&std::path::Path>,
    ) -> Option<
        Box<
            dyn crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation,
        >,
    >;
}

pub(crate) struct OutboundLeafClaim<'a> {
    pub(crate) tcp_path: TcpPathCategory,
    pub(crate) tcp: Option<Box<dyn ClaimedTcpOutboundLeaf<'a> + 'a>>,
    #[cfg(feature = "udp-runtime")]
    pub(crate) udp: Option<Box<dyn ClaimedUdpFlowLeaf<'a> + 'a>>,
    #[cfg(feature = "udp-runtime")]
    pub(crate) packet_path: Option<Box<dyn ClaimedUdpPacketPathLeaf<'a> + 'a>>,
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) packet: Option<Box<dyn ClaimedPacketLeaf>>,
}

#[derive(Clone, Copy)]
pub(crate) enum OutboundLeafInput<'a> {
    Direct {
        tag: Option<&'a str>,
    },
    Proxy {
        outbound: &'a OutboundConfig,
        endpoint: (&'a str, u16),
    },
    Virtual {
        outbound: &'a OutboundConfig,
    },
}

pub(crate) trait ProtocolSupportCapability: ProtocolMetadata + Send + Sync {
    fn name(&self) -> &'static str;
    fn feature_name(&self) -> &'static str;
    fn supports_inbound(&self, config: &InboundProtocolConfig) -> bool;
    fn supports_outbound(&self, config: &OutboundProtocolConfig) -> bool;
    fn has_inbound(&self) -> bool;
    fn has_outbound(&self) -> bool;

    fn validate_inbound_config(&self, _config: &InboundConfig) -> Result<(), EngineError> {
        Ok(())
    }

    fn on_config_reloaded(&self, _config: &RuntimeConfig) {}
}

#[cfg(feature = "raw-ip-runtime")]
#[async_trait]
pub(crate) trait OutboundDeviceLifecycleCapability: ProtocolSupportCapability {
    async fn prepare_outbound_devices(
        &self,
        outbounds: &[&OutboundConfig],
        inbounds: &[InboundConfig],
        context: crate::protocol_registry::OutboundDevicePreparationContext,
    ) -> Result<Box<dyn PreparedOutboundDeviceState>, EngineError>;

    fn shutdown_outbound_devices(&self);

    fn outbound_device_health(
        &self,
        _outbounds: &[&OutboundConfig],
    ) -> Vec<zero_api::OutboundDeviceHealthSnapshot> {
        Vec::new()
    }
}

#[cfg(feature = "raw-ip-runtime")]
pub(crate) trait PreparedOutboundDeviceState: Send {
    fn publish(self: Box<Self>);
}

#[async_trait]
pub(crate) trait InboundListenerCapability: Send + Sync {
    /// A related outbound change may alter an inbound's runtime shape even
    /// when its own configuration is unchanged.
    fn inbound_listener_requires_restart(&self, _inbound: &InboundConfig) -> bool {
        false
    }

    /// Apply a prepared update to an active listener without replacing its
    /// bound socket. Returning false leaves the ordinary restart path intact.
    fn update_inbound_listener(
        &self,
        _inbound: InboundConfig,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<bool, EngineError> {
        Ok(false)
    }

    /// Bind the listener socket eagerly so port-in-use errors surface before
    /// the proxy announces "started".
    async fn bind_inbound(
        &self,
        inbound: &InboundConfig,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<BoundInbound, EngineError> {
        super::defaults::bind_tcp_inbound(inbound).await
    }

    /// Validate protocol-local listener state and prepare a runtime-executed
    /// listener operation.
    fn prepare_inbound_listener(
        &self,
        _inbound: InboundConfig,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<
        Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation>,
        EngineError,
    > {
        Err(EngineError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "this adapter does not provide an inbound listener",
        )))
    }

    /// Recreate the published listener after a candidate reload failed.
    fn prepare_rollback_inbound_listener(
        &self,
        inbound: InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<
        Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation>,
        EngineError,
    > {
        self.prepare_inbound_listener(inbound, source_dir)
    }
}

pub(crate) trait TcpOutboundCapability: Send + Sync {}

#[cfg(feature = "udp-runtime")]
pub(crate) trait UdpFlowCapability: Send + Sync {}

#[cfg(feature = "upstream-association-runtime")]
pub(crate) trait UpstreamUdpHandlerProvider: Send + Sync {
    fn upstream_association_handler(&self) -> Box<dyn UpstreamAssociationHandler>;
}

#[cfg(any(
    feature = "managed-datagram-runtime",
    feature = "managed-stream-runtime"
))]
pub(crate) trait ManagedUdpHandlerProvider: Send + Sync {
    #[cfg(feature = "managed-datagram-runtime")]
    fn managed_datagram_udp_handler(&self) -> Option<Box<dyn ManagedDatagramFlowHandler>> {
        None
    }

    #[cfg(feature = "managed-stream-runtime")]
    fn managed_stream_udp_handlers(&self) -> Option<ManagedStreamHandlerPair> {
        None
    }
}

#[cfg(feature = "udp-runtime")]
pub(crate) trait UdpPacketPathCapability: Send + Sync {}
