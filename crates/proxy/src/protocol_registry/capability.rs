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
    ) -> Result<Box<dyn PreparedTcpConnectOperation + 'a>, TcpOutboundFailure>;

    fn prepare_tcp_relay_hop(
        &self,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<Box<dyn PreparedTcpRelayOperation + 'a>, EngineError> {
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

#[cfg(feature = "udp-runtime")]
pub(crate) trait ClaimedUdpPacketPathLeaf<'a>: Send + Sync {
    fn prepare_udp_packet_path(
        &self,
    ) -> Option<
        Box<
            dyn crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation
                + 'a,
        >,
    >;
}

pub(crate) struct OutboundLeafClaim<'a> {
    pub(crate) tcp_path: TcpPathCategory,
    pub(crate) tcp: Box<dyn ClaimedTcpOutboundLeaf<'a> + 'a>,
    #[cfg(feature = "udp-runtime")]
    pub(crate) udp: Option<Box<dyn ClaimedUdpFlowLeaf<'a> + 'a>>,
    #[cfg(feature = "udp-runtime")]
    pub(crate) packet_path: Option<Box<dyn ClaimedUdpPacketPathLeaf<'a> + 'a>>,
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
}

pub(crate) trait ProtocolSupportCapability: ProtocolMetadata + Send + Sync {
    fn name(&self) -> &'static str;
    fn feature_name(&self) -> &'static str;
    fn supports_inbound(&self, config: &InboundProtocolConfig) -> bool;
    fn supports_outbound(&self, config: &OutboundProtocolConfig) -> bool;
    fn has_inbound(&self) -> bool;
    fn has_outbound(&self) -> bool;

    fn on_config_reloaded(&self, _config: &RuntimeConfig) {}
}

#[async_trait]
pub(crate) trait InboundListenerCapability: Send + Sync {
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
