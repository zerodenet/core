use zero_config::{
    InboundConfig, InboundProtocolConfig, OutboundConfig, OutboundProtocolConfig, RuntimeConfig,
};
use zero_engine::EngineError;
use zero_traits::ProtocolMetadata;

use crate::runtime::path::TcpPathCategory;
use crate::runtime::tcp_dispatch::operation::{
    PreparedTcpConnectOperation, PreparedTcpRelayOperation,
};
#[cfg(feature = "udp-runtime")]
use crate::runtime::udp_dispatch::operation::PreparedUdpFlowOperation;
#[cfg(feature = "udp-runtime")]
use crate::runtime::udp_dispatch::relay::PreparedUdpRelayOperation;

// Cold result data, not another protocol preparation interface. Invalid TCP
// materialization remains dormant until the corresponding execution is selected.
pub(crate) type TcpConnectResult =
    Result<std::sync::Arc<dyn PreparedTcpConnectOperation>, (&'static str, String)>;
pub(crate) type TcpRelayResult = Result<std::sync::Arc<dyn PreparedTcpRelayOperation>, String>;

#[cfg(feature = "udp-runtime")]
pub(crate) type UdpFlowResult<'a> =
    Result<Box<dyn PreparedUdpFlowOperation + 'a>, crate::runtime::udp_dispatch::FlowFailure>;
#[cfg(feature = "udp-runtime")]
pub(crate) type UdpRelayResult<'a> =
    Result<Box<dyn PreparedUdpRelayOperation<'a> + 'a>, crate::runtime::udp_dispatch::FlowFailure>;

pub(crate) struct OutboundLeafClaim<'a> {
    #[cfg(not(feature = "udp-runtime"))]
    pub(crate) _lifetime: std::marker::PhantomData<&'a ()>,
    pub(crate) tcp_path: TcpPathCategory,
    pub(crate) tcp: Option<TcpConnectResult>,
    pub(crate) tcp_relay: Option<TcpRelayResult>,
    #[cfg(feature = "udp-runtime")]
    pub(crate) udp: Option<UdpFlowResult<'a>>,
    #[cfg(feature = "udp-runtime")]
    pub(crate) udp_relay: Option<UdpRelayResult<'a>>,
    #[cfg(feature = "udp-runtime")]
    /// An inert, source-materialized operation, shared by all projections of
    /// this leaf. Acquisition and retained-owner checks stay in its executor.
    pub(crate) packet_path: Option<
        std::sync::Arc<
            dyn crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation,
        >,
    >,
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) packet: Option<crate::runtime::packet_route::PacketRouteOperations>,
}

#[derive(Clone)]
pub(crate) enum OutboundLeafInput<'a> {
    Direct {
        tag: Option<&'a str>,
        dial_policy: zero_traits::DialPolicy,
        dial_generation: u64,
    },
    Proxy {
        config: &'a zero_config::RuntimeConfig,
        outbound: &'a OutboundConfig,
        endpoint: (&'a str, u16),
        #[allow(dead_code, reason = "used by materialized transport capabilities")]
        source_dir: Option<&'a std::path::Path>,
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

    fn validate_runtime_candidate(&self, _config: &RuntimeConfig) -> Result<(), EngineError> {
        Ok(())
    }
    fn on_config_reloaded(&self, _config: &RuntimeConfig) {}
}
