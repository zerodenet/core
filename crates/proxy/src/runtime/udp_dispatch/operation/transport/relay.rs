//! Existing owned relay operations; no protocol preparation callbacks.
use super::*;
use crate::runtime::udp_dispatch::relay::PreparedUdpRelayOperation;

pub(crate) struct PreparedTransportUdpRelay<TLeaf> {
    pub(crate) prepared: PreparedTransportLeaf<TLeaf>,
}
pub(crate) struct PreparedTwoStreamTransportUdpRelay<TLeaf> {
    pub(crate) prepared: PreparedTransportLeaf<TLeaf>,
    pub(crate) two_stream: bool,
}

impl<'a, TLeaf> PreparedUdpRelayOperation<'a> for PreparedTransportUdpRelay<TLeaf>
where
    TLeaf: ProxyTransportLeaf + ProxyTransportUdpLeaf + Send + 'a,
{
    fn bind_final_hop(
        self: Box<Self>,
        carrier: RelayCarrier,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        Ok(super::prepare_transport_udp_relay_final_hop(
            carrier,
            self.prepared,
        ))
    }
}

impl<'a, TLeaf> PreparedUdpRelayOperation<'a> for PreparedTwoStreamTransportUdpRelay<TLeaf>
where
    TLeaf: ProxyRelayTwoStreamTransportLeaf + Send + Sync + 'a,
{
    fn uses_lazy_stream_carrier(&self) -> bool {
        self.prepared.udp_relay_uses_connector()
    }
    fn bind_lazy_stream_carrier(
        self: Box<Self>,
        carrier: crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier<'a>,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        let connector = carrier.connector().ok_or_else(|| FlowFailure {
            stage: "udp_relay_connector",
            error: EngineError::Io(std::io::Error::other(
                "relay prefix has no reusable connector",
            )),
            upstream: None,
        })?;
        Ok(super::prepare_transport_udp_relay_connector(
            connector,
            self.prepared,
        ))
    }
    fn needs_two_streams(&self) -> bool {
        self.two_stream
    }

    fn bind_final_hop(
        self: Box<Self>,
        carrier: RelayCarrier,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        Ok(super::prepare_transport_udp_relay_final_hop(
            carrier,
            self.prepared,
        ))
    }

    fn bind_two_stream(
        self: Box<Self>,
        post_carrier: RelayCarrier,
        get_carrier: RelayCarrier,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        Ok(super::prepare_transport_udp_relay_two_stream(
            post_carrier,
            get_carrier,
            self.prepared,
        ))
    }
}
