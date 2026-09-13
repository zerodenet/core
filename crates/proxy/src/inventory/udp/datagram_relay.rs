use crate::runtime::udp_dispatch::packet_path_operation::PreparedDatagramRelayCarrier;
use crate::runtime::udp_dispatch::relay::{PreparedUdpRelayChain, PreparedUdpRelayOperation};
use crate::runtime::udp_dispatch::FlowFailure;

pub(super) fn prepare<'a>(
    carrier: Option<PreparedDatagramRelayCarrier>,
    operation: Box<dyn PreparedUdpRelayOperation<'a> + 'a>,
) -> Result<PreparedUdpRelayChain<'a>, FlowFailure> {
    let carrier = carrier
        .ok_or_else(|| failure("relay prefix does not provide a composable datagram carrier"))?;
    Ok(PreparedUdpRelayChain::DatagramFinalHop { carrier, operation })
}

fn failure(message: &'static str) -> FlowFailure {
    FlowFailure {
        stage: "udp_relay_datagram_carrier",
        error: zero_engine::EngineError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            message,
        )),
        upstream: None,
    }
}
