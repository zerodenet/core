use super::super::ClaimedRelayChain;
use crate::runtime::udp_dispatch::packet_path_operation::PreparedDatagramRelayCarrier;
use crate::runtime::udp_dispatch::relay::{PreparedUdpRelayChain, PreparedUdpRelayOperation};
use crate::runtime::udp_dispatch::FlowFailure;

pub(super) fn prepare<'a>(
    claimed_chain: &ClaimedRelayChain<'a>,
    operation: Box<dyn PreparedUdpRelayOperation<'a> + 'a>,
) -> Result<PreparedUdpRelayChain<'a>, FlowFailure> {
    if claimed_chain.len() != 2 {
        return Err(failure(
            "datagram relay final hop currently requires exactly one packet-path carrier",
        ));
    }
    let carrier_operation = claimed_chain
        .first()
        .prepare_udp_packet_path()
        .ok_or_else(|| failure("relay prefix does not provide a datagram carrier"))?;
    let carrier = PreparedDatagramRelayCarrier::new(carrier_operation)
        .ok_or_else(|| failure("relay prefix does not expose datagram carrier identity"))?;
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
