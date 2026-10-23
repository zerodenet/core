use crate::inventory::PreparedTcpRelayHop;
use crate::transport::establishment::EstablishmentFailure;
use crate::transport::TcpOutboundFailure;
use zero_engine::EngineError;

pub(super) fn hop_failure(
    hop: &PreparedTcpRelayHop,
    error: EngineError,
    stage: &'static str,
) -> TcpOutboundFailure {
    TcpOutboundFailure {
        stage,
        error: EstablishmentFailure::wrap(
            error,
            Some(hop.tag.clone()),
            hop.protocol.clone(),
            Some((hop.server.clone(), hop.port)),
            stage,
            None,
        ),
        upstream_endpoint: None,
        network: None,
    }
}
