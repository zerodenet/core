use zero_engine::{
    CompletedSessionRecord, EngineError, FlowNetworkObservation, PassiveRelayOutcome,
};

use crate::runtime::relay_failure::classify_relay_failure;

const EARLY_RELAY_FAILURE_LIMIT_MS: u64 = 3_000;

pub(crate) fn classify_relay_outcome(
    record: &CompletedSessionRecord,
    error: Option<&EngineError>,
) -> PassiveRelayOutcome {
    if error.is_some_and(|error| !classify_relay_failure(error).upstream) {
        return PassiveRelayOutcome::Neutral;
    }
    if record.outbound_rx_bytes > 0 {
        return PassiveRelayOutcome::Success;
    }

    if record.duration_ms <= EARLY_RELAY_FAILURE_LIMIT_MS
        && record.outbound_tx_bytes > 0
        && error.is_some_and(is_early_transport_failure)
    {
        return PassiveRelayOutcome::Failure;
    }

    PassiveRelayOutcome::Neutral
}

/// Attribute failures that happen before a relay stream exists. Local TUN
/// family availability and DNS/bootstrap failures do not prove that the
/// selected proxy member is unhealthy, so they must remain neutral.
pub(crate) fn classify_outbound_establishment_failure(
    error: &EngineError,
    network: Option<&FlowNetworkObservation>,
) -> PassiveRelayOutcome {
    if !classify_relay_failure(error).upstream
        || matches!(error, EngineError::UnhealthyOutbound { .. })
    {
        return PassiveRelayOutcome::Neutral;
    }
    if network.is_some_and(|network| {
        network
            .socket_binding
            .as_ref()
            .is_some_and(|binding| binding.reason == "tun_egress_unavailable")
            || network
                .egress
                .as_ref()
                .is_some_and(|egress| egress.tun_active && egress.unavailable_reason.is_some())
    }) {
        return PassiveRelayOutcome::Neutral;
    }

    let message = error.to_string().to_ascii_lowercase();
    if message.contains("tun physical egress is unavailable")
        || message.contains("tun_ipv4_egress_unavailable")
        || message.contains("tun_ipv6_egress_unavailable")
        || message.contains("failed to resolve upstream target")
        || message.contains("failed to resolve proxy node")
        || message.contains("dns backend")
    {
        PassiveRelayOutcome::Neutral
    } else {
        PassiveRelayOutcome::Failure
    }
}

fn is_early_transport_failure(error: &EngineError) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("unexpected eof")
        || message.contains("broken pipe")
        || message.contains("connection reset")
        || message.contains("forcibly closed")
        || message.contains("os error 10054")
}

#[cfg(test)]
mod tests;
