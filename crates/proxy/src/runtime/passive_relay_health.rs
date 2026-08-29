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
mod tests {
    use std::io;

    use zero_core::{Address, Network, ProtocolType};
    use zero_engine::SessionOutcome;

    use super::*;

    fn record(network: Network, duration_ms: u64, tx: u64, rx: u64) -> CompletedSessionRecord {
        CompletedSessionRecord {
            id: 1,
            revision: 1,
            inbound_tag: Some("entry".to_owned()),
            outbound_tag: Some("hk-ss-1".to_owned()),
            route: None,
            path: zero_engine::FlowPathObservation::default(),
            target: Address::Domain("landing.example".to_owned()),
            original_target: None,
            target_host_source: None,
            fake_ip_reverse_status: None,
            port: 14788,
            protocol: ProtocolType::UNKNOWN,
            auth: None,
            network,
            mode: "rule".to_owned(),
            started_at_unix_ms: 0,
            last_activity_at_unix_ms: 0,
            finished_at_unix_ms: duration_ms,
            duration_ms,
            bytes_up: tx,
            bytes_down: rx,
            inbound_rx_bytes: tx,
            inbound_tx_bytes: rx,
            outbound_rx_bytes: rx,
            outbound_tx_bytes: tx,
            throughput_up_bps: 0,
            throughput_down_bps: 0,
            process_id: None,
            process_name: None,
            process_path: None,
            sni: None,
            source_ip: None,
            source_port: None,
            outcome: SessionOutcome::Failed,
            close_reason: Some("upstream_error".to_owned()),
            failure: None,
        }
    }

    #[test]
    fn classifies_early_transport_failures_for_tcp_and_udp() {
        let error = EngineError::Io(io::Error::other("shadowsocks unexpected EOF"));
        for network in [Network::Tcp, Network::Udp] {
            assert_eq!(
                classify_relay_outcome(&record(network, 459, 1749, 0), Some(&error)),
                PassiveRelayOutcome::Failure
            );
        }
    }

    #[test]
    fn upstream_data_wins_over_a_later_transport_error() {
        let error = EngineError::Io(io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe"));
        assert_eq!(
            classify_relay_outcome(&record(Network::Udp, 459, 1749, 1), Some(&error)),
            PassiveRelayOutcome::Success
        );
    }

    #[test]
    fn ignores_late_and_unclassified_failures() {
        let eof = EngineError::Io(io::Error::other("shadowsocks unexpected EOF"));
        let other = EngineError::Io(io::Error::other("application rejected request"));
        assert_eq!(
            classify_relay_outcome(&record(Network::Udp, 3_001, 1749, 0), Some(&eof)),
            PassiveRelayOutcome::Neutral
        );
        assert_eq!(
            classify_relay_outcome(&record(Network::Udp, 459, 1749, 0), Some(&other)),
            PassiveRelayOutcome::Neutral
        );
    }

    #[test]
    fn local_tun_failures_do_not_penalize_outbound_health() {
        for message in [
            "connection reset by local client",
            "local TUN TCP acknowledgement timed out",
            "local TUN packet transport closed",
        ] {
            let error = EngineError::Io(io::Error::other(message));
            assert_eq!(
                classify_relay_outcome(&record(Network::Tcp, 100, 1_024, 0), Some(&error)),
                PassiveRelayOutcome::Neutral
            );
        }
    }

    #[test]
    fn establishment_failures_keep_local_egress_and_dns_errors_neutral() {
        let error = EngineError::Io(io::Error::new(
            io::ErrorKind::NotConnected,
            "TUN physical egress is unavailable",
        ));
        let network = FlowNetworkObservation {
            egress: Some(zero_engine::FlowEgressObservation {
                generation: 7,
                address_family: "ipv6".to_owned(),
                tun_active: true,
                configured_interface: None,
                unavailable_reason: Some("no_default_route".to_owned()),
            }),
            socket_binding: Some(zero_engine::FlowSocketBindingObservation {
                mode: "system".to_owned(),
                reason: "tun_egress_unavailable".to_owned(),
                interface_bound: false,
            }),
            ..FlowNetworkObservation::default()
        };
        assert_eq!(
            classify_outbound_establishment_failure(&error, Some(&network)),
            PassiveRelayOutcome::Neutral
        );

        let dns = EngineError::Io(io::Error::other("failed to resolve upstream target"));
        assert_eq!(
            classify_outbound_establishment_failure(&dns, None),
            PassiveRelayOutcome::Neutral
        );
    }

    #[test]
    fn establishment_failure_still_penalizes_a_real_node_failure() {
        let error = EngineError::Io(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            "proxy node refused the connection",
        ));
        assert_eq!(
            classify_outbound_establishment_failure(&error, None),
            PassiveRelayOutcome::Failure
        );
    }
}
