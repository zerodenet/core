use std::io;

use zero_core::{Address, Network, ProtocolType};
use zero_engine::SessionOutcome;

use super::*;
use crate::transport::{attributed_error, TransportFailureOrigin};

#[test]
fn client_cancellation_and_local_network_loss_do_not_quarantine_nodes() {
    for origin in [
        TransportFailureOrigin::Client,
        TransportFailureOrigin::LocalNetwork,
    ] {
        let error = EngineError::Io(attributed_error(
            origin,
            "TCP read failed",
            io::ErrorKind::ConnectionReset.into(),
        ));
        // SS sends its address header before the client sends any application bytes.
        let mut completed = record(Network::Tcp, 268, 81, 0);
        completed.inbound_rx_bytes = 0;
        assert_eq!(
            classify_relay_outcome(&completed, Some(&error)),
            PassiveRelayOutcome::Neutral
        );
    }
    let error = EngineError::Io(attributed_error(
        TransportFailureOrigin::Upstream,
        "TCP read failed",
        io::ErrorKind::ConnectionReset.into(),
    ));
    assert_eq!(
        classify_relay_outcome(&record(Network::Tcp, 268, 81, 0), Some(&error)),
        PassiveRelayOutcome::Failure
    );
}

#[test]
fn health_gate_rejection_is_not_new_evidence_against_a_policy_member() {
    let error = EngineError::UnhealthyOutbound {
        tag: "node".to_owned(),
    };
    assert_eq!(
        classify_outbound_establishment_failure(&error, None),
        PassiveRelayOutcome::Neutral
    );
}

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
