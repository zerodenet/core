use std::io;

use zero_engine::EngineError;

use super::OutboundProbeError;

#[test]
fn local_egress_and_dns_failures_are_inconclusive() {
    for message in [
        "tun_ipv6_egress_unavailable",
        "failed to resolve upstream target",
        "DNS backend `system` timed out",
        "TUN route is temporarily unavailable",
    ] {
        let error = OutboundProbeError::from_engine(EngineError::Io(io::Error::other(message)));
        assert!(error.is_environmental_failure(), "{message}");
    }
}

#[test]
fn actual_node_connection_failure_is_not_environmental() {
    let error = OutboundProbeError::from_engine(EngineError::Io(io::Error::new(
        io::ErrorKind::ConnectionRefused,
        "proxy node refused the connection",
    )));
    assert!(!error.is_environmental_failure());
}
