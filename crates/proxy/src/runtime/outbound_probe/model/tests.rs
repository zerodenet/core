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

#[test]
fn establishment_metadata_preserves_typed_failure_classification_and_diagnostics() {
    use crate::transport::establishment::EstablishmentFailure;

    for (source, expected) in [
        (
            EngineError::Core(zero_core::Error::Io(io::Error::from(
                io::ErrorKind::ConnectionRefused,
            ))),
            "connection_failed",
        ),
        (
            EngineError::Io(io::Error::from(io::ErrorKind::TimedOut)),
            "probe_timeout",
        ),
    ] {
        let error = OutboundProbeError::from_engine(EstablishmentFailure::wrap(
            source,
            Some("direct".to_owned()),
            "direct".to_owned(),
            None,
            "connect_socket",
            None,
        ));
        assert_eq!(error.code(), expected);
        assert!(error.message().contains("outbound_tag=direct"));
        assert!(error.message().contains("stage=connect_socket"));
    }
}
