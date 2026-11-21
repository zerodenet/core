use std::io;

use zero_engine::EngineError;

use super::{OutboundProbeError, OutboundProbeRequest};

#[test]
fn probe_preserves_literal_address_and_http_authority() {
    use zero_core::Address;
    for (authority, address, port) in [
        ("198.51.100.2", Address::Ipv4([198, 51, 100, 2]), 80),
        (
            "[::1]:8080",
            Address::Ipv6(std::net::Ipv6Addr::LOCALHOST.octets()),
            8080,
        ),
        (
            "node.example:81",
            Address::Domain("node.example".into()),
            81,
        ),
    ] {
        let probe = OutboundProbeRequest::parse(&format!("http://{authority}/probe")).unwrap();
        assert_eq!(probe.destination.address, address);
        assert_eq!(probe.destination.port, port);
        assert!(probe.request.starts_with("HEAD /probe HTTP/1.1\r\n"));
        assert!(probe
            .request
            .contains(&format!("\r\nHost: {authority}\r\n")));
    }
}

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
            EngineError::Core(zero_core::Error::Io("failed to connect direct target")),
            "connection_failed",
        ),
        (
            EngineError::Io(io::Error::from(io::ErrorKind::TimedOut)),
            "probe_timeout",
        ),
        (
            EngineError::Io(io::Error::other(
                zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Dial,
                    zero_platform_tokio::contract::Reason::Io,
                    zero_platform_tokio::contract::Impact::Component,
                )
                .with_source(io::Error::from(io::ErrorKind::ConnectionRefused)),
            )),
            "connection_failed",
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

#[test]
fn standalone_probe_error_retains_original_engine_io_source_without_changing_wire_code() {
    use std::{error::Error, sync::Arc};
    let source = Arc::new(io::Error::new(
        io::ErrorKind::ConnectionRefused,
        "actual standalone probe source",
    ));
    let original = EngineError::Io(io::Error::new(source.kind(), source.clone()));
    let error = OutboundProbeError::from_engine(original);
    assert_eq!(error.code(), "connection_failed");
    let EngineError::Io(io) = error
        .source()
        .unwrap()
        .downcast_ref::<EngineError>()
        .unwrap()
    else {
        panic!("original EngineError lost")
    };
    let retained = io
        .get_ref()
        .unwrap()
        .downcast_ref::<Arc<io::Error>>()
        .unwrap();
    assert!(Arc::ptr_eq(retained, &source));
}
