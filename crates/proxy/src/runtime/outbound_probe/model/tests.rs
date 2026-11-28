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
fn typed_non_environmental_probe_origins_override_legacy_messages_directly() {
    assert_typed_non_environmental_probe_origins_override_legacy_messages(false);
}

#[test]
fn typed_non_environmental_probe_origins_override_legacy_messages_through_nested_io() {
    assert_typed_non_environmental_probe_origins_override_legacy_messages(true);
}

fn assert_typed_non_environmental_probe_origins_override_legacy_messages(nested: bool) {
    use crate::transport::{attributed_error, failure_origin, TransportFailureOrigin};

    for origin in [
        TransportFailureOrigin::Upstream,
        TransportFailureOrigin::Client,
    ] {
        for message in [
            "dns backend refused by remote peer",
            "tun route unavailable on remote peer",
        ] {
            let source = EngineError::Io(attributed_error(
                origin,
                "remote handshake",
                io::Error::other(message),
            ));
            let source = if nested {
                EngineError::Io(io::Error::other(source))
            } else {
                source
            };
            let error = OutboundProbeError::from_engine(source);
            let original_source = error.source.as_ref().unwrap().clone();
            let original_code = error.code();
            let original_message = error.message().to_owned();
            assert_eq!(failure_origin(original_source.as_ref()), Some(origin));
            // Preserve the conflicting text in both cases: a ChannelError
            // wrapper would hide its source Display and miss the regression.
            assert!(error.message().contains(message));
            assert!(
                !error.is_environmental_failure(),
                "typed origin must override legacy text: origin={origin:?}, nested={nested}, error={error}"
            );
            assert_eq!(error.code(), original_code);
            assert_eq!(error.message(), original_message);
            assert!(std::sync::Arc::ptr_eq(
                &original_source,
                error.source.as_ref().unwrap()
            ));
        }
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

#[test]
fn native_capacity_probe_failures_are_inconclusive_without_rewriting_the_error() {
    use std::error::Error;
    use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};

    for message in [
        "actual taskless original or caller ACK still occupies capacity",
        "previous actual taskless carrier cleanup is unfinished",
        // Classification is by the retained decision field, never these words.
        "opaque admission diagnostic",
    ] {
        for wrapped in [false, true] {
            let capacity = ChannelError::new(Stage::Dial, Reason::Capacity, Impact::Session)
                .with_source(io::Error::other(message));
            let native = if wrapped {
                ChannelError::new(Stage::Prepare, Reason::Io, Impact::Session)
                    .with_source(io::Error::other(capacity))
            } else {
                capacity
            };
            // ChannelError Display deliberately does not expand its sources.
            // Preserve that exact public diagnostic, including an outer wrapper.
            let expected_message = native.to_string();
            let expected_stage = native.stage;
            let expected_reason = native.reason;
            let expected_source = native.source().unwrap() as *const dyn Error;
            let error = OutboundProbeError::from_probe_failure(native);
            let before_source = error.source.as_ref().unwrap().clone();
            let EngineError::Io(io) = before_source.as_ref() else {
                panic!("native failure must retain its engine/io wrapper");
            };
            let retained = io
                .get_ref()
                .unwrap()
                .downcast_ref::<ChannelError>()
                .unwrap();
            assert_eq!(retained.stage, expected_stage);
            assert_eq!(retained.reason, expected_reason);
            assert!(std::ptr::eq(expected_source, retained.source().unwrap()));
            assert_eq!(error.code(), "probe_io_failed");
            assert_eq!(error.message(), expected_message);

            assert!(error.is_environmental_failure(), "{error}");
            assert_eq!(error.code(), "probe_io_failed");
            assert_eq!(error.message(), expected_message);
            assert!(std::sync::Arc::ptr_eq(
                &before_source,
                error.source.as_ref().unwrap()
            ));
            assert_eq!(
                crate::transport::failure_origin(before_source.as_ref()),
                Some(crate::transport::TransportFailureOrigin::LocalNetwork)
            );
        }
    }
}

#[test]
fn remote_probe_failures_remain_conclusive_even_when_diagnostic_mentions_capacity() {
    use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};

    for (stage, reason) in [
        (Stage::Dial, Reason::Io),
        (Stage::Handshake, Reason::Protocol),
    ] {
        for nested in [false, true] {
            let native = ChannelError::new(stage, reason, Impact::Session).with_source(
                io::Error::new(io::ErrorKind::ConnectionRefused, "remote capacity exceeded"),
            );
            let native = if nested {
                ChannelError::new(Stage::Prepare, Reason::Io, Impact::Session)
                    .with_source(io::Error::other(native))
            } else {
                native
            };
            let error = OutboundProbeError::from_probe_failure(native);
            assert!(
                !error.is_environmental_failure(),
                "nested={nested}: {error}"
            );
        }
    }
}

#[test]
fn typed_local_probe_origins_do_not_require_dns_or_tun_diagnostic_keywords() {
    use crate::transport::{attributed_error, TransportFailureOrigin};

    for origin in [
        TransportFailureOrigin::LocalNetwork,
        TransportFailureOrigin::NameResolution,
    ] {
        let error = OutboundProbeError::from_engine(
            attributed_error(
                origin,
                "opaque local failure",
                io::ErrorKind::TimedOut.into(),
            )
            .into(),
        );
        assert!(error.is_environmental_failure(), "{error}");
    }
}
