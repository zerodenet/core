use std::io;

use super::{attributed_error, failure_origin, TransportFailureOrigin};

#[test]
fn provenance_survives_runtime_engine_and_io_wrappers() {
    for origin in [
        TransportFailureOrigin::Client,
        TransportFailureOrigin::NameResolution,
        TransportFailureOrigin::LocalNetwork,
        TransportFailureOrigin::Upstream,
    ] {
        let error = attributed_error(
            origin,
            "controlled failure",
            io::Error::from(io::ErrorKind::ConnectionReset),
        );
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
        let runtime = zero_transport::RuntimeError::Io(error);
        assert_eq!(failure_origin(&runtime), Some(origin));
        let engine = zero_engine::EngineError::from(runtime);
        assert_eq!(failure_origin(&engine), Some(origin));
        let wrapped = io::Error::other(engine);
        assert_eq!(failure_origin(&wrapped), Some(origin));
    }
}

#[test]
fn local_address_errors_remain_local_without_tun_or_text_matching() {
    for kind in [
        io::ErrorKind::AddrNotAvailable,
        io::ErrorKind::NetworkDown,
        io::ErrorKind::NetworkUnreachable,
        io::ErrorKind::HostUnreachable,
    ] {
        let error =
            zero_engine::EngineError::Io(io::Error::new(kind, "opaque operating system detail"));
        assert_eq!(
            failure_origin(&error),
            Some(TransportFailureOrigin::LocalNetwork)
        );
    }
    assert_eq!(
        failure_origin(&io::Error::from(io::ErrorKind::ConnectionRefused)),
        None
    );
}
