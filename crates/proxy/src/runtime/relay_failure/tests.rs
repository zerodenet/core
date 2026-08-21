use std::io;

use zero_engine::EngineError;

use super::{classify_relay_failure, RelayFailureAttribution};

#[test]
fn distinguishes_client_tun_and_upstream_failures() {
    let client = EngineError::Io(io::Error::new(
        io::ErrorKind::ConnectionReset,
        "connection reset by local client",
    ));
    assert_eq!(
        classify_relay_failure(&client),
        RelayFailureAttribution {
            close_reason: "client_error",
            stage: "client_transport",
            upstream: false,
        }
    );

    let tun = EngineError::Io(io::Error::new(
        io::ErrorKind::TimedOut,
        "local TUN TCP acknowledgement timed out",
    ));
    assert_eq!(classify_relay_failure(&tun).close_reason, "tun_error");

    let upstream = EngineError::Io(io::Error::new(
        io::ErrorKind::ConnectionReset,
        "remote host forcibly closed the connection",
    ));
    assert!(classify_relay_failure(&upstream).upstream);
}
