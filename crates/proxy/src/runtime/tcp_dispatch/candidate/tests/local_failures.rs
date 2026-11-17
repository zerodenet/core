use std::io;

use super::*;
use crate::transport::{attributed_error, TransportFailureOrigin};

#[tokio::test]
async fn local_failures_do_not_block_the_first_connection_after_recovery() {
    let cases: [fn() -> EngineError; 4] = [
        || io::Error::from(io::ErrorKind::AddrNotAvailable).into(),
        || io::Error::from(io::ErrorKind::NetworkUnreachable).into(),
        || {
            attributed_error(
                TransportFailureOrigin::NameResolution,
                "resolver unavailable",
                io::ErrorKind::TimedOut.into(),
            )
            .into()
        },
        || {
            attributed_error(
                TransportFailureOrigin::LocalNetwork,
                "bind_interface",
                io::ErrorKind::NotFound.into(),
            )
            .into()
        },
    ];
    for make_error in cases {
        let services = test_services();
        for _ in 0..6 {
            let failure = complete_error(&services, TcpDispatchIntent::Traffic, make_error());
            assert_ne!(
                failure.stage, "health_check",
                "local errors must not turn into quarantine rejection"
            );
        }
        services
            .engine()
            .check_outbound_health(HEALTH_TAG)
            .expect("no local-failure quarantine");
        assert!(
            complete_wire(&services, TcpDispatchIntent::Traffic)
                .await
                .is_ok(),
            "recovered traffic must be attempted immediately"
        );
    }
}
