use std::io;

use super::*;
use crate::transport::{attributed_error, TransportFailureOrigin};

struct FailsWith(fn() -> EngineError);

impl PreparedTcpConnectOperation for FailsWith {
    fn execute<'a>(
        self: Box<Self>,
        _: TcpRuntimeServices,
        _: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<EstablishedTcpOutbound, TcpOutboundFailure>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            Err(TcpOutboundFailure {
                stage: "test_connect",
                error: (self.0)(),
                upstream_endpoint: None,
                network: None,
            })
        })
    }
}

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
        let session = test_session();
        for _ in 0..6 {
            let mut candidate = failing_candidate();
            candidate.execution =
                PreparedTcpCandidateExecution::Connect(Box::new(FailsWith(make_error)));
            let failure = dispatch_prepared_tcp_candidate(
                services.clone(),
                &session,
                candidate,
                TcpDispatchIntent::Traffic,
            )
            .await
            .err()
            .expect("local failure");
            assert_ne!(
                failure.stage, "health_check",
                "local errors must not turn into quarantine rejection"
            );
        }
        services
            .check_outbound_health(HEALTH_TAG)
            .expect("no local-failure quarantine");
        assert!(
            dispatch_prepared_tcp_candidate(
                services,
                &session,
                successful_candidate(),
                TcpDispatchIntent::Traffic
            )
            .await
            .is_ok(),
            "recovered traffic must be attempted immediately"
        );
    }
}
