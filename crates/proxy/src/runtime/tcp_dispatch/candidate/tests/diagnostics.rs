use super::*;
use crate::transport::establishment::establishment_failure;
use crate::transport::{failure_origin, TransportFailureOrigin};

struct DialThenFail {
    host: String,
    port: u16,
}

impl PreparedTcpConnectOperation for DialThenFail {
    fn execute<'a>(
        &'a self,
        services: TcpExecutionServices,
        _: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<EstablishedTcpOutbound, TcpOutboundFailure>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let error = match services
                .connect_upstream_owned(self.host.clone(), self.port)
                .await
            {
                Ok(_socket) => {
                    EngineError::Io(std::io::Error::other("controlled handshake failure"))
                }
                Err(error) => error.into(),
            };
            Err(TcpOutboundFailure {
                stage: "protocol_handshake",
                error,
                upstream_endpoint: None,
                network: None,
            })
        })
    }
}

async fn fail_at(host: &str, port: u16) -> EngineError {
    let mut candidate = failing_candidate();
    candidate.tag = Some("selected-node".to_owned());
    candidate.protocol = "test-protocol".to_owned();
    candidate.endpoint = Some((host.to_owned(), port));
    candidate.execution =
        PreparedTcpCandidateExecution::Connect(std::sync::Arc::new(DialThenFail {
            host: host.to_owned(),
            port,
        }));
    dispatch_prepared_tcp_candidate(
        test_services(),
        &test_session(),
        candidate,
        TcpDispatchIntent::Traffic,
    )
    .await
    .err()
    .expect("controlled establishment failure")
    .error
}

#[tokio::test]
async fn upstream_refusal_retains_selected_node_actual_ip_and_os_error_through_wrappers() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = listener.local_addr().unwrap();
    drop(listener);
    let error = fail_at("127.0.0.1", peer.port()).await;
    let wrapped = EngineError::Io(std::io::Error::other(error));
    let failure = establishment_failure(&wrapped).unwrap();
    assert_eq!(failure.tag.as_deref(), Some("selected-node"));
    assert_eq!(failure.stage, "connect_socket");
    let dial = failure.dial.as_ref().unwrap();
    assert_eq!(dial.remote, Some(peer));
    assert_eq!(dial.candidates, vec![peer]);
    assert!(dial.os_error.is_some());
    assert_eq!(
        failure_origin(&wrapped),
        Some(TransportFailureOrigin::Upstream)
    );
    let observation =
        crate::logging::session_failure_observation("route_or_establish", &wrapped, None);
    assert_eq!(observation.stage, "connect_socket");
    assert_eq!(observation.remote.unwrap().port, peer.port());
    let message = wrapped.to_string();
    for field in [
        "outbound_tag=selected-node",
        "dial_ip=127.0.0.1",
        "stage=connect_socket",
    ] {
        assert!(message.contains(field), "{message}");
    }
}

#[tokio::test]
async fn upstream_resolution_failure_has_no_invented_dial_ip() {
    let error = fail_at("invalid..node", 443).await;
    let failure = establishment_failure(&error).unwrap();
    assert_eq!(failure.stage, "resolve_node");
    let dial = failure.dial.as_ref().unwrap();
    assert_eq!(dial.remote, None);
    assert!(dial.candidates.is_empty());
    assert!(!error.to_string().contains("dial_ip="));
    assert_eq!(
        failure_origin(&error),
        Some(TransportFailureOrigin::NameResolution)
    );
}

#[tokio::test]
async fn upstream_handshake_failure_retains_successfully_dialed_peer_and_handshake_stage() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = listener.local_addr().unwrap();
    let error = fail_at("127.0.0.1", peer.port()).await;
    let failure = establishment_failure(&error).unwrap();
    assert_eq!(failure.stage, "protocol_handshake");
    let dial = failure.dial.as_ref().unwrap();
    assert_eq!(dial.stage, "connected");
    assert_eq!(dial.remote, Some(peer));
    assert!(dial.local.is_some());
}

#[tokio::test]
async fn concurrent_upstream_attempts_do_not_share_dial_metadata() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = listener.local_addr().unwrap();
    let (handshake, dns) = tokio::join!(
        fail_at("127.0.0.1", peer.port()),
        fail_at("invalid..node", 443)
    );
    assert_eq!(
        establishment_failure(&handshake)
            .unwrap()
            .dial
            .as_ref()
            .unwrap()
            .remote,
        Some(peer)
    );
    assert_eq!(
        establishment_failure(&dns)
            .unwrap()
            .dial
            .as_ref()
            .unwrap()
            .remote,
        None
    );
}

#[tokio::test]
async fn upstream_relay_wrapper_preserves_the_node_that_actually_failed() {
    let error = fail_at("invalid..node", 443).await;
    let wrapped = crate::transport::establishment::EstablishmentFailure::wrap(
        error,
        Some("final-relay-node".to_owned()),
        "test-relay".to_owned(),
        Some(("final.example".to_owned(), 443)),
        "relay_last",
        None,
    );
    let failure = establishment_failure(&wrapped).unwrap();
    assert_eq!(failure.tag.as_deref(), Some("selected-node"));
    assert_eq!(failure.stage, "resolve_node");
    assert_eq!(failure.endpoint.as_ref().unwrap().0, "invalid..node");
}
