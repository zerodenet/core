#![cfg(feature = "runtime")]

use mieru::inbound::MieruInboundAcceptedSession;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::{
    Address, InboundStreamRoute, InboundStreamUdpRelay, Network, ProtocolType, Session, SessionAuth,
};
use zero_platform_tokio::TcpRelayStream;

fn session(network: Network) -> Session {
    let mut session = Session::new(
        17,
        Address::Domain("route.example".into()),
        443,
        network,
        ProtocolType::new("mieru"),
    );
    let mut auth = SessionAuth::new("mieru");
    auth.principal_key = Some("opaque-principal".into());
    session.apply_auth(auth);
    session
}

#[tokio::test]
async fn tcp_route_preserves_payload_identity_and_runtime_error() {
    let (stream, mut peer) = tokio::io::duplex(64);
    peer.write_all(b"payload").await.unwrap();
    let route = MieruInboundAcceptedSession::from_session_stream(
        session(Network::Tcp),
        TcpRelayStream::new(stream),
    );
    let result: Result<(), &'static str> = route
        .dispatch_inbound_route(
            |session, mut stream| async move {
                assert_eq!(session.target, Address::Domain("route.example".into()));
                assert_eq!(
                    session.auth.unwrap().principal_key.as_deref(),
                    Some("opaque-principal")
                );
                let mut payload = [0; 7];
                stream.read_exact(&mut payload).await.unwrap();
                assert_eq!(&payload, b"payload");
                Err("runtime route rejected")
            },
            |_, _| async { panic!("TCP must not dispatch to UDP") },
        )
        .await;
    assert_eq!(result, Err("runtime route rejected"));
}

#[tokio::test]
async fn udp_route_preserves_the_protocol_owned_relay_and_authentication() {
    let (stream, mut peer) = tokio::io::duplex(64);
    peer.write_all(b"payload").await.unwrap();
    let route = MieruInboundAcceptedSession::from_session_stream(
        session(Network::Udp),
        TcpRelayStream::new(stream),
    );
    let result: Result<(), &'static str> = route
        .dispatch_inbound_route(
            |_, _| async { panic!("UDP must not dispatch to TCP") },
            |session, relay| async move {
                assert_eq!(session.network, Network::Udp);
                let (mut stream, _responder, auth) = relay.into_stream_udp_parts();
                assert_eq!(auth, session.auth);
                let mut payload = [0; 7];
                stream.read_exact(&mut payload).await.unwrap();
                assert_eq!(&payload, b"payload");
                Ok(())
            },
        )
        .await;
    assert_eq!(result, Ok(()));
}
