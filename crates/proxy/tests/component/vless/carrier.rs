use super::fixtures::*;
use std::sync::{atomic::Ordering, Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{timeout, Duration},
};
use zero_platform_tokio::contract::stream::{BoxStream, ConnectRequest, PeerConfirmation};
use zero_transport::profile::OwnedWebSocketProfile;

#[tokio::test]
async fn real_tls_websocket_keeps_its_profile_and_only_vless_bytes_in_flow() {
    for early in [false, true] {
        timeout(Duration::from_secs(5), async {
            let ws = OwnedWebSocketProfile {
                accept_proxy_protocol: false,
                heartbeat_period_secs: 0,
                host: Some("native.example".into()),
                path: if early {
                    "/native-vless?ed=4096"
                } else {
                    "/native-vless"
                }
                .into(),
                headers: vec![("x-native-profile".into(), "preserved".into())],
            };
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let acceptor = acceptor_with_alpn(false);
            let server_ws = ws.clone();
            let server = tokio::spawn(async move {
                let (io, _) = listener.accept().await.unwrap();
                let tls = acceptor.accept(io).await.unwrap();
                assert_eq!(tls.get_ref().1.alpn_protocol(), None);
                let mut stream: BoxStream = Box::pin(
                    zero_transport::ws::accept_ws_profile(tls, &server_ws)
                        .await
                        .unwrap(),
                );
                let expected = request();
                let mut header = vec![0; expected.len()];
                stream.read_exact(&mut header).await.unwrap();
                assert_eq!(header, expected);
                let mut payload = [0; 7];
                stream.read_exact(&mut payload).await.unwrap();
                assert_eq!(&payload, b"payload");
                stream.write_all(b"\x00\x00carrier-echo").await.unwrap();
                stream.flush().await.unwrap();
                // Keep the carrier alive until the client receives the response.
                let mut byte = [0];
                let _ = stream.read(&mut byte).await;
            });
            let observations = Arc::new(Observations::default());
            let tls = tls_profile();
            let (mut instance, mut scope) = instance(address, Some(&tls), Some(&ws), None);
            instance
                .lifecycle
                .start(super::super::fixtures::deadline())
                .await
                .unwrap();
            let mut connected = instance
                .roles
                .stream
                .as_ref()
                .unwrap()
                .connect(
                    ConnectRequest {
                        destination: destination(),
                    },
                    context(address, observations.clone()),
                )
                .await
                .unwrap();
            assert_eq!(connected.peer_confirmation, PeerConfirmation::Deferred);
            assert_eq!(
                *observations.protocol.lock().unwrap(),
                Some((0, request().len() as u64))
            );
            assert!(observations.outer.load(Ordering::Relaxed) > request().len() as u64);
            connected.io.write_all(b"payload").await.unwrap();
            connected.io.flush().await.unwrap();
            let mut echoed = [0; 12];
            connected.io.read_exact(&mut echoed).await.unwrap();
            assert_eq!(&echoed, b"carrier-echo");
            assert_eq!(
                observations.facts.lock().unwrap().last(),
                Some(&(
                    None,
                    zero_platform_tokio::contract::context::ConnectFact::PeerConfirmed
                ))
            );
            drop(connected);
            server.await.unwrap();
            instance
                .lifecycle
                .shutdown(super::super::fixtures::deadline())
                .await
                .unwrap();
            assert!(scope
                .shutdown(super::super::fixtures::deadline())
                .await
                .is_complete());
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn rejected_actual_lazy_websocket_upgrade_does_not_report_ready_or_proxy_submission() {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let acceptor = acceptor_with_alpn(false);
        let ws = OwnedWebSocketProfile {
            accept_proxy_protocol: false,
            heartbeat_period_secs: 0,
            host: None,
            path: "/native?ed=4096".into(),
            headers: vec![],
        };
        let mut rejected = ws.clone();
        rejected.path = "/different".into();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let tls = acceptor.accept(io).await.unwrap();
            assert!(zero_transport::ws::accept_ws_profile(tls, &rejected)
                .await
                .is_err());
        });
        let observations = Arc::new(Observations::default());
        let tls = tls_profile();
        let (mut instance, mut scope) = instance(address, Some(&tls), Some(&ws), None);
        instance
            .lifecycle
            .start(super::super::fixtures::deadline())
            .await
            .unwrap();
        let result = instance
            .roles
            .stream
            .as_ref()
            .unwrap()
            .connect(
                ConnectRequest {
                    destination: destination(),
                },
                context(address, observations.clone()),
            )
            .await;
        let error = match result {
            Ok(_) => panic!("rejected upgrade must fail real flush"),
            Err(error) => error,
        };
        assert_eq!(error.reason, zero_platform_tokio::contract::Reason::Io);
        assert!(std::error::Error::source(&error).is_some());
        assert!(observations.facts.lock().unwrap().is_empty());
        assert!(observations.protocol.lock().unwrap().is_none());
        server.await.unwrap();
        instance
            .lifecycle
            .shutdown(super::super::fixtures::deadline())
            .await
            .unwrap();
        assert!(scope
            .shutdown(super::super::fixtures::deadline())
            .await
            .is_complete());
    })
    .await
    .unwrap();
}
