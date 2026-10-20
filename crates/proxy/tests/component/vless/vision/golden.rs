use super::super::fixtures::{
    acceptor_with_alpn, context, destination, instance_with_flow, tls_profile, Observations,
};
use super::fixtures::{frame, request, tls12_acceptor};
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{timeout, Duration},
};
use zero_platform_tokio::contract::{
    context::ConnectFact,
    stream::{ConnectRequest, PeerConfirmation},
    DeliveryState, Reason,
};
#[tokio::test]
async fn native_vision_uses_exact_original_flow_addons_and_deterministic_padding_over_real_tls() {
    for flow in ["xtls-rprx-vision", "xtls-rprx-vision-udp443"] {
        timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let acceptor = acceptor_with_alpn(false);
            let server = tokio::spawn(async move {
                let (io, _) = listener.accept().await.unwrap();
                let mut io = acceptor.accept(io).await.unwrap();
                let expected = request();
                let mut header = vec![0; expected.len()];
                io.read_exact(&mut header).await.unwrap();
                assert_eq!(header, expected);
                let mut body = Vec::new();
                io.read_to_end(&mut body).await.unwrap();
                let hello = [0x16, 3, 1, 0, 2, 1, 0];
                assert_eq!(body, frame(true, 0, &hello, 13));
                let mut reply = vec![0, 0];
                reply.extend(frame(true, 1, b"after-half-close", 3));
                io.write_all(&reply).await.unwrap();
                io.shutdown().await.unwrap();
            });
            let tls = tls_profile();
            let observations = Arc::new(Observations::default());
            let (mut instance, mut scope) =
                instance_with_flow(address, Some(&tls), None, None, Some(flow), &[10, 1, 20, 1]);
            instance
                .lifecycle
                .start(super::super::super::fixtures::deadline())
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
            assert_eq!(*observations.protocol.lock().unwrap(), Some((0, 55)));
            connected
                .io
                .write_all(&[0x16, 3, 1, 0, 2, 1, 0])
                .await
                .unwrap();
            connected.io.shutdown().await.unwrap();
            let mut reply = Vec::new();
            connected.io.read_to_end(&mut reply).await.unwrap();
            assert_eq!(reply, b"after-half-close");
            assert_eq!(
                observations.facts.lock().unwrap().last(),
                Some(&(None, ConnectFact::PeerConfirmed))
            );
            server.await.unwrap();
            instance
                .lifecycle
                .shutdown(super::super::super::fixtures::deadline())
                .await
                .unwrap();
            assert!(scope
                .shutdown(super::super::super::fixtures::deadline())
                .await
                .is_complete());
        })
        .await
        .unwrap();
    }
}
#[tokio::test]
async fn actual_tls12_cannot_be_presented_as_a_switchable_vision_carrier() {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let acceptor = tls12_acceptor();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let mut io = acceptor.accept(io).await.unwrap();
            assert_eq!(
                io.get_ref().1.protocol_version(),
                Some(rustls::ProtocolVersion::TLSv1_2)
            );
            let mut bytes = Vec::new();
            let _ = io.read_to_end(&mut bytes).await;
            assert!(
                bytes.is_empty(),
                "no VLESS request may be submitted on TLS1.2 Vision"
            );
        });
        let tls = tls_profile();
        let observations = Arc::new(Observations::default());
        let (mut instance, mut scope) = instance_with_flow(
            address,
            Some(&tls),
            None,
            None,
            Some("xtls-rprx-vision"),
            &[],
        );
        instance
            .lifecycle
            .start(super::super::super::fixtures::deadline())
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
            Ok(_) => panic!("TLS1.2 must not authorize a raw transition"),
            Err(error) => error,
        };
        assert_eq!(error.reason, Reason::Unsupported);
        assert_eq!(error.delivery, DeliveryState::NotSubmitted);
        assert!(std::error::Error::source(&error).is_some());
        assert!(observations.facts.lock().unwrap().is_empty());
        server.await.unwrap();
        instance
            .lifecycle
            .shutdown(super::super::super::fixtures::deadline())
            .await
            .unwrap();
        assert!(scope
            .shutdown(super::super::super::fixtures::deadline())
            .await
            .is_complete());
    })
    .await
    .unwrap();
}
