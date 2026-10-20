use super::fixtures::*;
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{timeout, Duration},
};
use zero_platform_tokio::contract::{
    context::ConnectFact,
    stream::{ConnectRequest, PeerConfirmation},
};

#[tokio::test]
async fn real_native_vmess_all_ciphers_preserve_destination_half_close_and_authenticate_before_confirmation(
) {
    for cipher in [
        "aes-128-gcm",
        "chacha20-poly1305",
        "none",
        "zero",
        "zero-plus",
    ] {
        timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let profile = profile(cipher);
            let server = tokio::spawn(async move {
                let (io, _) = listener.accept().await.unwrap();
                let (facts, mut io) = profile
                    .accept_tcp_stream(
                        vmess::inbound::VmessInbound,
                        zero_platform_tokio::TcpRelayStream::new(io),
                    )
                    .await
                    .unwrap();
                assert_eq!(facts.target, destination().address);
                assert_eq!(facts.port, destination().port);
                let mut body = Vec::new();
                io.read_to_end(&mut body).await.unwrap();
                assert_eq!(body, b"payload");
                io.write_all(b"after-half-close").await.unwrap();
                io.shutdown().await.unwrap();
            });
            let observations = Arc::new(Observations::default());
            let (mut instance, mut scope) = instance(address, cipher, None, None, None);
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
                Some((0, REQUEST_LEN))
            );
            assert_eq!(
                *observations.facts.lock().unwrap(),
                vec![
                    (None, ConnectFact::CarrierReady),
                    (None, ConnectFact::RequestSubmitted)
                ]
            );
            connected.io.write_all(b"payload").await.unwrap();
            connected.io.shutdown().await.unwrap();
            let mut reply = Vec::new();
            connected.io.read_to_end(&mut reply).await.unwrap();
            assert_eq!(reply, b"after-half-close");
            assert_eq!(
                *observations.facts.lock().unwrap(),
                vec![
                    (None, ConnectFact::CarrierReady),
                    (None, ConnectFact::RequestSubmitted),
                    (None, ConnectFact::PeerConfirmed)
                ]
            );
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
async fn corrupt_real_vmess_response_remains_deferred_until_read_and_reports_peer_rejection_with_source(
) {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut io, _) = listener.accept().await.unwrap();
            let mut request = vec![0; REQUEST_LEN as usize];
            io.read_exact(&mut request).await.unwrap();
            io.write_all(&[0; 18]).await.unwrap();
            io.flush().await.unwrap();
            let mut rest = Vec::new();
            io.read_to_end(&mut rest).await.unwrap();
        });
        let observations = Arc::new(Observations::default());
        let (mut instance, mut scope) = instance(address, "aes-128-gcm", None, None, None);
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
        let error = connected.io.read_u8().await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.get_ref().is_some());
        assert_eq!(
            *observations.facts.lock().unwrap(),
            vec![
                (None, ConnectFact::CarrierReady),
                (None, ConnectFact::RequestSubmitted),
                (None, ConnectFact::PeerRejected)
            ]
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
