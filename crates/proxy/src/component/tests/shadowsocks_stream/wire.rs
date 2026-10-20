use super::fixtures::*;
use shadowsocks::transport::ShadowsocksTransportState;
use std::sync::{atomic::Ordering, Arc};
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
async fn actual_native_shadowsocks_plain_legacy_and_2022_keep_wire_traffic_halfclose_and_real_confirmation(
) {
    for (cipher, password, legacy_size) in [
        ("none", PASSWORD, Some(18)),
        ("aes-128-gcm", PASSWORD, Some(68)),
        ("2022-blake3-aes-128-gcm", KEY2022, None),
    ] {
        timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (received, accepted) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let (io, _) = listener.accept().await.unwrap();
                let mut metered = zero_transport::MeteredStream::new(
                    zero_platform_tokio::TcpRelayStream::new(io),
                );
                let profile =
                    shadowsocks::ShadowsocksInboundProfile::from_config(cipher, password).unwrap();
                let (accept, user) = profile
                    .accept_request(&shadowsocks::ShadowsocksInbound, &mut metered)
                    .await
                    .unwrap();
                assert_eq!(accept.session.target, destination().address);
                assert_eq!(accept.session.port, destination().port);
                received.send(metered.drain_traffic().read_bytes).unwrap();
                let mut io = profile
                    .into_aead_stream(accept, &user, metered.into_inner())
                    .unwrap();
                let mut payload = Vec::new();
                io.read_to_end(&mut payload).await.unwrap();
                assert_eq!(payload, b"real-half-close");
                io.write_all(&payload).await.unwrap();
                io.shutdown().await.unwrap();
            });
            let state = ShadowsocksTransportState::default();
            let assembly = assembly(&state, address, cipher, password, 1, None);
            assembly.start().await;
            let observations = Arc::new(Observations::default());
            let role = assembly.streams[0].stream(deadline()).await.unwrap();
            let mut connected = role
                .connect(
                    ConnectRequest {
                        destination: destination(),
                    },
                    context(address, observations.clone()),
                )
                .await
                .unwrap();
            assert_eq!(connected.peer_confirmation, PeerConfirmation::Deferred);
            let bytes = accepted.await.unwrap();
            if let Some(expected) = legacy_size {
                assert_eq!(bytes, expected);
            }
            assert_eq!(*observations.protocol.lock().unwrap(), Some((0, bytes)));
            assert_eq!(observations.outer.load(Ordering::Relaxed), bytes);
            assert_eq!(
                *observations.facts.lock().unwrap(),
                vec![
                    (None, ConnectFact::CarrierReady),
                    (None, ConnectFact::RequestSubmitted)
                ]
            );
            connected.io.write_all(b"real-half-close").await.unwrap();
            connected.io.shutdown().await.unwrap();
            let mut echo = Vec::new();
            connected.io.read_to_end(&mut echo).await.unwrap();
            assert_eq!(echo, b"real-half-close");
            let confirmed = observations
                .facts
                .lock()
                .unwrap()
                .iter()
                .any(|(_, fact)| *fact == ConnectFact::PeerConfirmed);
            assert_eq!(confirmed, cipher.starts_with("2022-"));
            server.await.unwrap();
            drop(connected);
            assembly.close().await;
            let revoked = role
                .connect(
                    ConnectRequest {
                        destination: destination(),
                    },
                    context(address, observations.clone()),
                )
                .await;
            assert!(revoked.is_err());
            assert_eq!(observations.dials.load(Ordering::Relaxed), 1);
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn two_canonical_native_consumers_reject_the_second_authenticated_2022_response_salt_from_the_same_history(
) {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (io, _) = listener.accept().await.unwrap();
                let mut io = zero_platform_tokio::TcpRelayStream::new(io);
                let profile = shadowsocks::ShadowsocksInboundProfile::from_config(
                    "2022-blake3-aes-128-gcm",
                    KEY2022,
                )
                .unwrap();
                let (accept, _) = profile
                    .accept_request(&shadowsocks::ShadowsocksInbound, &mut io)
                    .await
                    .unwrap();
                // Deliberately malicious salt reuse, with a real freshly
                // authenticated header echoing each actual request salt.
                let mut io = accept
                    .into_aead_stream_with_response_salt(io, KEY2022.as_bytes(), vec![7; 16])
                    .unwrap();
                let mut payload = Vec::new();
                io.read_to_end(&mut payload).await.unwrap();
                assert_eq!(payload, b"payload");
                io.write_all(b"response").await.unwrap();
                io.shutdown().await.unwrap();
            }
        });
        let state = ShadowsocksTransportState::default();
        let assembly = assembly(&state, address, "2022-blake3-aes-128-gcm", KEY2022, 2, None);
        assembly.start().await;
        assert_eq!(
            assembly.streams[0].dependency_instances(),
            assembly.streams[1].dependency_instances()
        );
        for index in 0..2 {
            let observations = Arc::new(Observations::default());
            let role = assembly.streams[index].stream(deadline()).await.unwrap();
            let mut connected = role
                .connect(
                    ConnectRequest {
                        destination: destination(),
                    },
                    context(address, observations.clone()),
                )
                .await
                .unwrap();
            connected.io.write_all(b"payload").await.unwrap();
            connected.io.shutdown().await.unwrap();
            let mut response = Vec::new();
            let result = connected.io.read_to_end(&mut response).await;
            if index == 0 {
                result.unwrap();
                assert_eq!(response, b"response");
                assert_eq!(
                    observations.facts.lock().unwrap().last(),
                    Some(&(None, ConnectFact::PeerConfirmed))
                );
            } else {
                let error = result.unwrap_err();
                assert!(error.to_string().contains("replay salt rejected"));
                assert!(std::error::Error::source(&error).is_some());
                assert_eq!(
                    observations.facts.lock().unwrap().last(),
                    Some(&(None, ConnectFact::PeerRejected))
                );
                assert!(response.is_empty());
            }
        }
        server.await.unwrap();
        assembly.close().await;
    })
    .await
    .unwrap();
}
