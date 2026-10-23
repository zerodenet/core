use super::fixtures::*;
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{timeout, Duration},
};
use zero_platform_tokio::contract::{
    context::ConnectFact,
    stream::{BoxStream, ConnectRequest, PeerConfirmation},
};

#[tokio::test]
async fn actual_native_vless_tcp_and_tls_keep_wire_destination_half_close_and_deferred_addon_validation(
) {
    for secure in [false, true] {
        timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let acceptor = secure.then(|| acceptor_with_alpn(false));
            let server = tokio::spawn(async move {
                let (io, _) = listener.accept().await.unwrap();
                let mut io: BoxStream = match acceptor {
                    Some(acceptor) => Box::pin(acceptor.accept(io).await.unwrap()),
                    None => Box::pin(io),
                };
                let expected = request();
                let mut header = vec![0; expected.len()];
                io.read_exact(&mut header).await.unwrap();
                assert_eq!(header, expected);
                let mut body = Vec::new();
                io.read_to_end(&mut body).await.unwrap();
                assert_eq!(body, b"payload");
                io.write_all(b"\x00\x02\xaa\xbbafter-half-close")
                    .await
                    .unwrap();
                io.shutdown().await.unwrap();
            });
            let tls = secure.then(tls_profile);
            let observations = Arc::new(Observations::default());
            let (mut instance, mut scope) = instance(address, tls.as_ref(), None, None);
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
async fn actual_invalid_or_truncated_vless_response_never_confirms_the_peer() {
    for (reply, kind, rejected) in [
        (vec![1, 0], std::io::ErrorKind::InvalidData, true),
        (vec![0, 3, 0xaa], std::io::ErrorKind::UnexpectedEof, false),
    ] {
        timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut io, _) = listener.accept().await.unwrap();
                let mut header = vec![0; REQUEST_LEN as usize];
                io.read_exact(&mut header).await.unwrap();
                assert_eq!(header, request());
                io.write_all(&reply).await.unwrap();
                io.shutdown().await.unwrap();
            });
            let observations = Arc::new(Observations::default());
            let (mut instance, mut scope) = instance(address, None, None, None);
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
            assert_eq!(connected.io.read_u8().await.unwrap_err().kind(), kind);
            {
                let facts = observations.facts.lock().unwrap();
                assert!(!facts
                    .iter()
                    .any(|(_, fact)| *fact == ConnectFact::PeerConfirmed));
                assert_eq!(
                    facts
                        .iter()
                        .any(|(_, fact)| *fact == ConnectFact::PeerRejected),
                    rejected
                );
            }

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
