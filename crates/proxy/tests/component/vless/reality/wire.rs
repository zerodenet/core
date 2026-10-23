use super::super::fixtures::{
    context, destination, instance_with_reality, request, Observations, REQUEST_LEN,
};
use super::fixtures::{profile, server};
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
async fn authenticated_reality_role_preserves_destination_deferred_confirmation_and_half_close() {
    timeout(Duration::from_secs(5), async {
        let (private, public) = vless::reality::generate_reality_key_pair();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let mut io = server(&private).upgrade_server(io).await.unwrap();
            let mut header = vec![0; REQUEST_LEN as usize];
            io.read_exact(&mut header).await.unwrap();
            assert_eq!(header, request());
            io.write_all(&[0, 0]).await.unwrap();
            let mut body = Vec::new();
            io.read_to_end(&mut body).await.unwrap();
            assert_eq!(body, b"real-half-close");
            io.write_all(&body).await.unwrap();
            io.shutdown().await.unwrap();
        });
        let observations = Arc::new(Observations::default());
        let (mut instance, mut scope) = instance_with_reality(
            address,
            profile(&public, "0123456789abcdef", "localhost"),
            None,
        );
        instance
            .lifecycle
            .start(super::super::super::fixtures::deadline())
            .await
            .unwrap();
        let connected = instance
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
            observations.facts.lock().unwrap().as_slice(),
            &[
                (None, ConnectFact::CarrierReady),
                (None, ConnectFact::RequestSubmitted)
            ]
        );
        let mut io = connected.io;
        io.write_all(b"real-half-close").await.unwrap();
        io.shutdown().await.unwrap();
        let mut body = Vec::new();
        io.read_to_end(&mut body).await.unwrap();
        assert_eq!(body, b"real-half-close");
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

#[tokio::test]
async fn authenticated_reality_vision_reaches_actual_bypass_in_both_directions() {
    timeout(Duration::from_secs(5), async {
        let (private, public) = vless::reality::generate_reality_key_pair();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (inner, client_config) = super::super::vision::fixtures::inner_tls();
        let payload: Vec<u8> = (0..65_537).map(|i| (i % 251) as u8).collect();
        let expected = payload.clone();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let mut io = server(&private).upgrade_server(io).await.unwrap();
            let control = io.transport_bypass_control();
            let expected_header = super::super::vision::fixtures::request();
            let mut header = vec![0; expected_header.len()];
            io.read_exact(&mut header).await.unwrap();
            assert_eq!(header, expected_header);
            io.write_all(&[0, 0]).await.unwrap();
            io.flush().await.unwrap();
            let vision = vless::vision::VisionStream::new(
                io,
                super::super::vision::fixtures::uuid(),
                Some(control.clone()),
            );
            let mut io = inner.accept(vision).await.unwrap();
            let mut body = Vec::new();
            io.read_to_end(&mut body).await.unwrap();
            assert_eq!(body, expected);
            io.write_all(&body).await.unwrap();
            io.shutdown().await.unwrap();
            assert!(control.read_bypass_requested());
            assert!(control.write_bypass_requested());
        });
        let observations = Arc::new(Observations::default());
        let (mut instance, mut scope) = instance_with_reality(
            address,
            profile(&public, "0123456789abcdef", "localhost"),
            Some("xtls-rprx-vision"),
        );
        instance
            .lifecycle
            .start(super::super::super::fixtures::deadline())
            .await
            .unwrap();
        let connected = instance
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
        let mut io = tokio_rustls::TlsConnector::from(client_config)
            .connect(
                rustls::pki_types::ServerName::try_from("localhost")
                    .unwrap()
                    .to_owned(),
                connected.io,
            )
            .await
            .unwrap();
        io.write_all(&payload).await.unwrap();
        io.shutdown().await.unwrap();
        let mut echo = Vec::new();
        io.read_to_end(&mut echo).await.unwrap();
        assert_eq!(echo, payload);
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
