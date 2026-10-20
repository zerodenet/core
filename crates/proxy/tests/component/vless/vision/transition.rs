use super::super::fixtures::{
    acceptor_with_alpn, context, destination, instance_with_flow, tls_profile, Observations,
};
use super::fixtures::{inner_tls, request, uuid};
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
use zero_traits::AsyncSocket;
#[tokio::test]
async fn actual_nested_authenticated_tls_drives_both_real_vision_bypass_directions_and_half_close()
{
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let outer = zero_transport::tls::TlsAcceptor::Rustls(acceptor_with_alpn(false));
        let (inner, client_config) = inner_tls();
        let payload: Vec<u8> = (0..65_537).map(|index| (index % 251) as u8).collect();
        let expected = payload.clone();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let mut tls = zero_transport::tls::accept_tls_handshake(&outer, io)
                .await
                .unwrap();
            let control = tls
                .transport_bypass_control()
                .expect("actual outer TLS1.3 switchable control");
            let expected_header = request();
            let mut header = vec![0; expected_header.len()];
            AsyncReadExt::read_exact(&mut tls, &mut header)
                .await
                .unwrap();
            assert_eq!(header, expected_header);
            AsyncWriteExt::write_all(&mut tls, &[0, 0]).await.unwrap();
            AsyncWriteExt::flush(&mut tls).await.unwrap();
            let vision = vless::vision::VisionStream::new(tls, uuid(), Some(control.clone()));
            let mut io = inner.accept(vision).await.unwrap();
            let mut received = Vec::new();
            io.read_to_end(&mut received).await.unwrap();
            assert_eq!(received, expected);
            io.write_all(&received).await.unwrap();
            io.shutdown().await.unwrap();
            assert!(
                control.read_bypass_requested(),
                "real inner client TLS records must cross the actual raw read handoff"
            );
            assert!(
                control.write_bypass_requested(),
                "real inner server TLS records must cross the actual raw write handoff"
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
        let connector = tokio_rustls::TlsConnector::from(client_config);
        let mut io = connector
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
