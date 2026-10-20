use super::super::fixtures::{context, destination, instance_with_reality, Observations};
use super::fixtures::{profile, server};
use std::sync::Arc;
use tokio::{
    io::AsyncReadExt,
    net::TcpListener,
    time::{timeout, Duration},
};
use zero_platform_tokio::contract::{stream::ConnectRequest, DeliveryState, Reason, Stage};

#[tokio::test]
async fn actual_reality_factory_rejects_wrong_public_key_short_id_name_and_authenticated_version_policy(
) {
    timeout(Duration::from_secs(8), async {
        for invalid in ["public-key", "short-id", "name", "version-policy"] {
            let (private, public) = vless::reality::generate_reality_key_pair();
            let (_, wrong_public) = vless::reality::generate_reality_key_pair();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let mut policy = server(&private);
            if invalid == "version-policy" {
                policy = policy
                    .with_policy(vless::reality_policy::PolicyRef {
                        max_client_version: Some("26.3.26"),
                        ..Default::default()
                    })
                    .unwrap();
            }
            let peer = tokio::spawn(async move {
                let (io, _) = listener.accept().await.unwrap();
                // No authenticated stream is returned, hence no VLESS request
                // may reach a server application through this connection.
                assert!(policy.upgrade_server(io).await.is_err());
            });
            let key = if invalid == "public-key" {
                &wrong_public
            } else {
                &public
            };
            let short = if invalid == "short-id" {
                "00"
            } else {
                "0123456789abcdef"
            };
            let name = if invalid == "name" {
                "other.test"
            } else {
                "localhost"
            };
            let observations = Arc::new(Observations::default());
            let (mut instance, mut scope) =
                instance_with_reality(address, profile(key, short, name), None);
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
                Ok(_) => panic!("REALITY admitted {invalid}"),
                Err(error) => error,
            };
            assert_eq!(error.stage, Stage::Handshake);
            assert_eq!(error.reason, Reason::Io);
            assert_eq!(error.delivery, DeliveryState::NotSubmitted);
            assert!(std::error::Error::source(&error).is_some());
            assert!(observations.facts.lock().unwrap().is_empty());
            assert!(observations.protocol.lock().unwrap().is_none());
            peer.await.unwrap();
            instance
                .lifecycle
                .shutdown(super::super::super::fixtures::deadline())
                .await
                .unwrap();
            assert!(scope
                .shutdown(super::super::super::fixtures::deadline())
                .await
                .is_complete());
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelling_pending_real_reality_handshakes_closes_carrier_and_next_open_has_fresh_client_hello(
) {
    timeout(Duration::from_secs(5), async {
        let (_, public) = vless::reality::generate_reality_key_pair();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (ready, mut wire) = tokio::sync::mpsc::channel(1);
        let peer = tokio::spawn(async move {
            let mut hellos = Vec::new();
            for _ in 0..2 {
                let (mut io, _) = listener.accept().await.unwrap();
                let mut header = [0; 5];
                io.read_exact(&mut header).await.unwrap();
                assert_eq!(header[0], 22); // Actual TLS handshake record.
                let mut hello = vec![0; u16::from_be_bytes([header[3], header[4]]) as usize];
                io.read_exact(&mut hello).await.unwrap();
                assert_eq!(hello[0], 1); // Actual ClientHello, not VLESS data.
                hellos.push(hello);
                ready.send(()).await.unwrap();
                let mut tail = Vec::new();
                io.read_to_end(&mut tail).await.unwrap();
                assert!(tail.is_empty(), "no response, replay or proxy request after cancellation");
            }
            assert_ne!(&hellos[0][6..38], &hellos[1][6..38], "ClientHello random must be generated per actual open");
        });
        let observations = Arc::new(Observations::default());
        let (mut instance, mut scope) = instance_with_reality(address, profile(&public, "0123456789abcdef", "localhost"), None);
        instance.lifecycle.start(super::super::super::fixtures::deadline()).await.unwrap();
        for _ in 0..2 {
            let ctx = context(address, observations.clone());
            let cancel = ctx.cancel.clone();
            let connect = instance.roles.stream.as_ref().unwrap().connect(ConnectRequest { destination: destination() }, ctx);
            tokio::pin!(connect);
            tokio::select! {
                result = &mut connect => panic!("handshake finished without a ServerHello: {}", result.is_ok()),
                result = wire.recv() => { result.unwrap(); }
            }
            cancel.cancel();
            let error = match connect.await { Ok(_) => panic!("cancelled handshake admitted"), Err(error) => error };
            assert_eq!(error.stage, Stage::Handshake);
            assert_eq!(error.reason, Reason::Cancelled);
            assert!(observations.facts.lock().unwrap().is_empty());
        }
        peer.await.unwrap(); // Both actual sockets reached EOF, with no replay.
        instance.lifecycle.shutdown(super::super::super::fixtures::deadline()).await.unwrap();
        assert!(scope.shutdown(super::super::super::fixtures::deadline()).await.is_complete());
    }).await.unwrap();
}
