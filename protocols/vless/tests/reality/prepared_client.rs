use super::*;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use zero_platform_tokio::contract::{
    admission::SessionOwner, lifecycle::ResourceLifecycle, scope::TaskScope, CancellationToken,
};

#[tokio::test]
async fn verified_decoy_is_rejected_while_real_scoped_navigation_is_cancelled_and_joined() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let cert = rcgen::generate_simple_self_signed(vec!["decoy.test".into()]).unwrap();
        let trust =
            ztls::certificate::ServerTrust::certificates(&[cert.cert.der().to_vec()]).unwrap();
        let server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (peer, _) = listener.accept().await.unwrap();
        let (ready, pending) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut io = tokio_rustls::TlsAcceptor::from(Arc::new(server_config))
                .accept(peer)
                .await
                .unwrap();
            let mut preface = [0; 24];
            io.read_exact(&mut preface).await.unwrap();
            assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
            // The actual H2 handshake is now waiting for this peer's settings.
            ready.send(()).unwrap();
            let mut wire = Vec::new();
            if let Err(error) = io.get_mut().0.read_to_end(&mut wire).await {
                // Cancelling the pending H2 client closes this actual carrier.
                // With unread TLS records the peer may observe an OS reset
                // rather than EOF; neither outcome substitutes for Scope join.
                assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
            }
        });
        let prepared = PreparedRealityClient {
            config: RealityClientConfig {
                public_key: *PublicKey::from(&StaticSecret::from([21; 32])).as_bytes(),
                // An explicit real test certificate root, never insecure trust.
                decoy_trust: Some(trust),
                server_name: "decoy.test".into(),
                hybrid_key_exchange: false,
                ..Default::default()
            },
            spider: crate::reality_spider::Profile::parse("/").unwrap(),
        };
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(64).unwrap(),
        );
        let parent = CancellationToken::new();
        let (mut owner, admission) =
            SessionOwner::new(zero_core::component::ComponentId(91), &parent);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        owner.start(deadline).await.unwrap();
        let registrar = scope.registrar();
        let result = prepared
            .connect_with_navigation(client, move |stream, host, plan| {
                crate::component::navigation::start(stream, host, plan, registrar, admission)
            })
            .await;
        assert!(
            matches!(result, Err(ref error) if error.kind() == io::ErrorKind::PermissionDenied)
        );
        pending.await.unwrap();
        // A successful caller rejection neither detaches nor closes this job.
        // Scope cancellation must stop and join the real pending H2 operation.
        let report = scope.shutdown(deadline).await;
        assert!(report.is_complete());
        assert!(report.failed_tasks.is_empty());
        assert_eq!(report.exited_tasks.len(), 1);
        server.await.unwrap(); // Actual original TCP carrier reached EOF or reset.
        owner.shutdown(deadline).await.unwrap();
    })
    .await
    .unwrap();
}

#[test]
fn cold_profile_rejects_invalid_keys_short_ids_ciphers_and_spider_without_opening_carrier() {
    let (_, public) = generate_reality_key_pair();
    for (key, short, cipher, spider) in [
        ("invalid!", "00", "", "/"),
        (public.as_str(), "001122334455667788", "", "/"),
        (public.as_str(), "00", "TLS_FAKE", "/"),
        (public.as_str(), "00", "", "//external.test"),
    ] {
        let ciphers = if cipher.is_empty() {
            vec![]
        } else {
            vec![cipher.into()]
        };
        assert!(PreparedRealityClient::from_options(RealityClientOptions {
            public_key: key,
            short_id: short,
            cipher_suites: &ciphers,
            spider_x: spider,
            hybrid_key_exchange: false,
            mldsa65_verify: None,
            server_name: "decoy.test",
            client_fingerprint: "chrome",
        })
        .is_err());
    }
}
