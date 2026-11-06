//! Same path/new CA bytes isolate the existing backend resumption contexts.
use super::*;
#[tokio::test]
async fn each_backend_refuses_the_previous_authority_at_the_same_declared_path() {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        let old = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let current = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        for backend in [TlsBackend::Rustls, TlsBackend::OpenSsl] {
            let mut policy = profile(backend);
            policy.options.parameters.enable_session_resumption = true;
            let previous = OwnedPreparedClient::from_material(
                &policy,
                None,
                "localhost",
                Some(old.cert.pem().as_bytes()),
            )
            .unwrap();
            let replacement = OwnedPreparedClient::from_material(
                &policy,
                None,
                "localhost",
                Some(current.cert.pem().as_bytes()),
            )
            .unwrap();
            for (prepared, accept) in [(replacement, true), (previous, false)] {
                let config = rustls::ServerConfig::builder_with_provider(Arc::new(
                    rustls::crypto::ring::default_provider(),
                ))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(
                    vec![current.cert.der().clone()],
                    rustls::pki_types::PrivatePkcs8KeyDer::from(
                        current.signing_key.serialize_der(),
                    )
                    .into(),
                )
                .unwrap();
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
                    .await
                    .unwrap();
                let (server, _) = listener.accept().await.unwrap();
                let server = tokio::spawn(async move {
                    let result = tokio_rustls::TlsAcceptor::from(Arc::new(config))
                        .accept(server)
                        .await;
                    if accept {
                        let mut io = result.unwrap();
                        let mut bytes = vec![];
                        io.read_to_end(&mut bytes).await.unwrap();
                        assert!(bytes.is_empty());
                        io.shutdown().await.unwrap();
                    } else {
                        assert!(result.is_err());
                    }
                });
                let result = prepared.connect(client).await;
                if accept {
                    let mut io = result.unwrap();
                    io.shutdown().await.unwrap();
                    let mut bytes = vec![];
                    io.read_to_end(&mut bytes).await.unwrap();
                    assert!(bytes.is_empty());
                } else {
                    assert!(result.is_err());
                }
                server.await.unwrap();
            }
        }
    })
    .await
    .unwrap();
}
