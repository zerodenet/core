//! Actual certificate verification/ALPN on both preserved TLS backends.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_traits::TlsBackend;
fn profile(backend: TlsBackend) -> crate::profile::OwnedClientTlsProfile {
    crate::profile::OwnedClientTlsProfile {
        server_name: Some("localhost".into()),
        insecure: false,
        disable_sni: false,
        ca_cert_path: Some("not-opened-owned/ca.pem".into()),
        alpn: vec!["h2".into()],
        client_fingerprint: None,
        options: zero_traits::ClientTlsOptions {
            backend,
            disable_system_roots: true,
            ..Default::default()
        },
    }
}
#[tokio::test]
async fn granted_ca_material_preserves_each_real_backend_and_native_authenticated_alpn() {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        for backend in [TlsBackend::Rustls, TlsBackend::OpenSsl] {
            let mut server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![certificate.cert.der().clone()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(
                    certificate.signing_key.serialize_der(),
                )
                .into(),
            )
            .unwrap();
            server_config.alpn_protocols = vec![b"h2".to_vec()];
            let policy = profile(backend);
            let prepared = OwnedPreparedClient::from_material(
                &policy,
                None,
                "ignored.invalid",
                Some(certificate.cert.pem().as_bytes()),
            )
            .unwrap();
            if backend == TlsBackend::OpenSsl {
                assert_eq!(
                    PreparedClient::validate_profile(&policy)
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::Unsupported
                );
            }
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let peer = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (server, _) = listener.accept().await.unwrap();
            let server = tokio::spawn(async move {
                let mut io = tokio_rustls::TlsAcceptor::from(Arc::new(server_config))
                    .accept(server)
                    .await
                    .unwrap();
                assert_eq!(io.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));
                let mut bytes = [0; 4];
                io.read_exact(&mut bytes).await.unwrap();
                assert_eq!(bytes, *b"real");
                io.write_all(&bytes).await.unwrap();
                io.flush().await.unwrap();
                let mut end = vec![];
                io.read_to_end(&mut end).await.unwrap();
                assert!(end.is_empty());
                io.shutdown().await.unwrap();
            });
            let mut io = prepared.connect(peer).await.unwrap();
            io.write_all(b"real").await.unwrap();
            io.flush().await.unwrap();
            let mut bytes = [0; 4];
            io.read_exact(&mut bytes).await.unwrap();
            assert_eq!(bytes, *b"real");
            io.shutdown().await.unwrap();
            let mut end = vec![];
            io.read_to_end(&mut end).await.unwrap();
            assert!(end.is_empty());
            server.await.unwrap();
        }
    })
    .await
    .unwrap();
}
#[test]
fn owning_material_requires_the_actual_ca_declaration_and_never_reopens_its_path() {
    for backend in [TlsBackend::Rustls, TlsBackend::OpenSsl] {
        let policy = profile(backend);
        assert!(OwnedPreparedClient::from_material(&policy, None, "localhost", None).is_err());
        let error = OwnedPreparedClient::from_material(
            &policy,
            None,
            "localhost",
            Some(b"not a CA certificate"),
        )
        .err()
        .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        let mut no_file = policy;
        no_file.ca_cert_path = None;
        assert!(OwnedPreparedClient::from_material(
            &no_file,
            None,
            "localhost",
            Some(b"bytes without a File declaration")
        )
        .is_err());
    }
}

#[path = "owned_material/authentication.rs"]
mod authentication;

#[path = "owned_material/half_close.rs"]
mod half_close;
