use super::*;
use crate::profile::OwnedClientTlsProfile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn profile() -> OwnedClientTlsProfile {
    let options = zero_traits::ClientTlsOptions {
        disable_system_roots: true,
        ..Default::default()
    };
    OwnedClientTlsProfile {
        options,
        server_name: Some("localhost".into()),
        disable_sni: false,
        ca_cert_path: Some("not-opened/authority.pem".into()),
        insecure: false,
        alpn: vec!["h2".into()],
        client_fingerprint: None,
    }
}

#[tokio::test]
async fn authorized_pem_builds_real_authenticated_tls_without_opening_declared_path() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
        )
        .unwrap();
        config.alpn_protocols = vec![b"h2".to_vec()];
        let prepared =
            PreparedClient::from_ca_pem(&profile(), None, cert.cert.pem().as_bytes()).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let server = tokio::spawn(async move {
            let mut io = tokio_rustls::TlsAcceptor::from(Arc::new(config))
                .accept(server)
                .await
                .unwrap();
            assert_eq!(io.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));
            let mut body = Vec::new();
            io.read_to_end(&mut body).await.unwrap();
            assert_eq!(body, b"authorized-root");
            io.write_all(&body).await.unwrap();
            io.shutdown().await.unwrap();
        });
        let mut io = prepared.connect(peer, "ignored.invalid").await.unwrap();
        io.write_all(b"authorized-root").await.unwrap();
        io.shutdown().await.unwrap();
        let mut echo = Vec::new();
        io.read_to_end(&mut echo).await.unwrap();
        assert_eq!(echo, b"authorized-root");
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[test]
fn pem_diagnostics_keep_base_directory_and_original_parse_source() {
    let profile = profile();
    let empty =
        match PreparedClient::from_ca_pem(&profile, Some(Path::new("/configured/base")), b"") {
            Ok(_) => panic!("empty authority accepted"),
            Err(error) => error,
        };
    assert_eq!(empty.kind(), io::ErrorKind::InvalidInput);
    assert!(empty
        .to_string()
        .contains("/configured/base/not-opened/authority.pem"));
    let malformed = match PreparedClient::from_ca_pem(
        &profile,
        None,
        b"-----BEGIN CERTIFICATE-----\ninvalid!\n-----END CERTIFICATE-----\n",
    ) {
        Ok(_) => panic!("malformed PEM accepted"),
        Err(error) => error,
    };
    assert!(malformed.get_ref().unwrap().is::<io::Error>());
}

#[test]
fn prepared_material_does_not_silently_replace_an_openssl_profile() {
    let mut profile = profile();
    profile.options.backend = zero_traits::TlsBackend::OpenSsl;
    assert_eq!(
        PreparedClient::validate_profile(&profile)
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
}

#[tokio::test]
async fn same_declared_path_with_new_authorized_roots_never_reuses_the_previous_verifier() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let old = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let replacement = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let mut policy = profile();
        policy.options.parameters.enable_session_resumption = true;
        let previous =
            PreparedClient::from_ca_pem(&policy, None, old.cert.pem().as_bytes()).unwrap();
        let current =
            PreparedClient::from_ca_pem(&policy, None, replacement.cert.pem().as_bytes()).unwrap();
        for (prepared, accepted) in [(current, true), (previous, false)] {
            let config = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![replacement.cert.der().clone()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(
                    replacement.signing_key.serialize_der(),
                )
                .into(),
            )
            .unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (peer, _) = listener.accept().await.unwrap();
            let server = tokio::spawn(async move {
                let result = tokio_rustls::TlsAcceptor::from(Arc::new(config))
                    .accept(peer)
                    .await;
                if accepted {
                    let mut io = result.unwrap();
                    let mut body = Vec::new();
                    io.read_to_end(&mut body).await.unwrap();
                    assert!(body.is_empty());
                    io.shutdown().await.unwrap();
                } else {
                    assert!(result.is_err());
                }
            });
            let result = prepared.connect(client, "ignored.invalid").await;
            if accepted {
                let mut io = result.unwrap();
                io.shutdown().await.unwrap();
                let mut body = Vec::new();
                io.read_to_end(&mut body).await.unwrap();
                assert!(body.is_empty());
            } else {
                assert!(result.is_err());
            }
            server.await.unwrap();
        }
    })
    .await
    .unwrap();
}
