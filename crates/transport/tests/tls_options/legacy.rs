//! Deferred interoperability regressions: these sources are not test results.
use super::*;
use openssl::{
    bn::BigNum,
    dh::Dh,
    pkey::PKey,
    rsa::Rsa,
    ssl::{Ssl, SslContextBuilder, SslMethod, SslVersion},
    x509::X509,
};
use std::pin::Pin;

#[tokio::test]
async fn fingerprint_cbc_rsa_and_dhe_complete_verified_and_resumed_tls12() {
    for (suite, openssl_name, fingerprint, ecdsa) in [
        (
            "TLS_RSA_WITH_AES_128_CBC_SHA",
            "AES128-SHA",
            "chrome-83",
            false,
        ),
        (
            "TLS_RSA_WITH_AES_256_CBC_SHA",
            "AES256-SHA",
            "chrome-83",
            false,
        ),
        (
            "TLS_RSA_WITH_AES_128_CBC_SHA256",
            "AES128-SHA256",
            "ios-14",
            false,
        ),
        (
            "TLS_RSA_WITH_AES_256_CBC_SHA256",
            "AES256-SHA256",
            "ios-14",
            false,
        ),
        (
            "TLS_RSA_WITH_AES_128_GCM_SHA256",
            "AES128-GCM-SHA256",
            "chrome-83",
            false,
        ),
        (
            "TLS_RSA_WITH_AES_256_GCM_SHA384",
            "AES256-GCM-SHA384",
            "chrome-83",
            false,
        ),
        (
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
            "ECDHE-RSA-AES128-SHA",
            "chrome-83",
            false,
        ),
        (
            "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
            "ECDHE-RSA-AES256-SHA",
            "chrome-83",
            false,
        ),
        (
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
            "ECDHE-RSA-AES128-SHA256",
            "ios-14",
            false,
        ),
        (
            "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
            "ECDHE-RSA-AES256-SHA384",
            "ios-14",
            false,
        ),
        (
            "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
            "ECDHE-ECDSA-AES128-SHA",
            "chrome-83",
            true,
        ),
        (
            "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
            "ECDHE-ECDSA-AES256-SHA",
            "chrome-83",
            true,
        ),
        (
            "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
            "ECDHE-ECDSA-AES128-SHA256",
            "ios-14",
            true,
        ),
        (
            "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
            "ECDHE-ECDSA-AES256-SHA384",
            "ios-14",
            true,
        ),
        (
            "TLS_DHE_RSA_WITH_AES_128_CBC_SHA",
            "DHE-RSA-AES128-SHA",
            "firefox-63",
            false,
        ),
        (
            "TLS_DHE_RSA_WITH_AES_256_CBC_SHA",
            "DHE-RSA-AES256-SHA",
            "firefox-63",
            false,
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let key = if ecdsa {
            rcgen::KeyPair::generate().unwrap()
        } else {
            let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
            rcgen::KeyPair::from_pem(
                &String::from_utf8(key.private_key_to_pem_pkcs8().unwrap()).unwrap(),
            )
            .unwrap()
        };
        let cert = rcgen::CertificateParams::new(vec!["one.test".into()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let ca = directory.path().join("ca.pem");
        std::fs::write(&ca, cert.pem()).unwrap();
        let mut server = SslContextBuilder::new(SslMethod::tls_server()).unwrap();
        server
            .set_min_proto_version(Some(SslVersion::TLS1_2))
            .unwrap();
        server
            .set_max_proto_version(Some(SslVersion::TLS1_2))
            .unwrap();
        server.set_cipher_list(openssl_name).unwrap();
        server
            .set_certificate(&X509::from_der(cert.der()).unwrap())
            .unwrap();
        server
            .set_private_key(&PKey::private_key_from_pem(key.serialize_pem().as_bytes()).unwrap())
            .unwrap();
        server
            .set_session_id_context(b"zero-legacy-interop")
            .unwrap();
        let group = rustls::ffdhe_groups::FFDHE2048;
        let dh = Dh::from_pqg(
            BigNum::from_slice(group.p).unwrap(),
            None,
            BigNum::from_slice(group.g).unwrap(),
        )
        .unwrap();
        server.set_tmp_dh(&dh).unwrap();
        let server = server.build();
        let profile = OwnedClientTlsProfile {
            options: zero_traits::ClientTlsOptions {
                disable_system_roots: true,
                parameters: zero_traits::TlsParameters {
                    min_version: "1.2".into(),
                    max_version: "1.2".into(),
                    cipher_suites: vec![suite.into()],
                    enable_session_resumption: true,
                    ..Default::default()
                },
                ..Default::default()
            },
            server_name: Some("one.test".into()),
            disable_sni: false,
            ca_cert_path: Some(ca.to_string_lossy().into_owned()),
            insecure: false,
            alpn: Vec::new(),
            client_fingerprint: Some(fingerprint.into()),
        };
        let config = Arc::new(tls::config::client(&profile, None, false).unwrap());
        for expected in [rustls::HandshakeKind::Full, rustls::HandshakeKind::Resumed] {
            tokio::time::timeout(Duration::from_secs(10), async {
                let (client_io, server_io) = tokio::io::duplex(128 * 1024);
                let mut server =
                    tokio_openssl::SslStream::new(Ssl::new(&server).unwrap(), server_io).unwrap();
                let connector = tokio_rustls::TlsConnector::from(config.clone());
                let client = async {
                    let mut client = connector
                        .connect("one.test".try_into().unwrap(), client_io)
                        .await
                        .unwrap();
                    assert_eq!(
                        client.get_ref().1.handshake_kind(),
                        Some(expected),
                        "{suite}"
                    );
                    assert_eq!(
                        u16::from(
                            client
                                .get_ref()
                                .1
                                .negotiated_cipher_suite()
                                .unwrap()
                                .suite()
                        ),
                        ztls::settings::cipher_suite(suite).unwrap()
                    );
                    let data = vec![0x5a; 32769];
                    client.write_all(&data).await.unwrap();
                    client.flush().await.unwrap();
                    let mut echo = vec![0; data.len()];
                    client.read_exact(&mut echo).await.unwrap();
                    assert_eq!(echo, data);
                };
                let server = async {
                    Pin::new(&mut server).accept().await.unwrap();
                    let mut data = vec![0; 32769];
                    server.read_exact(&mut data).await.unwrap();
                    server.write_all(&data).await.unwrap();
                    server.flush().await.unwrap();
                };
                tokio::join!(client, server);
            })
            .await
            .unwrap();
        }
    }
}
