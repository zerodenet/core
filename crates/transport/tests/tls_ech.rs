use base64::Engine;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use zero_traits::{ClientTlsOptions, EchForceQuery, TlsBackend, TlsParameters};
use zero_transport::profile::OwnedClientTlsProfile;

struct Resolver {
    material: Option<Vec<u8>>,
}

impl zero_transport::tls::ech::EchConfigResolver for Resolver {
    fn resolve(
        &self,
        server: String,
        query_name: String,
        force_query: EchForceQuery,
    ) -> zero_transport::tls::ech::EchConfigResolveFuture {
        assert_eq!(server, "udp://1.1.1.1");
        assert_eq!(query_name, "secret.example");
        assert_eq!(force_query, EchForceQuery::Half);
        let material = self.material.clone();
        Box::pin(async move { Ok(material) })
    }
}

fn ech_config_list_with_suite(public_name: &str, kdf_id: u16, aead_id: u16) -> Vec<u8> {
    let mut contents = vec![7, 0, 0x20, 0, 32];
    contents.extend_from_slice(&[
        9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0,
    ]);
    contents.extend_from_slice(&[0, 4]);
    contents.extend_from_slice(&kdf_id.to_be_bytes());
    contents.extend_from_slice(&aead_id.to_be_bytes());
    contents.push(0);
    contents.push(public_name.len() as u8);
    contents.extend_from_slice(public_name.as_bytes());
    contents.extend_from_slice(&[0, 0]);
    let mut config = vec![0xfe, 0x0d];
    config.extend_from_slice(&(contents.len() as u16).to_be_bytes());
    config.extend_from_slice(&contents);
    let mut list = Vec::new();
    list.extend_from_slice(&(config.len() as u16).to_be_bytes());
    list.extend_from_slice(&config);
    list
}

fn ech_config_list(public_name: &str) -> Vec<u8> {
    ech_config_list_with_suite(public_name, 1, 1)
}

#[test]
fn static_ech_builds_an_encrypted_client_hello() {
    let public_name = "public.example";
    let secret_name = "secret.example";
    let mut profile = OwnedClientTlsProfile {
        options: Default::default(),
        server_name: Some(secret_name.to_owned()),
        disable_sni: false,
        ca_cert_path: None,
        insecure: true,
        alpn: vec!["h2".to_owned()],
        client_fingerprint: None,
    };
    profile.options.ech.config_list =
        base64::engine::general_purpose::STANDARD.encode(ech_config_list(public_name));
    let config = zero_transport::tls::config::client(&profile, None, false).unwrap();
    let server_name = rustls::pki_types::ServerName::try_from(secret_name)
        .unwrap()
        .to_owned();
    let mut connection = rustls::ClientConnection::new(Arc::new(config), server_name).unwrap();
    let mut wire = Vec::new();
    connection.write_tls(&mut wire).unwrap();

    assert!(wire
        .windows(public_name.len())
        .any(|part| part == public_name.as_bytes()));
    assert!(!wire
        .windows(secret_name.len())
        .any(|part| part == secret_name.as_bytes()));
    assert!(wire.windows(2).any(|part| part == [0xfe, 0x0d]));
}

#[tokio::test]
async fn xray_x25519_hpke_matrix_uses_ech_with_client_fingerprint() {
    let public_name = "public.example";
    let secret_name = "secret.example";
    for kdf_id in 1..=3 {
        for aead_id in 1..=3 {
            let mut profile = OwnedClientTlsProfile {
                options: ClientTlsOptions {
                    parameters: TlsParameters {
                        min_version: "1.0".to_owned(),
                        max_version: "1.3".to_owned(),
                        cipher_suites: vec!["TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA".to_owned()],
                        ..Default::default()
                    },
                    ..Default::default()
                },
                server_name: Some(secret_name.to_owned()),
                disable_sni: false,
                ca_cert_path: None,
                insecure: true,
                alpn: vec!["h2".to_owned()],
                client_fingerprint: Some("chrome".to_owned()),
            };
            profile.options.ech.config_list = base64::engine::general_purpose::STANDARD
                .encode(ech_config_list_with_suite(public_name, kdf_id, aead_id));

            let (client_io, mut server_io) = tokio::io::duplex(64 * 1024);
            let connect =
                zero_transport::tls::connect_tls_stream(client_io, &profile, None, secret_name);
            let capture = async move {
                let mut header = [0_u8; 5];
                server_io.read_exact(&mut header).await.unwrap();
                let record_len = usize::from(u16::from_be_bytes([header[3], header[4]]));
                let mut wire = vec![0_u8; record_len];
                server_io.read_exact(&mut wire).await.unwrap();
                (header, wire)
            };
            let (connect, (header, wire)) = tokio::join!(connect, capture);
            assert_eq!(header[0], 22, "kdf={kdf_id}, aead={aead_id}");

            assert!(
                wire.windows(public_name.len())
                    .any(|part| part == public_name.as_bytes()),
                "kdf={kdf_id}, aead={aead_id}"
            );
            assert!(
                !wire
                    .windows(secret_name.len())
                    .any(|part| part == secret_name.as_bytes()),
                "kdf={kdf_id}, aead={aead_id}"
            );
            assert!(
                wire.windows(2).any(|part| part == [0xfe, 0x0d]),
                "kdf={kdf_id}, aead={aead_id}"
            );

            assert!(connect.is_err());
        }
    }
}

#[tokio::test]
async fn explicit_openssl_client_does_not_ignore_ech() {
    let mut profile = OwnedClientTlsProfile {
        options: ClientTlsOptions {
            backend: TlsBackend::OpenSsl,
            ..Default::default()
        },
        server_name: Some("secret.example".to_owned()),
        disable_sni: false,
        ca_cert_path: None,
        insecure: true,
        alpn: Vec::new(),
        client_fingerprint: None,
    };
    profile.options.ech.config_list =
        base64::engine::general_purpose::STANDARD.encode(ech_config_list("public.example"));
    let (client_io, _server_io) = tokio::io::duplex(4096);
    let error =
        match zero_transport::tls::connect_tls_stream(client_io, &profile, None, "secret.example")
            .await
        {
            Ok(_) => panic!("OpenSSL silently accepted unsupported client ECH"),
            Err(error) => error,
        };
    assert!(error.to_string().contains("rustls TLS backend"));
}

#[test]
fn dns_ech_requires_async_preparation() {
    let mut profile = OwnedClientTlsProfile {
        options: Default::default(),
        server_name: Some("secret.example".to_owned()),
        disable_sni: false,
        ca_cert_path: None,
        insecure: true,
        alpn: Vec::new(),
        client_fingerprint: None,
    };
    profile.options.ech.config_list = "udp://127.0.0.1".to_owned();
    let error = zero_transport::tls::config::client(&profile, None, false).unwrap_err();
    assert!(error.to_string().contains("not prepared"));
}

#[tokio::test]
async fn async_preparation_materializes_dns_ech_and_plain_fallback() {
    let mut options = ClientTlsOptions::default();
    options.ech.config_list = "udp://1.1.1.1".to_owned();
    options.ech.force_query = EchForceQuery::Half;
    let material = ech_config_list("public.example");
    zero_transport::tls::ech::prepare_options(
        &mut options,
        Some("secret.example"),
        "unused.example",
        &Resolver {
            material: Some(material.clone()),
        },
    )
    .await
    .unwrap();
    assert_eq!(options.ech.prepared_config_list, Some(material));

    zero_transport::tls::ech::prepare_options(
        &mut options,
        Some("secret.example"),
        "unused.example",
        &Resolver { material: None },
    )
    .await
    .unwrap();
    assert_eq!(options.ech.prepared_config_list, Some(Vec::new()));
}
