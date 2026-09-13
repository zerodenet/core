use std::{io, pin::Pin};

use base64::Engine;
use foreign_types::ForeignTypeRef;
use openssl::ssl::{Ssl, SslContextBuilder, SslMethod, SslVerifyMode, SslVersion};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_traits::{AsyncSocket, ClientTlsOptions, ServerTlsOptions, TlsBackend, TlsParameters};

use crate::profile::{OwnedClientTlsProfile, OwnedServerTlsProfile};

#[path = "openssl/authority.rs"]
mod authority;
#[path = "openssl/ocsp.rs"]
mod ocsp;
#[path = "openssl/pin.rs"]
mod pin;
#[path = "openssl/session.rs"]
mod session;

unsafe extern "C" {
    fn SSL_set1_ech_config_list(
        ssl: *mut openssl_sys::SSL,
        config: *const u8,
        config_length: usize,
    ) -> std::ffi::c_int;
}

struct Material {
    directory: tempfile::TempDir,
    cert_path: String,
    key_path: String,
}

impl Material {
    fn new(name: &str) -> Self {
        let generated = rcgen::generate_simple_self_signed(vec![name.into()]).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let cert_path = directory.path().join("server.pem");
        let key_path = directory.path().join("server.key");
        std::fs::write(&cert_path, generated.cert.pem()).unwrap();
        std::fs::write(&key_path, generated.signing_key.serialize_pem()).unwrap();
        Self {
            directory,
            cert_path: cert_path.to_string_lossy().into_owned(),
            key_path: key_path.to_string_lossy().into_owned(),
        }
    }

    fn server(&self, options: ServerTlsOptions) -> OwnedServerTlsProfile {
        OwnedServerTlsProfile {
            options,
            cert_path: self.cert_path.clone(),
            key_path: self.key_path.clone(),
            alpn: vec!["h2".into(), "http/1.1".into()],
            server_fingerprint: None,
        }
    }

    fn client(&self, parameters: TlsParameters) -> OwnedClientTlsProfile {
        OwnedClientTlsProfile {
            options: ClientTlsOptions {
                backend: TlsBackend::OpenSsl,
                parameters,
                ..Default::default()
            },
            server_name: Some("secret.example".into()),
            disable_sni: false,
            ca_cert_path: None,
            insecure: true,
            alpn: vec!["h2".into()],
            client_fingerprint: None,
        }
    }
}

fn ech_material(kdf: u16, aead: u16) -> (String, Vec<u8>) {
    // OpenSSL 4.0.2 test vector `test/certs/echdir/ech-eg.pem`, converted to
    // Xray's ECHServerKeys wire shape: key<2> || ECHConfig<2>.
    let private_der = base64::engine::general_purpose::STANDARD
        .decode("MC4CAQAwBQYDK2VuBCIEIKBC3rocwIF5tGY+/TaYQrCxY+ULsch94ja9DojkcvlT")
        .unwrap();
    let private = &private_der[private_der.len() - 32..];
    let mut config_list = base64::engine::general_purpose::STANDARD
        .decode("ADn+DQA1agAgACBtuySC1pphjFlGYKTaSm2KWNg7GQVRS8uAYvLTm5QlGwAEAAEAAQAGZWcuY29tAAA=")
        .unwrap();
    // The official vector has one HPKE symmetric suite at bytes 45..49.
    // RFC 9180 assigns 1/2/3 to SHA-256/SHA-384/SHA-512 and
    // AES-128-GCM/AES-256-GCM/ChaCha20-Poly1305 respectively.
    config_list[45..47].copy_from_slice(&kdf.to_be_bytes());
    config_list[47..49].copy_from_slice(&aead.to_be_bytes());
    let config_length = usize::from(u16::from_be_bytes([config_list[0], config_list[1]]));
    assert_eq!(config_length + 2, config_list.len());
    let config = &config_list[2..];
    let mut xray = Vec::new();
    xray.extend_from_slice(&(private.len() as u16).to_be_bytes());
    xray.extend_from_slice(private);
    xray.extend_from_slice(&(config.len() as u16).to_be_bytes());
    xray.extend_from_slice(config);
    (
        base64::engine::general_purpose::STANDARD.encode(xray),
        config_list,
    )
}

#[tokio::test]
async fn openssl_server_accepts_rfc9849_ech_and_reports_success() {
    let material = Material::new("secret.example");
    for kdf in 1..=3 {
        for aead in 1..=3 {
            let (server_keys, config_list) = ech_material(kdf, aead);
            let profile = material.server(ServerTlsOptions {
                backend: TlsBackend::OpenSsl,
                ech_server_keys: server_keys,
                one_time_loading: true,
                parameters: TlsParameters {
                    min_version: "1.3".into(),
                    max_version: "1.3".into(),
                    ..Default::default()
                },
                ..Default::default()
            });
            let context = super::OpenSslServerContext::build(&profile, None).unwrap();
            let (client_io, server_io) = tokio::io::duplex(256 * 1024);

            let server = async {
                let mut stream =
                    super::super::stream::OpenSslTlsStream::accept(&context, server_io).await?;
                let status = stream.ech_status();
                let mut input = [0; 4];
                stream.read_exact(&mut input).await?;
                stream.write_all(&input).await?;
                stream.flush().await?;
                Ok::<_, io::Error>(status)
            };
            let client = async {
                let mut builder = SslContextBuilder::new(SslMethod::tls_client()).unwrap();
                builder.set_verify(SslVerifyMode::NONE);
                builder
                    .set_min_proto_version(Some(SslVersion::TLS1_3))
                    .unwrap();
                let context = builder.build();
                let mut ssl = Ssl::new(&context).unwrap();
                ssl.set_hostname("secret.example").unwrap();
                assert_eq!(
                    unsafe {
                        SSL_set1_ech_config_list(
                            ssl.as_ptr(),
                            config_list.as_ptr(),
                            config_list.len(),
                        )
                    },
                    1
                );
                let mut stream = tokio_openssl::SslStream::new(ssl, client_io).unwrap();
                Pin::new(&mut stream).connect().await.unwrap();
                stream.write_all(b"echo").await.unwrap();
                stream.flush().await.unwrap();
                let mut output = [0; 4];
                stream.read_exact(&mut output).await.unwrap();
                assert_eq!(&output, b"echo");
            };
            let (status, ()) = tokio::join!(server, client);
            let (status, inner_name, outer_name) = status.unwrap();
            assert_eq!(
                status, 1,
                "OpenSSL did not decrypt ECH with KDF {kdf} and AEAD {aead}"
            );
            assert_eq!(inner_name.as_deref(), Some("secret.example"));
            assert_eq!(outer_name.as_deref(), Some("eg.com"));
        }
    }
}

#[tokio::test]
async fn openssl_legacy_client_and_server_complete_tls10_roundtrip() {
    let material = Material::new("secret.example");
    let parameters = TlsParameters {
        min_version: "1.0".into(),
        max_version: "1.0".into(),
        cipher_suites: vec!["TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA".into()],
        ..Default::default()
    };
    let profile = material.server(ServerTlsOptions {
        backend: TlsBackend::OpenSsl,
        one_time_loading: true,
        parameters: parameters.clone(),
        ..Default::default()
    });
    let acceptor = crate::tls::build_tls_acceptor(&profile, None).unwrap();
    let client_profile = material.client(parameters);
    let (client_io, server_io) = tokio::io::duplex(256 * 1024);
    let (client, server) = tokio::join!(
        crate::tls::connect_tls_stream(client_io, &client_profile, None, "secret.example"),
        crate::tls::accept_tls_handshake(&acceptor, server_io),
    );
    let mut client = client.unwrap();
    let mut server = server.unwrap();
    assert!(client.transport_bypass_control().is_none());
    assert!(server.transport_bypass_control().is_none());
    AsyncSocket::write_all(&mut client, b"legacy")
        .await
        .unwrap();
    let mut output = [0; 6];
    server.read_exact(&mut output).await.unwrap();
    assert_eq!(&output, b"legacy");
}

#[tokio::test]
async fn openssl_tls13_handoff_switches_each_direction_without_duplex_barrier() {
    let material = Material::new("secret.example");
    let parameters = TlsParameters {
        min_version: "1.3".into(),
        max_version: "1.3".into(),
        ..Default::default()
    };
    let profile = material.server(ServerTlsOptions {
        backend: TlsBackend::OpenSsl,
        one_time_loading: true,
        parameters: parameters.clone(),
        ..Default::default()
    });
    let acceptor = crate::tls::build_tls_acceptor(&profile, None).unwrap();
    let client_profile = material.client(parameters);
    let (client_io, server_io) = tokio::io::duplex(256 * 1024);
    let (client, server) = tokio::join!(
        crate::tls::connect_tls_stream(client_io, &client_profile, None, "secret.example"),
        crate::tls::accept_tls_handshake(&acceptor, server_io),
    );
    let mut client = client.unwrap();
    let mut server = server.unwrap();
    let client_control = client.transport_bypass_control().unwrap();
    let server_control = server.transport_bypass_control().unwrap();

    AsyncSocket::write_all(&mut client, b"DIRECT")
        .await
        .unwrap();
    client_control.request_write_bypass();
    tokio::io::AsyncWriteExt::write_all(&mut client, b"large-post-body")
        .await
        .unwrap();
    client.flush().await.unwrap();
    let mut direct = [0; 6];
    server.read_exact(&mut direct).await.unwrap();
    assert_eq!(&direct, b"DIRECT");
    server_control.request_read_bypass();
    let mut body = [0; 15];
    server.read_exact(&mut body).await.unwrap();
    assert_eq!(&body, b"large-post-body");

    AsyncSocket::write_all(&mut server, b"DIRECT")
        .await
        .unwrap();
    server_control.request_write_bypass();
    tokio::io::AsyncWriteExt::write_all(&mut server, b"response-body")
        .await
        .unwrap();
    server.flush().await.unwrap();
    client.read_exact(&mut direct).await.unwrap();
    assert_eq!(&direct, b"DIRECT");
    client_control.request_read_bypass();
    let mut response = [0; 13];
    client.read_exact(&mut response).await.unwrap();
    assert_eq!(&response, b"response-body");
}

#[tokio::test]
async fn openssl_certificate_refresh_keeps_last_good_and_stops_with_context() {
    let material = Material::new("secret.example");
    let profile = material.server(ServerTlsOptions {
        backend: TlsBackend::OpenSsl,
        reload_interval_secs: 1,
        ..Default::default()
    });
    let context = super::OpenSslServerContext::build(&profile, None).unwrap();
    let runtime = std::sync::Arc::downgrade(&context.runtime);
    let original = context
        .new_server_ssl()
        .unwrap()
        .certificate()
        .unwrap()
        .to_der()
        .unwrap();

    std::fs::write(&material.cert_path, b"invalid certificate").unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    let retained = context
        .new_server_ssl()
        .unwrap()
        .certificate()
        .unwrap()
        .to_der()
        .unwrap();
    assert_eq!(retained, original);

    let replacement = rcgen::generate_simple_self_signed(vec!["secret.example".into()]).unwrap();
    std::fs::write(&material.cert_path, replacement.cert.pem()).unwrap();
    std::fs::write(&material.key_path, replacement.signing_key.serialize_pem()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(4), async {
        loop {
            let current = context
                .new_server_ssl()
                .unwrap()
                .certificate()
                .unwrap()
                .to_der()
                .unwrap();
            if current != original {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();

    drop(context);
    assert!(runtime.upgrade().is_none());
}

#[tokio::test]
async fn fingerprint_ech_and_psk_share_the_authenticated_transcript() {
    let material = Material::new("secret.example");
    let (server_keys, config_list) = ech_material(1, 1);
    let profile = material.server(ServerTlsOptions {
        backend: TlsBackend::OpenSsl,
        ech_server_keys: server_keys,
        one_time_loading: true,
        parameters: TlsParameters {
            min_version: "1.3".into(),
            enable_session_resumption: true,
            ..Default::default()
        },
        ..Default::default()
    });
    let context = super::OpenSslServerContext::build(&profile, None).unwrap();
    let mut client_profile = material.client(Default::default());
    client_profile.options.backend = TlsBackend::Rustls;
    client_profile.client_fingerprint = Some("chrome".into());
    client_profile.options.parameters.enable_session_resumption = true;
    client_profile.options.ech.config_list =
        base64::engine::general_purpose::STANDARD.encode(config_list);
    for expected in [rustls::HandshakeKind::Full, rustls::HandshakeKind::Resumed] {
        let config = crate::tls::config::client(&client_profile, None, false).unwrap();
        let (a, b) = tokio::io::duplex(256 * 1024);
        let server = async {
            let mut stream = super::super::stream::OpenSslTlsStream::accept(&context, b)
                .await
                .unwrap();
            assert_eq!(stream.ech_status().0, 1);
            stream.write_all(b"x").await.unwrap();
            stream.flush().await.unwrap();
        };
        let client = async {
            let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
            let mut stream = connector
                .connect("secret.example".try_into().unwrap(), a)
                .await
                .unwrap();
            let mut data = [0];
            stream.read_exact(&mut data).await.unwrap();
            assert_eq!(data, [b'x']);
            assert_eq!(stream.get_ref().1.handshake_kind().unwrap(), expected);
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(server, client);
        })
        .await
        .unwrap();
    }
}
