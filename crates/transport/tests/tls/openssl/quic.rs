use std::time::Duration;

use base64::Engine;
use bytes::BytesMut;
use quinn_proto::Side;
use zero_traits::{
    ClientTlsOptions, EchClientOptions, ServerTlsOptions, TlsBackend, TlsParameters,
};

use super::keys::{self, CipherSuite, Version};
use super::session::{decode_transport_parameters, HandshakeData};
use crate::profile::OwnedServerTlsProfile;

const INNER_NAME: &str = "inner.example.com";
const PUBLIC_NAME: &str = "eg.com";
const ECH_CONFIG_LIST: &str =
    "ADn+DQA1agAgACBtuySC1pphjFlGYKTaSm2KWNg7GQVRS8uAYvLTm5QlGwAEAAEAAQAGZWcuY29tAAA=";
const ECH_PRIVATE_PKCS8: &str = "MC4CAQAwBQYDK2VuBCIEIKBC3rocwIF5tGY+/TaYQrCxY+ULsch94ja9DojkcvlT";

struct Material {
    dir: std::path::PathBuf,
    profile: OwnedServerTlsProfile,
}

impl Material {
    fn new() -> Self {
        let dir =
            std::env::temp_dir().join(format!("zero-openssl-quic-ech-{}", rand::random::<u64>()));
        std::fs::create_dir(&dir).unwrap();
        let certified =
            rcgen::generate_simple_self_signed(vec![INNER_NAME.to_owned(), PUBLIC_NAME.to_owned()])
                .unwrap();
        std::fs::write(dir.join("server.pem"), certified.cert.pem()).unwrap();
        std::fs::write(
            dir.join("server.key"),
            certified.signing_key.serialize_pem(),
        )
        .unwrap();

        let list = base64::engine::general_purpose::STANDARD
            .decode(ECH_CONFIG_LIST)
            .unwrap();
        let private = base64::engine::general_purpose::STANDARD
            .decode(ECH_PRIVATE_PKCS8)
            .unwrap();
        let private = &private[private.len() - 32..];
        let config = &list[2..];
        let mut server_keys = Vec::new();
        server_keys.extend_from_slice(&(private.len() as u16).to_be_bytes());
        server_keys.extend_from_slice(private);
        server_keys.extend_from_slice(&(config.len() as u16).to_be_bytes());
        server_keys.extend_from_slice(config);

        let profile = OwnedServerTlsProfile {
            options: ServerTlsOptions {
                backend: TlsBackend::OpenSsl,
                ech_server_keys: base64::engine::general_purpose::STANDARD.encode(server_keys),
                one_time_loading: true,
                parameters: TlsParameters {
                    min_version: "1.3".into(),
                    max_version: "1.3".into(),
                    cipher_suites: vec!["TLS_AES_128_GCM_SHA256".into()],
                    ..Default::default()
                },
                ..Default::default()
            },
            cert_path: "server.pem".into(),
            key_path: "server.key".into(),
            alpn: Vec::new(),
            server_fingerprint: None,
        };
        Self { dir, profile }
    }
}

impl Drop for Material {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn assert_updated_keys_interoperate(suite: CipherSuite) {
    let local = vec![7_u8; suite.secret_len()];
    let remote = vec![9_u8; suite.secret_len()];
    let next_local = keys::next_secret(Version::V1, suite, &local).unwrap();
    let next_remote = keys::next_secret(Version::V1, suite, &remote).unwrap();
    assert_ne!(next_local, local);
    assert_ne!(next_remote, remote);

    let server = keys::keys_from_pair(Version::V1, suite, &next_local, &next_remote).unwrap();
    let client = keys::keys_from_pair(Version::V1, suite, &next_remote, &next_local).unwrap();
    let header = [0x43, 0, 0, 0, 1];
    let mut packet = header.to_vec();
    packet.extend_from_slice(b"key update payload long enough for header sample");
    packet.resize(packet.len() + server.packet.local.tag_len(), 0);
    server.packet.local.encrypt(17, &mut packet, header.len());
    let unprotected = packet.clone();
    server.header.local.encrypt(1, &mut packet);
    assert_ne!(packet, unprotected);
    client.header.remote.decrypt(1, &mut packet);
    assert_eq!(packet, unprotected);
    let mut payload = BytesMut::from(&packet[header.len()..]);
    client
        .packet
        .remote
        .decrypt(17, &header, &mut payload)
        .unwrap();
    assert_eq!(
        &payload[..],
        b"key update payload long enough for header sample"
    );
}

#[test]
fn aes128_gcm_sha256_packet_header_and_key_update_interoperate() {
    assert_updated_keys_interoperate(CipherSuite::Aes128GcmSha256);
}

#[test]
fn aes256_gcm_sha384_packet_header_and_key_update_interoperate() {
    assert_updated_keys_interoperate(CipherSuite::Aes256GcmSha384);
}

#[test]
fn chacha20_poly1305_sha256_packet_header_and_key_update_interoperate() {
    assert_updated_keys_interoperate(CipherSuite::ChaCha20Poly1305Sha256);
}

#[test]
fn server_rejects_server_only_transport_parameter_from_client() {
    // original_destination_connection_id (0x00) is legal only when sent by a server.
    let original_destination_connection_id = [0x00, 0x01, 0x01];
    assert!(
        decode_transport_parameters(Side::Server, &original_destination_connection_id).is_err()
    );
    assert!(decode_transport_parameters(Side::Client, &original_destination_connection_id).is_ok());
}

#[tokio::test]
async fn openssl_server_accepts_an_actual_ech_quic_handshake() {
    let material = Material::new();
    let server = crate::quic::QuicInbound::bind_with_tls_profile(
        "127.0.0.1:0",
        &material.profile,
        Some(&material.dir),
        &[b"h3".to_vec()],
        quinn::TransportConfig::default(),
        &[],
    )
    .await
    .unwrap();

    let client_options = ClientTlsOptions {
        ech: EchClientOptions {
            config_list: ECH_CONFIG_LIST.into(),
            ..Default::default()
        },
        ..Default::default()
    };
    let client_config = crate::quic::client_config_with_options(
        true,
        None,
        &[b"h3".to_vec()],
        None,
        None,
        &client_options,
    )
    .unwrap();
    let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client.set_default_client_config(client_config);

    let address = server.local_addr().unwrap();
    let client_connecting = client.connect(address, INNER_NAME).unwrap();
    let server_connecting = async {
        let incoming = server.accept_incoming().await.unwrap();
        crate::quic::QuicInbound::establish_incoming(incoming).await
    };
    let (client_connection, server_connection) =
        tokio::time::timeout(Duration::from_secs(5), async {
            let (client, server) = tokio::join!(client_connecting, server_connecting);
            (
                client.expect("rustls QUIC client"),
                server.expect("OpenSSL QUIC server"),
            )
        })
        .await
        .expect("ECH QUIC handshake timed out");

    let negotiated = server_connection
        .handshake_data()
        .unwrap()
        .downcast::<HandshakeData>()
        .unwrap();
    assert_eq!(negotiated.server_name.as_deref(), Some(INNER_NAME));
    assert_eq!(negotiated.protocol.as_deref(), Some(b"h3".as_slice()));
    assert_eq!(
        crate::quic::handshake_metadata(&server_connection),
        (Some(INNER_NAME.into()), Some("h3".into()))
    );

    let mut client_export = [0_u8; 32];
    let mut server_export = [0_u8; 32];
    client_connection
        .export_keying_material(&mut client_export, b"zero quic test", b"ech")
        .unwrap();
    server_connection
        .export_keying_material(&mut server_export, b"zero quic test", b"ech")
        .unwrap();
    assert_eq!(client_export, server_export);

    client_connection.force_key_update();
    server_connection.force_key_update();
    let (mut send, mut receive) = client_connection.open_bi().await.unwrap();
    send.write_all(b"ech-quic").await.unwrap();
    send.finish().unwrap();
    let (mut peer_send, mut peer_receive) = server_connection.accept_bi().await.unwrap();
    let request = peer_receive.read_to_end(64).await.unwrap();
    assert_eq!(request, b"ech-quic");
    peer_send.write_all(b"accepted").await.unwrap();
    peer_send.finish().unwrap();
    assert_eq!(receive.read_to_end(64).await.unwrap(), b"accepted");

    let cancelled_client = client.connect(address, INNER_NAME).unwrap();
    let cancelled_server = tokio::time::timeout(Duration::from_secs(5), server.accept_incoming())
        .await
        .expect("cancelled handshake did not reach the server")
        .unwrap();
    let cancelled_server = cancelled_server.accept().unwrap();
    drop(cancelled_client);
    drop(cancelled_server);

    client_connection.close(0_u32.into(), b"done");
    server_connection.close(0_u32.into(), b"done");
    client.wait_idle().await;
}
