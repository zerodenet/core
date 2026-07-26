use std::sync::Arc;

use shadowsocks::transport::ShadowsocksInboundUserRef;
use shadowsocks::udp::{
    ShadowsocksDatagramCodec, ShadowsocksInboundUdpCodec, ShadowsocksInboundUdpResponder,
};
use shadowsocks::{
    ShadowsocksInboundProfile, ShadowsocksInboundProfileStore, ShadowsocksInboundTcpAcceptor,
    ShadowsocksOutbound,
};
use tokio::io::{duplex, AsyncRead, AsyncWrite, ReadBuf};
use zero_core::{Address, DatagramUdpResponder, Network, ProtocolType, Session};
use zero_traits::{AsyncSocket, DatagramCodec};

#[cfg(feature = "blake3")]
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};

struct TestSocket(tokio::io::DuplexStream);

impl AsyncSocket for TestSocket {
    type Error = std::io::Error;

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        tokio::io::AsyncReadExt::read(&mut self.0, buf).await
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        tokio::io::AsyncWriteExt::write_all(&mut self.0, buf).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        tokio::io::AsyncWriteExt::shutdown(&mut self.0).await
    }
}

impl AsyncRead for TestSocket {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for TestSocket {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

fn users<'a>() -> [ShadowsocksInboundUserRef<'a>; 2] {
    [
        ShadowsocksInboundUserRef {
            password: "first-secret",
            principal_key: Some("principal-1"),
            up_bps: Some(10),
            down_bps: Some(20),
            device_limit: Some(1),
            quota_remaining_bytes: Some(1024),
            policy_revision: Some(1),
        },
        ShadowsocksInboundUserRef {
            password: "second-secret",
            principal_key: Some("principal-2"),
            up_bps: Some(30),
            down_bps: Some(40),
            device_limit: Some(2),
            quota_remaining_bytes: Some(4096),
            policy_revision: Some(2),
        },
    ]
}

#[tokio::test]
async fn tcp_acceptor_matches_user_and_applies_identity() {
    let profile = ShadowsocksInboundProfile::from_config_users("aes-128-gcm", users()).unwrap();
    let acceptor = ShadowsocksInboundTcpAcceptor::new(profile);
    let (client, server) = duplex(8192);
    let (mut client, server) = (TestSocket(client), TestSocket(server));
    let target = Session::new(
        0,
        Address::Domain("example.com".to_owned()),
        443,
        Network::Tcp,
        ProtocolType::new("shadowsocks"),
    );

    ShadowsocksOutbound
        .send_request(
            &mut client,
            &target,
            shadowsocks::CipherKind::Aes128Gcm,
            b"second-secret",
        )
        .await
        .unwrap();
    let (session, _stream) = acceptor.accept_stream(server).await.unwrap();
    let auth = session.auth.unwrap();
    assert_eq!(auth.principal_key.as_deref(), Some("principal-2"));
    assert_eq!(session.up_bps, Some(30));
    assert_eq!(session.down_bps, Some(40));
    assert_eq!(auth.device_limit, Some(2));
    assert_eq!(auth.quota_remaining_bytes, Some(4096));
    assert_eq!(auth.policy_revision, Some(2));
}

#[test]
fn udp_responder_matches_user_and_exposes_packet_auth() {
    let profile = ShadowsocksInboundProfile::from_config_users("aes-128-gcm", users()).unwrap();
    let mut responder = ShadowsocksInboundUdpResponder::from_profile(profile);
    let codec = ShadowsocksDatagramCodec {
        cipher: shadowsocks::CipherKind::Aes128Gcm,
        password: b"second-secret".to_vec(),
    };
    let datagram = codec
        .encode(&Address::Domain("dns.example".to_owned()), 53, b"query")
        .unwrap();

    let dispatch = responder.decode_inbound_dispatch(&datagram).unwrap();
    assert_eq!(dispatch.port(), 53);
    let auth = <ShadowsocksInboundUdpResponder as DatagramUdpResponder<
        Arc<tokio::net::UdpSocket>,
    >>::auth(&responder)
    .unwrap();
    assert_eq!(auth.principal_key.as_deref(), Some("principal-2"));
    assert_eq!(auth.device_limit, Some(2));
    assert_eq!(auth.quota_remaining_bytes, Some(4096));
    assert_eq!(auth.policy_revision, Some(2));
}

#[test]
fn profile_store_updates_existing_listener_profile_atomically() {
    let store = ShadowsocksInboundProfileStore::default();
    let first = [users()[0]];
    let profile = store
        .replace("ss-in", "aes-128-gcm", &first)
        .expect("initial profile");
    assert_eq!(profile.user_count(), 1);

    let replacement = [users()[1]];
    let same_profile = store
        .replace("ss-in", "aes-128-gcm", &replacement)
        .expect("replacement profile");
    assert_eq!(profile.user_count(), 1);
    assert_eq!(same_profile.user_count(), 1);

    let empty: [ShadowsocksInboundUserRef<'_>; 0] = [];
    store
        .replace("ss-in", "aes-128-gcm", &empty)
        .expect("empty authorization set");
    assert_eq!(profile.user_count(), 0);
}

#[cfg(feature = "blake3")]
fn eih_keys(cipher: shadowsocks::CipherKind) -> (&'static str, &'static str) {
    match cipher {
        shadowsocks::CipherKind::Blake3Aes128Gcm => {
            ("MDEyMzQ1Njc4OWFiY2RlZg==", "ZmVkY2JhOTg3NjU0MzIxMA==")
        }
        shadowsocks::CipherKind::Blake3Aes256Gcm => (
            "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
            "ZmVkY2JhOTg3NjU0MzIxMGZlZGNiYTk4NzY1NDMyMTA=",
        ),
        _ => panic!("test only supports SIP023 AES methods"),
    }
}

#[cfg(feature = "blake3")]
fn encrypt_eih_block(cipher: shadowsocks::CipherKind, key: &[u8], block: &mut [u8; 16]) {
    use aes::cipher::{BlockEncrypt, KeyInit};
    match cipher {
        shadowsocks::CipherKind::Blake3Aes128Gcm => {
            aes::Aes128::new_from_slice(key)
                .unwrap()
                .encrypt_block(block.into());
        }
        shadowsocks::CipherKind::Blake3Aes256Gcm => {
            aes::Aes256::new_from_slice(key)
                .unwrap()
                .encrypt_block(block.into());
        }
        _ => panic!("test only supports SIP023 AES methods"),
    }
}

#[cfg(feature = "blake3")]
fn eih_user(cipher: shadowsocks::CipherKind, password: &str) -> ShadowsocksInboundUserRef<'_> {
    let _ = cipher;
    ShadowsocksInboundUserRef {
        password,
        principal_key: Some("account:eih-user"),
        up_bps: Some(1024),
        down_bps: Some(2048),
        device_limit: Some(3),
        quota_remaining_bytes: Some(8192),
        policy_revision: Some(7),
    }
}

#[cfg(feature = "blake3")]
fn encode_tcp_eih_request(
    cipher: shadowsocks::CipherKind,
    identity_password: &str,
    user_password: &str,
    target: &Address,
    port: u16,
) -> Vec<u8> {
    let identity_key = BASE64_STANDARD.decode(identity_password).unwrap();
    let user_key = BASE64_STANDARD.decode(user_password).unwrap();
    let salt = vec![0x42_u8; cipher.salt_len()];

    let mut material = identity_key.clone();
    material.extend_from_slice(&salt);
    let identity_subkey = blake3::derive_key("shadowsocks 2022 identity subkey", &material);
    let mut identity_header = [0_u8; 16];
    identity_header.copy_from_slice(&blake3::hash(&user_key).as_bytes()[..16]);
    encrypt_eih_block(
        cipher,
        &identity_subkey[..cipher.key_len()],
        &mut identity_header,
    );

    let variable = shadowsocks::build_2022_request_var_header(target, port, &[], b"hello")
        .expect("variable header");
    let fixed = shadowsocks::build_2022_request_fixed_header(
        shadowsocks::now_unix_seconds(),
        variable.len() as u16,
    );
    let session_key =
        shadowsocks::derive_session_key(cipher, user_password.as_bytes(), &salt).unwrap();
    let mut nonce = 0;
    let encrypted_fixed =
        shadowsocks::encrypt_tcp_2022_single_chunk(cipher, &session_key, &mut nonce, &fixed)
            .unwrap();
    let encrypted_variable =
        shadowsocks::encrypt_tcp_2022_single_chunk(cipher, &session_key, &mut nonce, &variable)
            .unwrap();

    let mut request = salt;
    request.extend_from_slice(&identity_header);
    request.extend_from_slice(&encrypted_fixed);
    request.extend_from_slice(&encrypted_variable);
    request
}

#[cfg(feature = "blake3")]
fn encode_udp_eih_request(
    cipher: shadowsocks::CipherKind,
    identity_password: &str,
    user_password: &str,
    target: &Address,
    port: u16,
) -> Vec<u8> {
    let identity_key = BASE64_STANDARD.decode(identity_password).unwrap();
    let user_key = BASE64_STANDARD.decode(user_password).unwrap();
    let mut separate_header = [0_u8; 16];
    separate_header[..8].copy_from_slice(&0x1122_3344_5566_7788_u64.to_be_bytes());
    separate_header[8..].copy_from_slice(&1_u64.to_be_bytes());

    let mut identity_header = [0_u8; 16];
    identity_header.copy_from_slice(&blake3::hash(&user_key).as_bytes()[..16]);
    for (byte, header_byte) in identity_header.iter_mut().zip(separate_header) {
        *byte ^= header_byte;
    }
    encrypt_eih_block(cipher, &identity_key, &mut identity_header);

    let mut body = Vec::new();
    body.push(0);
    body.extend_from_slice(&shadowsocks::now_unix_seconds().to_be_bytes());
    body.extend_from_slice(&0_u16.to_be_bytes());
    body.extend_from_slice(&shadowsocks::build_target_data(target, port, b"dns-query").unwrap());
    let session_key =
        shadowsocks::derive_key_blake3(&user_key, &separate_header[..8], cipher.key_len()).unwrap();
    let nonce: [u8; 12] = separate_header[4..16].try_into().unwrap();
    let encrypted_body = shadowsocks::aead_encrypt(cipher, &session_key, &nonce, &body).unwrap();

    let mut encrypted_separate_header = separate_header;
    encrypt_eih_block(cipher, &identity_key, &mut encrypted_separate_header);
    let mut datagram = encrypted_separate_header.to_vec();
    datagram.extend_from_slice(&identity_header);
    datagram.extend_from_slice(&encrypted_body);
    datagram
}

#[cfg(feature = "blake3")]
#[tokio::test]
async fn sip023_tcp_eih_selects_the_user_psk_for_both_aes_methods() {
    for cipher in [
        shadowsocks::CipherKind::Blake3Aes128Gcm,
        shadowsocks::CipherKind::Blake3Aes256Gcm,
    ] {
        let (identity_password, user_password) = eih_keys(cipher);
        let profile = ShadowsocksInboundProfile::from_config_users_with_identity(
            match cipher {
                shadowsocks::CipherKind::Blake3Aes128Gcm => "2022-blake3-aes-128-gcm",
                shadowsocks::CipherKind::Blake3Aes256Gcm => "2022-blake3-aes-256-gcm",
                _ => unreachable!(),
            },
            Some(identity_password),
            [eih_user(cipher, user_password)],
        )
        .unwrap();
        let acceptor = ShadowsocksInboundTcpAcceptor::new(profile);
        let (client, server) = duplex(8192);
        let (mut client, server) = (TestSocket(client), TestSocket(server));
        let request = encode_tcp_eih_request(
            cipher,
            identity_password,
            user_password,
            &Address::Domain("eih.example".to_owned()),
            443,
        );
        AsyncSocket::write_all(&mut client, &request).await.unwrap();

        let (session, _stream) = acceptor.accept_stream(server).await.unwrap();
        assert_eq!(session.target, Address::Domain("eih.example".to_owned()));
        assert_eq!(session.port, 443);
        assert_eq!(session.auth.unwrap().policy_revision, Some(7));
    }
}

#[cfg(feature = "blake3")]
#[tokio::test]
async fn sip023_tcp_password_chain_interops_with_the_eih_acceptor() {
    for cipher in [
        shadowsocks::CipherKind::Blake3Aes128Gcm,
        shadowsocks::CipherKind::Blake3Aes256Gcm,
    ] {
        let (identity_password, user_password) = eih_keys(cipher);
        let profile = ShadowsocksInboundProfile::from_config_users_with_identity(
            match cipher {
                shadowsocks::CipherKind::Blake3Aes128Gcm => "2022-blake3-aes-128-gcm",
                shadowsocks::CipherKind::Blake3Aes256Gcm => "2022-blake3-aes-256-gcm",
                _ => unreachable!(),
            },
            Some(identity_password),
            [eih_user(cipher, user_password)],
        )
        .unwrap();
        let acceptor = ShadowsocksInboundTcpAcceptor::new(profile);
        let (client, server) = duplex(8192);
        let (mut client, server) = (TestSocket(client), TestSocket(server));
        let target = Session::new(
            0,
            Address::Domain("chain.eih.example".to_owned()),
            8443,
            Network::Tcp,
            ProtocolType::new("shadowsocks"),
        );
        let password_chain = format!("{identity_password}:{user_password}");
        ShadowsocksOutbound
            .send_request(&mut client, &target, cipher, password_chain.as_bytes())
            .await
            .unwrap();

        let (session, _stream) = acceptor.accept_stream(server).await.unwrap();
        assert_eq!(session.target, target.target);
        assert_eq!(session.port, target.port);
        assert_eq!(
            session.auth.unwrap().principal_key.as_deref(),
            Some("account:eih-user")
        );
    }
}

#[cfg(feature = "blake3")]
#[tokio::test]
async fn sip023_tcp_response_uses_the_selected_user_psk() {
    for cipher in [
        shadowsocks::CipherKind::Blake3Aes128Gcm,
        shadowsocks::CipherKind::Blake3Aes256Gcm,
    ] {
        let (identity_password, user_password) = eih_keys(cipher);
        let cipher_name = match cipher {
            shadowsocks::CipherKind::Blake3Aes128Gcm => "2022-blake3-aes-128-gcm",
            shadowsocks::CipherKind::Blake3Aes256Gcm => "2022-blake3-aes-256-gcm",
            _ => unreachable!(),
        };
        let profile = ShadowsocksInboundProfile::from_config_users_with_identity(
            cipher_name,
            Some(identity_password),
            [eih_user(cipher, user_password)],
        )
        .unwrap();
        let acceptor = ShadowsocksInboundTcpAcceptor::new(profile);
        let password_chain = format!("{identity_password}:{user_password}");
        let connect = shadowsocks::tcp_connect_config_from_config(cipher_name, &password_chain)
            .expect("EIH connect config");
        let (client, server) = duplex(8192);
        let (mut client, server) = (TestSocket(client), TestSocket(server));
        let target = Session::new(
            0,
            Address::Domain("response.eih.example".to_owned()),
            443,
            Network::Tcp,
            ProtocolType::new("shadowsocks"),
        );
        let outbound_session = connect
            .establish_tcp_session(&mut client, &target)
            .await
            .unwrap();
        let (_session, mut server_stream) = acceptor.accept_stream(server).await.unwrap();
        let mut client_stream = connect.wrap_outbound_stream(client, outbound_session);

        tokio::io::AsyncWriteExt::write_all(&mut server_stream, b"server-response")
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::flush(&mut server_stream)
            .await
            .unwrap();
        let mut response = [0_u8; 15];
        tokio::io::AsyncReadExt::read_exact(&mut client_stream, &mut response)
            .await
            .unwrap();
        assert_eq!(&response, b"server-response");
    }
}

#[cfg(feature = "blake3")]
#[test]
fn sip023_udp_eih_selects_the_user_psk_for_both_aes_methods() {
    for cipher in [
        shadowsocks::CipherKind::Blake3Aes128Gcm,
        shadowsocks::CipherKind::Blake3Aes256Gcm,
    ] {
        let (identity_password, user_password) = eih_keys(cipher);
        let profile = ShadowsocksInboundProfile::from_config_users_with_identity(
            match cipher {
                shadowsocks::CipherKind::Blake3Aes128Gcm => "2022-blake3-aes-128-gcm",
                shadowsocks::CipherKind::Blake3Aes256Gcm => "2022-blake3-aes-256-gcm",
                _ => unreachable!(),
            },
            Some(identity_password),
            [eih_user(cipher, user_password)],
        )
        .unwrap();
        let mut responder = ShadowsocksInboundUdpResponder::from_profile(profile);
        let datagram = encode_udp_eih_request(
            cipher,
            identity_password,
            user_password,
            &Address::Domain("dns.eih.example".to_owned()),
            53,
        );

        let dispatch = responder.decode_inbound_dispatch(&datagram).unwrap();
        assert_eq!(dispatch.port(), 53);
        let auth = <ShadowsocksInboundUdpResponder as DatagramUdpResponder<
            Arc<tokio::net::UdpSocket>,
        >>::auth(&responder)
        .unwrap();
        assert_eq!(auth.principal_key.as_deref(), Some("account:eih-user"));
        assert_eq!(auth.policy_revision, Some(7));
    }
}

#[cfg(feature = "blake3")]
#[test]
fn sip023_udp_password_chain_interops_with_the_eih_responder() {
    for cipher in [
        shadowsocks::CipherKind::Blake3Aes128Gcm,
        shadowsocks::CipherKind::Blake3Aes256Gcm,
    ] {
        let (identity_password, user_password) = eih_keys(cipher);
        let profile = ShadowsocksInboundProfile::from_config_users_with_identity(
            match cipher {
                shadowsocks::CipherKind::Blake3Aes128Gcm => "2022-blake3-aes-128-gcm",
                shadowsocks::CipherKind::Blake3Aes256Gcm => "2022-blake3-aes-256-gcm",
                _ => unreachable!(),
            },
            Some(identity_password),
            [eih_user(cipher, user_password)],
        )
        .unwrap();
        let mut responder = ShadowsocksInboundUdpResponder::from_profile(profile);
        let password_chain = format!("{identity_password}:{user_password}");
        let codec = ShadowsocksDatagramCodec {
            cipher,
            password: password_chain.into_bytes(),
        };
        let datagram = codec
            .encode(
                &Address::Domain("chain.dns.eih.example".to_owned()),
                53,
                b"query",
            )
            .unwrap();

        let dispatch = responder.decode_inbound_dispatch(&datagram).unwrap();
        assert_eq!(dispatch.port(), 53);
        let auth = <ShadowsocksInboundUdpResponder as DatagramUdpResponder<
            Arc<tokio::net::UdpSocket>,
        >>::auth(&responder)
        .unwrap();
        assert_eq!(auth.principal_key.as_deref(), Some("account:eih-user"));
    }
}

#[cfg(feature = "blake3")]
#[test]
fn sip023_udp_response_uses_the_selected_user_psk() {
    for cipher in [
        shadowsocks::CipherKind::Blake3Aes128Gcm,
        shadowsocks::CipherKind::Blake3Aes256Gcm,
    ] {
        let (identity_password, user_password) = eih_keys(cipher);
        let password_chain = format!("{identity_password}:{user_password}");
        let outbound = ShadowsocksDatagramCodec {
            cipher,
            password: password_chain.into_bytes(),
        };
        let request = outbound
            .encode(
                &Address::Domain("request.eih.example".to_owned()),
                53,
                b"query",
            )
            .unwrap();
        let mut inbound = ShadowsocksInboundUdpCodec::new_eih(
            cipher,
            identity_password.as_bytes(),
            user_password.as_bytes(),
        );
        let decoded_request = inbound.decode_request(&request).unwrap();
        let response = inbound
            .encode_response(
                decoded_request.client_session_id(),
                &Address::Domain("answer.eih.example".to_owned()),
                53,
                b"answer",
            )
            .unwrap();
        let (target, port, payload) = outbound.decode(&response).unwrap();
        assert_eq!(target, Address::Domain("answer.eih.example".to_owned()));
        assert_eq!(port, 53);
        assert_eq!(payload, b"answer");
    }
}
