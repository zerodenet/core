#![cfg(feature = "runtime")]
#[path = "support/socket.rs"]
mod socket;
use shadowsocks::udp::{ShadowsocksDatagramCodec, ShadowsocksInboundUdpCodec};
use shadowsocks::{
    validation::ReplayPolicy, CipherKind, ShadowsocksInboundProfile, ShadowsocksInboundTcpAcceptor,
    ShadowsocksTcpConnectConfig,
};
use socket::Socket;
use tokio::io::AsyncReadExt;
use zero_core::{Address, Network, ProtocolType, Session};
use zero_traits::DatagramCodec;
const PASSWORD: &[u8] = b"replay-test";
fn target() -> Address {
    Address::Domain("example.com".into())
}

#[test]
fn udp_legacy_policy_checks_authenticated_packets_in_both_directions() {
    for policy in [
        ReplayPolicy::Default,
        ReplayPolicy::Ignore,
        ReplayPolicy::Detect,
        ReplayPolicy::Reject,
    ] {
        let sender = ShadowsocksDatagramCodec::new(CipherKind::Aes128Gcm, PASSWORD);
        let packet = sender.encode(&target(), 53, b"query").unwrap();
        let mut bad = packet.clone();
        *bad.last_mut().unwrap() ^= 1;
        let mut server = ShadowsocksInboundUdpCodec::new(CipherKind::Aes128Gcm, PASSWORD)
            .with_replay_policy(policy);
        let client = sender.with_replay_policy(policy);
        assert!(server.decode_request(&bad).is_err());
        assert!(client.decode(&bad).is_none());
        assert!(server.decode_request(&packet).is_ok());
        assert!(client.decode(&packet).is_some());
        assert_eq!(
            server.decode_request(&packet).is_ok(),
            policy != ReplayPolicy::Reject
        );
        assert_eq!(
            client.clone().decode(&packet).is_some(),
            policy != ReplayPolicy::Reject
        );
    }
}

#[tokio::test]
async fn tcp_legacy_policy_is_shared_across_connections_and_checks_response_salts() {
    for policy in [
        ReplayPolicy::Default,
        ReplayPolicy::Ignore,
        ReplayPolicy::Detect,
        ReplayPolicy::Reject,
    ] {
        let config = ShadowsocksTcpConnectConfig::from_config("aes-128-gcm", "replay-test")
            .unwrap()
            .with_replay_policy(policy);
        let session = Session::new(
            0,
            target(),
            443,
            Network::Tcp,
            ProtocolType::new("shadowsocks"),
        );
        let mut request = Socket::new(vec![]);
        config
            .establish_tcp_session(&mut request, &session)
            .await
            .unwrap();
        let wire = request.output.lock().unwrap().clone();
        let profile =
            ShadowsocksInboundProfile::from_config_cipher_password("aes-128-gcm", "replay-test")
                .unwrap()
                .with_replay_policy(policy);
        let acceptor = ShadowsocksInboundTcpAcceptor::new(profile);
        assert!(acceptor
            .accept_stream(Socket::new(wire.clone()))
            .await
            .is_ok());
        assert_eq!(
            acceptor.accept_stream(Socket::new(wire)).await.is_ok(),
            policy != ReplayPolicy::Reject
        );
        let salt = vec![7; 16];
        let key = shadowsocks::derive_download_key(CipherKind::Aes128Gcm, PASSWORD, &salt).unwrap();
        let response = [
            salt,
            shadowsocks::encrypt_tcp_chunk(CipherKind::Aes128Gcm, &key, &mut 0, b"reply").unwrap(),
        ]
        .concat();
        for attempt in 0..2 {
            let mut sink = Socket::new(vec![]);
            let established = config
                .establish_tcp_session(&mut sink, &session)
                .await
                .unwrap();
            let mut stream =
                config.wrap_outbound_stream(Socket::new(response.clone()), established);
            let mut result = [0; 5];
            assert_eq!(
                stream.read_exact(&mut result).await.is_ok(),
                attempt == 0 || policy != ReplayPolicy::Reject
            );
        }
    }
}

#[test]
fn truncated_domain_length_is_an_error() {
    assert!(shadowsocks::decode_address(&[3]).is_err());
}

#[cfg(feature = "blake3")]
#[tokio::test]
async fn tcp2022_rejects_reused_server_salt_even_with_a_fresh_request_and_ignore_policy() {
    let password = b"MDEyMzQ1Njc4OWFiY2RlZg==";
    let cipher = CipherKind::Blake3Aes128Gcm;
    let config = ShadowsocksTcpConnectConfig::from_config(
        cipher.name(),
        core::str::from_utf8(password).unwrap(),
    )
    .unwrap()
    .with_replay_policy(ReplayPolicy::Ignore);
    let target = Session::new(
        0,
        target(),
        443,
        Network::Tcp,
        ProtocolType::new("shadowsocks"),
    );
    for attempt in 0..2 {
        let mut sink = Socket::new(vec![]);
        let session = config
            .establish_tcp_session(&mut sink, &target)
            .await
            .unwrap();
        let salt = vec![0x51; 16];
        let key = shadowsocks::derive_download_key(cipher, password, &salt).unwrap();
        let header = shadowsocks::build_2022_response_fixed_header(
            shadowsocks::now_unix_seconds(),
            &session.request_salt,
            5,
        )
        .unwrap();
        let mut nonce = 0;
        let response = [
            salt,
            shadowsocks::encrypt_tcp_2022_single_chunk(cipher, &key, &mut nonce, &header).unwrap(),
            shadowsocks::encrypt_tcp_2022_single_chunk(cipher, &key, &mut nonce, b"reply").unwrap(),
        ]
        .concat();
        let mut stream = config.wrap_outbound_stream(Socket::new(response), session);
        let mut bytes = [0; 5];
        assert_eq!(stream.read_exact(&mut bytes).await.is_ok(), attempt == 0);
    }
}

#[tokio::test]
async fn legacy_tcp_target_may_span_authenticated_chunks() {
    let cipher = CipherKind::Aes128Gcm;
    let salt = vec![0x73; 16];
    let key = shadowsocks::derive_session_key(cipher, PASSWORD, &salt).unwrap();
    let address = shadowsocks::build_target_data(&target(), 443, b"initial").unwrap();
    let mut nonce = 0;
    let mut wire = salt;
    for part in address.chunks(1) {
        wire.extend_from_slice(
            &shadowsocks::encrypt_tcp_chunk(cipher, &key, &mut nonce, part).unwrap(),
        );
    }
    let profile = ShadowsocksInboundProfile::from_config_cipher_password(
        cipher.name(),
        core::str::from_utf8(PASSWORD).unwrap(),
    )
    .unwrap();
    let (session, mut stream) = ShadowsocksInboundTcpAcceptor::new(profile)
        .accept_stream(Socket::new(wire))
        .await
        .unwrap();
    assert_eq!(session.target, target());
    assert_eq!(session.port, 443);
    let mut payload = Vec::new();
    stream.read_to_end(&mut payload).await.unwrap();
    assert_eq!(payload, b"initial");
}

#[tokio::test]
async fn reject_policy_remembers_locally_generated_legacy_salts() {
    let cipher = CipherKind::Aes128Gcm;
    let config = ShadowsocksTcpConnectConfig::from_config(
        cipher.name(),
        core::str::from_utf8(PASSWORD).unwrap(),
    )
    .unwrap()
    .with_replay_policy(ReplayPolicy::Reject);
    let target = Session::new(
        0,
        target(),
        443,
        Network::Tcp,
        ProtocolType::new("shadowsocks"),
    );
    let mut sink = Socket::new(vec![]);
    let session = config
        .establish_tcp_session(&mut sink, &target)
        .await
        .unwrap();
    let key = shadowsocks::derive_session_key(cipher, PASSWORD, &session.request_salt).unwrap();
    let response = [
        session.request_salt.clone(),
        shadowsocks::encrypt_tcp_chunk(cipher, &key, &mut 0, b"reply").unwrap(),
    ]
    .concat();
    let mut stream = config.wrap_outbound_stream(Socket::new(response), session);
    let mut bytes = [0; 5];
    assert!(stream.read_exact(&mut bytes).await.is_err());
}
