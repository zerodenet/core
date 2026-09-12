#![cfg(all(feature = "runtime", feature = "blake3"))]
#[path = "support/socket.rs"]
mod socket;
use socket::Socket;
use zero_core::{Address, Network, ProtocolType, Session};
fn target() -> Session {
    Session::new(
        0,
        Address::Domain("example.com".into()),
        443,
        Network::Tcp,
        ProtocolType::new("audit"),
    )
}

#[tokio::test]
async fn variable_header_fragmentation_is_accepted() {
    let password = b"MDEyMzQ1Njc4OWFiY2RlZg==";
    let mut socket = Socket::new(vec![]);
    shadowsocks::ShadowsocksOutbound
        .send_request(
            &mut socket,
            &target(),
            shadowsocks::CipherKind::Blake3Aes128Gcm,
            password,
        )
        .await
        .unwrap();
    let wire = socket.output.lock().unwrap().clone();
    let profile = shadowsocks::ShadowsocksInboundProfile::from_config_users(
        "2022-blake3-aes-128-gcm",
        [shadowsocks::transport::ShadowsocksInboundUserRef {
            password: std::str::from_utf8(password).unwrap(),
            principal_key: None,
            up_bps: None,
            down_bps: None,
            device_limit: None,
            quota_remaining_bytes: None,
            policy_revision: None,
        }],
    )
    .unwrap();
    let acceptor = shadowsocks::ShadowsocksInboundTcpAcceptor::new(profile);
    let mut fragmented = Socket::new(wire.clone());
    fragmented.fragment = true;
    assert!(acceptor.accept_stream(fragmented).await.is_ok());
}
#[tokio::test(start_paused = true)]
async fn drain_uses_one_total_deadline_despite_continued_reads() {
    let profile = shadowsocks::ShadowsocksInboundProfile::from_config_users(
        "2022-blake3-aes-128-gcm",
        [shadowsocks::transport::ShadowsocksInboundUserRef {
            password: "MDEyMzQ1Njc4OWFiY2RlZg==",
            principal_key: None,
            up_bps: None,
            down_bps: None,
            device_limit: None,
            quota_remaining_bytes: None,
            policy_revision: None,
        }],
    )
    .unwrap();
    let acceptor = shadowsocks::ShadowsocksInboundTcpAcceptor::new(profile);
    let mut socket = Socket::new(vec![0; 20_000]);
    socket.slow = true;
    let start = tokio::time::Instant::now();
    assert!(acceptor.accept_stream(socket).await.is_err());
    let elapsed = start.elapsed();
    println!("failed handshake with one read per second completed after {elapsed:?}");
    assert!(elapsed.as_secs() <= 3);
}
