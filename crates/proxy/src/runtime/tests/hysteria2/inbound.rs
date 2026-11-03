//! Real standard sources exercise actual caller tasks and original close receipts.
use crate::runtime::test_listener;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
    time::{timeout, Duration, Instant},
};
use zero_core::{Address, Network, ProtocolType, Session};
pub(super) fn material() -> (tempfile::TempDir, serde_json::Value) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "zero_proxy=debug,zero_transport=debug,hysteria2=debug".into()),
        )
        .with_test_writer()
        .try_init();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let dir = tempfile::tempdir().unwrap();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate = dir.path().join("cert.pem");
    let key = dir.path().join("key.pem");
    std::fs::write(&certificate, cert.cert.pem()).unwrap();
    std::fs::write(&key, cert.signing_key.serialize_pem()).unwrap();
    let config = serde_json::json!({"inbounds":[{"tag":"hy","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"hysteria2","password":"secret","cert_path":certificate,"key_path":key}}],"route":{"final":{"type":"direct"}}});
    (dir, config)
}
pub(super) async fn connect(
    address: std::net::SocketAddr,
    alpn: &[u8],
) -> (quinn::Endpoint, quinn::Connection) {
    let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client.set_default_client_config(
        zero_transport::quic::client_config(true, None, &[alpn.to_vec()], Some(65536)).unwrap(),
    );
    let conn = client.connect(address, "localhost").unwrap().await.unwrap();
    (client, conn)
}
pub(super) async fn ready_proxy(value: &serde_json::Value) -> crate::Proxy {
    let proxy =
        crate::Proxy::new(zero_config::RuntimeConfig::parse(&value.to_string()).unwrap()).unwrap();
    let config = proxy.engine().config();
    proxy
        .protocols
        .start_components(config.clone(), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    proxy
        .protocols
        .commit_components(&config, || Ok(()))
        .unwrap();
    proxy
}
async fn exercise(legacy: bool) {
    let (_dir, value) = material();
    let proxy = ready_proxy(&value).await;
    let config = proxy.engine().config();
    let input = config.inbounds[0].clone();
    let running = test_listener::start(&proxy, input.clone()).await;
    eprintln!("standard HY2 ready {}", running.address);
    // An incompatible handshake and an abandoned connection cannot stop admission.
    let mut invalid = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    invalid.set_default_client_config(
        zero_transport::quic::client_config(true, None, &[b"wrong".to_vec()], None).unwrap(),
    );
    assert!(invalid
        .connect(running.address, "localhost")
        .unwrap()
        .await
        .is_err());
    invalid.close(0u32.into(), b"");
    eprintln!("incompatible ALPN rejected");
    invalid.wait_idle().await;
    eprintln!("incompatible client idle");
    let (client, connection) =
        connect(running.address, if legacy { b"hysteria2" } else { b"h3" }).await;
    eprintln!("valid QUIC connected");
    let mut guard = None;
    if legacy {
        let (send, recv) = connection.open_bi().await.unwrap();
        let mut auth = ::hysteria2::transport::Hysteria2Stream::new(send, recv);
        ::hysteria2::Hysteria2Outbound
            .authenticate_connection(&connection, &mut auth, "secret")
            .await
            .unwrap();
    } else {
        let (mut driver, mut sender) =
            h3::client::new(h3_quinn::Connection::new(connection.clone()))
                .await
                .unwrap();
        let worker =
            tokio::spawn(async move { std::future::poll_fn(|cx| driver.poll_close(cx)).await });
        ::hysteria2::transport::authenticate_http3_request(
            &mut sender,
            "secret",
            Default::default(),
        )
        .await
        .unwrap();
        guard = Some((sender, worker));
    }

    eprintln!("protocol authenticated");
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = target.local_addr().unwrap().port();
    let (send, recv) = connection.open_bi().await.unwrap();
    let mut stream = ::hysteria2::transport::Hysteria2Stream::new(send, recv);
    let session = Session::new(
        1,
        Address::Ipv4([127, 0, 0, 1]),
        port,
        Network::Tcp,
        ProtocolType::new("test"),
    );
    ::hysteria2::Hysteria2Outbound
        .establish_tcp_connect(&mut stream, &session)
        .await
        .unwrap();
    let (mut peer, _) = target.accept().await.unwrap();
    stream.write_all(b"ping").await.unwrap();
    let mut bytes = [0; 4];
    peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"ping");
    peer.write_all(b"pong").await.unwrap();
    stream.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"pong");
    eprintln!("TCP roundtrip complete");
    let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp_port = udp.local_addr().unwrap().port();
    let codec = ::hysteria2::udp::Hysteria2InboundUdpCodec;
    connection
        .send_datagram(
            codec
                .encode_datagram(71, &Address::Ipv4([127, 0, 0, 1]), udp_port, b"udp")
                .unwrap()
                .into(),
        )
        .unwrap();
    let mut buffer = [0; 100];
    let (size, sender) = udp.recv_from(&mut buffer).await.unwrap();
    assert_eq!(&buffer[..size], b"udp");
    udp.send_to(b"reply", sender).await.unwrap();
    let response = connection.read_datagram().await.unwrap();
    let packet = codec.decode_datagram(&response).unwrap();
    assert_eq!(packet.session_id(), 71);
    assert_eq!(packet.payload(), b"reply");
    eprintln!("UDP roundtrip complete");
    let stats = proxy.engine().stats_snapshot();
    assert_eq!(
        stats.total_started, 2,
        "auth and idle carriers must not invent business flows"
    );
    assert!(
        stats.bytes_up >= 7 && stats.bytes_down >= 9,
        "actual TCP/UDP accounting: {stats:?}"
    );
    // Close the real host while stream/datagram handles are still live.
    let owner = running.owner.clone();
    owner.task_access().request_stop();
    running.stop.send_replace(true);
    running.worker.await.unwrap().unwrap();
    eprintln!("host tasks joined");
    let receipt = proxy
        .inbound_owners
        .confirm_shutdown("hy", Instant::now() + Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap();
    assert!(
        receipt.terminal().failure().is_none(),
        "{:?}",
        receipt.terminal().failure()
    );
    assert!(receipt.terminal().closed_io_receipts() > 0);
    assert!(receipt.terminal().joined_tasks() > 1);
    assert!(proxy.inbound_owners.open_bind_admission().is_err());
    assert_eq!(
        peer.read(&mut bytes).await.unwrap(),
        0,
        "actual upstream original retired"
    );
    proxy
        .inbound_owners
        .release_closed(receipt, |_| Ok(()))
        .unwrap();
    drop(stream);
    if let Some((sender, worker)) = guard {
        drop(sender);
        worker.abort();
        let _ = worker.await;
    }
    connection.close(0u32.into(), b"");
    client.close(0u32.into(), b"");
    client.wait_idle().await;
    // Only the exact receipt above permits a replacement source on the same endpoint.
    let mut same = input;
    same.listen.port = running.address.port();
    let next = test_listener::start(&proxy, same).await;
    test_listener::close(&proxy, "hy", next).await;
    proxy
        .protocols
        .shutdown_components(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
}
#[tokio::test]
async fn standard_h3_tcp_udp_and_original_shutdown_replace() {
    timeout(Duration::from_secs(20), exercise(false))
        .await
        .unwrap();
}
#[tokio::test]
async fn standard_legacy_alpn_tcp_udp_and_original_shutdown_replace() {
    timeout(Duration::from_secs(20), exercise(true))
        .await
        .unwrap();
}
