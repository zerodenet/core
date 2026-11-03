//! Real mKCP, optional TLS and REALITY traffic under standard source ownership.
use super::*;
use zero_platform_tokio::TcpRelayStream;
const USER: &str = "11111111-2222-3333-4444-555555555555";
#[tokio::test]
async fn vless_mkcp_standard_source_routes_tcp_udp_and_confirms_originals_before_restart() {
    timeout(Duration::from_secs(30), exercise(false, false))
        .await
        .unwrap();
}
#[tokio::test]
async fn vless_mkcp_tls_standard_source_routes_tcp_udp_and_confirms_originals_before_restart() {
    timeout(Duration::from_secs(30), exercise(false, true))
        .await
        .unwrap();
}
#[tokio::test]
async fn vless_reality_standard_source_routes_tcp_udp_and_confirms_originals_before_restart() {
    timeout(Duration::from_secs(30), exercise(true, false))
        .await
        .unwrap();
}
fn request(command: u8, port: u16) -> Vec<u8> {
    let mut b = vec![0];
    b.extend(vless::parse_uuid(USER).unwrap());
    b.extend([0, command]);
    b.extend(port.to_be_bytes());
    b.extend([1, 127, 0, 0, 1]);
    b
}
async fn open(
    address: std::net::SocketAddr,
    reality: Option<&str>,
    tls: Option<&std::path::Path>,
) -> TcpRelayStream {
    if let Some(public) = reality {
        let io = TcpStream::connect(address).await.unwrap();
        let io = vless::reality::upgrade_reality_client(
            io,
            vless::reality::RealityClientOptions {
                spider_x: "/",
                hybrid_key_exchange: false,
                mldsa65_verify: None,
                public_key: public,
                short_id: "0123456789abcdef",
                server_name: "localhost",
                cipher_suites: &[],
                client_fingerprint: "chrome",
            },
        )
        .await
        .unwrap();
        return TcpRelayStream::new(io);
    }
    let socket = zero_transport::OutboundDatagramSocketFactory::new(Default::default());
    let io = zero_transport::mkcp::connect(
        &address.ip().to_string(),
        address.port(),
        zero_transport::mkcp::Settings::default(),
        &socket,
    )
    .await
    .unwrap();
    if let Some(cert) = tls {
        let profile = zero_transport::profile::OwnedClientTlsProfile {
            options: Default::default(),
            server_name: Some("localhost".into()),
            disable_sni: false,
            ca_cert_path: Some(cert.to_string_lossy().into_owned()),
            insecure: false,
            alpn: vec![],
            client_fingerprint: None,
        };
        return TcpRelayStream::new(
            zero_transport::tls::connect_tls_stream_with_profile(io, &profile, None, "localhost")
                .await
                .unwrap(),
        );
    }
    TcpRelayStream::new(io)
}
async fn exercise(reality: bool, tls: bool) {
    let material = tempfile::tempdir().unwrap();
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert = material.path().join("ca.pem");
    let key = material.path().join("key.pem");
    std::fs::write(&cert, certificate.cert.pem()).unwrap();
    std::fs::write(&key, certificate.signing_key.serialize_pem()).unwrap();
    let (private, public) = vless::reality::generate_reality_key_pair();
    let mut protocol = serde_json::json!({"type":"vless","users":[{"id":USER}]});
    if reality {
        protocol["reality"] = serde_json::json!({"private_key":private,"short_ids":["0123456789abcdef"],"server_name":"localhost"});
    } else {
        protocol["mkcp"] = serde_json::json!({});
    }
    if tls {
        protocol["tls"] = serde_json::json!({"cert_path":cert,"key_path":key});
    }
    let config = RuntimeConfig::parse(
        &serde_json::json!({"inbounds":[{"tag":"first",
        "listen":{"address":"127.0.0.1","port":0},"protocol":protocol}],
        "route":{"final":{"type":"direct"}}})
        .to_string(),
    )
    .unwrap();
    let proxy = Proxy::new(config).unwrap();
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
    let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let udp = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut previous = None;
    for generation in 0..2 {
        let mut inbound = proxy.engine().config().inbounds[0].clone();
        if let Some(port) = previous {
            inbound.listen.port = port;
        }
        let listener = crate::runtime::test_listener::start(&proxy, inbound).await;
        previous = Some(listener.address.port());
        let original = listener.owner.clone();
        let mut stream = open(
            listener.address,
            reality.then_some(public.as_str()),
            tls.then_some(cert.as_path()),
        )
        .await;
        let mut header = request(1, echo.local_addr().unwrap().port());
        header.extend(b"real");
        stream.write_all(&header).await.unwrap();
        stream.flush().await.unwrap();
        let mut target = echo.accept().await.unwrap().0;
        let mut bytes = [0; 4];
        target.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"real");
        target.write_all(b"back").await.unwrap();
        let mut reply = [0; 6];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"\0\0back");
        let mut datagram = open(
            listener.address,
            reality.then_some(public.as_str()),
            tls.then_some(cert.as_path()),
        )
        .await;
        let mut header = request(2, udp.local_addr().unwrap().port());
        header.extend(b"\0\x03udp");
        datagram.write_all(&header).await.unwrap();
        datagram.flush().await.unwrap();
        let mut packet = [0; 64];
        let (n, sender) = udp.recv_from(&mut packet).await.unwrap();
        assert_eq!(&packet[..n], b"udp");
        udp.send_to(b"answer", sender).await.unwrap();
        let mut reply = [0; 10];
        datagram.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"\0\0\0\x06answer");
        let mut byte = [0];
        let mut waiting = Box::pin(target.read(&mut byte));
        std::future::poll_fn(|cx| {
            assert!(waiting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        // Use the same host revocation entry as real replacement; a listener
        // watch notification alone does not revoke native peer custody.
        listener.owner.task_access().request_stop();
        listener.stop.send_replace(true);
        drop(stream);
        drop(datagram);
        crate::runtime::test_listener::close(&proxy, "first", listener).await;
        assert_eq!(
            timeout(Duration::from_secs(3), waiting)
                .await
                .expect("host stop must cancel actual egress I/O")
                .unwrap(),
            0,
            "wire observation, not the original native receipt"
        );
        let terminal = original.terminal().unwrap();
        assert!(terminal.joined_tasks() >= 4);
        assert!(terminal.closed_io_receipts() >= 3);
        assert!(proxy.engine().active_sessions().is_empty());
        assert_eq!(
            proxy.engine().completed_sessions().len(),
            (generation + 1) * 2
        );
        let stats = proxy.engine().stats_snapshot();
        assert!(stats.bytes_up >= 7 && stats.bytes_down >= 10);
    }
}
