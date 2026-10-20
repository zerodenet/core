use super::*;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn ordinary_socks5_udp_uses_standard_session_domain_zero_payload_and_confirmed_normal_retirement(
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let relay = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let relay_endpoint = relay.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let observed = seen.clone();
    let native_source = Arc::new(Mutex::new(None));
    let source = native_source.clone();
    let eof = Arc::new(tokio::sync::Notify::new());
    let peer_eof = eof.clone();
    let release = Arc::new(tokio::sync::Notify::new());
    let peer_release = release.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut control = zero_platform_tokio::TokioSocket::new(stream);
        assert!(matches!(
            ::socks5::Socks5Inbound
                .accept_command(&mut control)
                .await
                .unwrap(),
            ::socks5::Socks5Request::UdpAssociate(_)
        ));
        ::socks5::Socks5Inbound
            .send_success_response_with_bound(
                &mut control,
                &Address::Ipv4([127, 0, 0, 1]),
                relay_endpoint.port(),
            )
            .await
            .unwrap();
        let codec = ::socks5::udp::Socks5InboundUdpCodec;
        let mut bytes = [0; 65535];
        for _ in 0..2 {
            let (size, peer) = relay.recv_from(&mut bytes).await.unwrap();
            let previous = *source.lock().unwrap();
            if let Some(previous) = previous {
                assert_eq!(peer, previous);
            } else {
                *source.lock().unwrap() = Some(peer);
            }
            let frame = codec.decode_request(&bytes[..size]).unwrap();
            observed.lock().unwrap().push((
                frame.target().clone(),
                frame.port(),
                frame.payload().to_vec(),
            ));
            let response = codec
                .encode_response_to_client(frame.target(), frame.port(), frame.payload())
                .unwrap();
            relay.send_to(&response, peer).await.unwrap();
        }
        let mut control = control.into_inner();
        let mut extra = Vec::new();
        control.read_to_end(&mut extra).await.unwrap();
        assert!(extra.is_empty());
        peer_eof.notify_one();
        peer_release.notified().await;
        control.shutdown().await.unwrap();
    });
    let outer = free_port();
    let config=RuntimeConfig::parse(&format!(r#"{{
        "runtime":{{"udp_upstream_idle_timeout_seconds":1}},
        "inbounds":[{{"tag":"incoming","listen":{{"address":"127.0.0.1","port":{outer}}},"protocol":{{"type":"socks5"}}}}],
        "outbounds":[{{"tag":"selected","protocol":{{"type":"socks5","server":"127.0.0.1","port":{}}}}}],
        "route":{{"final":{{"type":"route","outbound":"selected"}}}}
    }}"#,endpoint.port())).unwrap();
    let proxy = Engine::new(config).unwrap();
    let probe = proxy.clone();
    let running = spawn_engine(proxy);
    wait_for_listener(outer).await;
    let mut control = TcpStream::connect(("127.0.0.1", outer)).await.unwrap();
    control.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    control.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    control
        .write_all(&[5, 3, 0, 1, 0, 0, 0, 0, 0, 0])
        .await
        .unwrap();
    let mut associated = [0; 10];
    control.read_exact(&mut associated).await.unwrap();
    assert_eq!(associated[1], 0);
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = u16::from_be_bytes([associated[8], associated[9]]);
    let packets = [
        (
            Address::Domain("wire.example".into()),
            5353,
            b"domain".as_slice(),
        ),
        (Address::Ipv4([203, 0, 113, 7]), 443, &[]),
    ];
    for (target, target_port, payload) in &packets {
        let wire = build_udp_packet(target, *target_port, payload).unwrap();
        client.send_to(&wire, ("127.0.0.1", port)).await.unwrap();
        let mut bytes = [0; 2048];
        let (size, _) = timeout(Duration::from_secs(3), client.recv_from(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        let response = parse_udp_packet(&bytes[..size]).unwrap();
        assert_eq!(&response.target, target);
        assert_eq!(response.port, *target_port);
        assert_eq!(response.payload.as_slice(), *payload);
    }
    let actual = seen.lock().unwrap().clone();
    assert_eq!(
        actual,
        packets
            .iter()
            .map(|(target, port, payload)| (target.clone(), *port, payload.to_vec()))
            .collect::<Vec<_>>()
    );
    timeout(Duration::from_secs(3), eof.notified())
        .await
        .unwrap();
    let pending = probe.stats_snapshot();
    assert_eq!(pending.udp_upstream.created_associations, 1);
    assert_eq!(pending.udp_upstream.reused_associations, 1);
    assert_eq!(pending.udp_upstream.active_associations, 1);
    assert_eq!(pending.udp_upstream.closed_associations, 0);
    assert_eq!(pending.udp_upstream.idle_timeouts, 0);
    // The client association stays active. Only normal retirement progress,
    // followed by the real remote EOF, can complete this upstream lifecycle.
    release.notify_one();
    support::wait_for("normal Datagram retirement confirmation", || {
        probe.stats_snapshot().udp_upstream.idle_timeouts == 1
    })
    .await;
    let complete = probe.stats_snapshot();
    assert_eq!(complete.udp_upstream.active_associations, 0);
    assert_eq!(complete.udp_upstream.closed_associations, 0);
    assert_eq!(complete.udp_upstream.packets_sent, 2);
    assert_eq!(complete.udp_upstream.packets_received, 2);
    let local = native_source.lock().unwrap().unwrap();
    UdpSocket::bind(local).await.unwrap();
    drop(control);
    running.shutdown().await.unwrap();
    timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}
