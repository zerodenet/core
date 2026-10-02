use super::*;
use crate::host;
use tokio::net::TcpListener;

#[tokio::test]
async fn bidirectional_resources_attribute_inbound_peer_flow_and_packet_roles() {
    let host = host::non_loopback_host_ipv4();
    let a_port = support::free_udp_port();
    let b_port = support::free_udp_port();
    let a_socks = support::free_port();
    let b_socks = support::free_port();
    let config = |seed: u8, remote_seed: u8, port: u16, remote_port: u16, socks: u16| {
        RuntimeConfig::parse(&serde_json::json!({
        "inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":socks},"protocol":{"type":"socks5"}}],
        "endpoints":[{"tag":"wg","directions":{"inbound":true,"outbound":true},"listen":{"address":"127.0.0.1","port":port},"protocol":{"type":"wireguard","private_key":STANDARD.encode([seed;32]),"addresses":[format!("10.0.0.{}/32",seed-130)],"peers":[{"public_key":peer::public(remote_seed),"endpoint":format!("127.0.0.1:{remote_port}"),"allowed_ips":["0.0.0.0/0"]}]}}],
        "route":{"rules":[{"condition":{"type":"inbound","values":["socks"]},"action":{"type":"route","outbound":"wg"}}],"final":{"type":"direct"}}
    }).to_string()).unwrap()
    };
    let a = Proxy::new(config(131, 132, a_port, b_port, a_socks)).unwrap();
    let b = Proxy::new(config(132, 131, b_port, a_port, b_socks)).unwrap();
    let a_running = support::spawn_engine(a.clone());
    let b_running = support::spawn_engine(b.clone());
    support::wait_for_listener(a_socks).await;
    support::wait_for_listener(b_socks).await;
    let socket = UdpSocket::bind((host, 0)).await.unwrap();
    let port = socket.local_addr().unwrap().port();
    let echo = tokio::spawn(async move {
        let mut data = [0; 2048];
        for _ in 0..2 {
            let (n, source) = socket.recv_from(&mut data).await.unwrap();
            socket.send_to(&data[..n], source).await.unwrap();
        }
    });
    for socks in [a_socks, b_socks] {
        let reply = timeout(
            Duration::from_secs(10),
            support::interop::socks5_udp_echo_to(
                socks,
                Address::Ipv4(host.octets()),
                port,
                b"peer-flow",
            ),
        )
        .await
        .unwrap();
        assert_eq!(reply, b"peer-flow");
    }
    echo.await.unwrap();
    let scope = |seed| TrafficScope::Peer {
        endpoint_id: "endpoint:wg".into(),
        peer_id: format!("wireguard:{}", peer::public(seed)),
    };
    for (proxy, remote_seed) in [(&a, 132), (&b, 131)] {
        support::wait_for("both peer Flow roles", || {
            snapshot(proxy, scope(remote_seed)).planes[0].source_roles
                == vec![TrafficRole::Inbound, TrafficRole::Outbound]
        })
        .await;
        let stat = snapshot(proxy, scope(remote_seed));
        assert_eq!(stat.planes[0].counters.bytes_up, Some(18));
        assert_eq!(stat.planes[0].counters.bytes_down, Some(18));
        assert_eq!(stat.planes[1].counters.rx_packets, Some(2));
        let incoming = snapshot(
            proxy,
            TrafficScope::Inbound {
                tag: "endpoint/wg".into(),
            },
        );
        assert_eq!(
            incoming.planes[1].counters.rx_packets,
            Some(1),
            "outbound replies must not count as inbound role"
        );
        assert_eq!(incoming.planes[1].counters.tx_packets, Some(1));
        let outgoing = snapshot(proxy, TrafficScope::Outbound { tag: "wg".into() });
        assert_eq!(outgoing.planes[1].counters.rx_packets, Some(1));
        assert_eq!(outgoing.planes[1].counters.tx_packets, Some(1));
        assert_eq!(outgoing.planes[1].source_roles, vec![TrafficRole::Outbound]);
        assert_eq!(
            snapshot(proxy, TrafficScope::Global).planes[1]
                .counters
                .rx_packets,
            Some(2),
            "shared resource device RX must count once across both roles"
        );
    }
    // Keep a TCP business connection open while observing and resetting its peer.
    let listener = TcpListener::bind((host, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let echo = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for _ in 0..2 {
            let mut data = [0; 4];
            stream.read_exact(&mut data).await.unwrap();
            stream.write_all(&data).await.unwrap();
        }
    });
    let mut stream = TcpStream::connect(("127.0.0.1", a_socks)).await.unwrap();
    stream.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    stream.read_exact(&mut auth).await.unwrap();
    let mut request = vec![5, 1, 0, 1];
    request.extend(host.octets());
    request.extend(port.to_be_bytes());
    stream.write_all(&request).await.unwrap();
    let mut response = [0; 10];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(response[1], 0);
    stream.write_all(b"live").await.unwrap();
    let mut data = [0; 4];
    timeout(Duration::from_secs(10), stream.read_exact(&mut data))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&data, b"live");
    support::wait_for("inbound peer active TCP", || {
        snapshot(&b, scope(131)).activity.active_stream_flows == Some(1)
    })
    .await;
    let before = snapshot(&b, scope(131));
    let usage = b.stats_snapshot().bytes_up;
    let active = b.active_sessions().len();
    let cleared = reset(&b, &before);
    assert_eq!(cleared.snapshots[0].activity, before.activity);
    assert_eq!(b.stats_snapshot().bytes_up, usage);
    assert_eq!(b.active_sessions().len(), active);
    stream.write_all(b"next").await.unwrap();
    timeout(Duration::from_secs(10), stream.read_exact(&mut data))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&data, b"next");
    support::wait_for("inbound peer live new period", || {
        snapshot(&b, scope(131)).planes[0].counters.bytes_up == Some(4)
    })
    .await;
    drop(stream);
    echo.await.unwrap();
    a_running.shutdown().await.unwrap();
    b_running.shutdown().await.unwrap();
}
