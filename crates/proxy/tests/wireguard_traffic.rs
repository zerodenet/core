#![cfg(all(feature = "wireguard", feature = "socks5"))]
#[path = "wireguard_traffic/inbound.rs"]
mod inbound;
#[path = "wireguard_traffic/peer.rs"]
mod peer;
use crate::support;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, UdpSocket},
    time::{timeout, Duration},
};
use zero_api::*;
use zero_config::RuntimeConfig;
use zero_core::Address;
use zero_proxy::Proxy;
fn snapshot(proxy: &Proxy, scope: TrafficScope) -> TrafficSnapshot {
    proxy
        .engine()
        .traffic_snapshot(&TrafficGetQuery { scope })
        .unwrap()
}
fn reset(proxy: &Proxy, snapshot: &TrafficSnapshot) -> StatsResetSnapshot {
    proxy
        .engine()
        .reset_traffic(&StatsResetCommand {
            expected_core_instance_id: snapshot.core_instance_id.clone(),
            operation_id: None,
            targets: vec![StatsResetTarget {
                scope: snapshot.scope.clone(),
                expected_stats_epoch: snapshot.stats_epoch.clone(),
                expected_generation: snapshot.generation,
            }],
        })
        .unwrap()
}
async fn connect(port: u16, method: u8) -> (TcpStream, [u8; 10]) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    stream.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    let request = if method == 3 {
        [5, method, 0, 1, 0, 0, 0, 0, 0, 0]
    } else {
        [5, method, 0, 1, 198, 51, 100, 10, 0, 80]
    };
    stream.write_all(&request).await.unwrap();
    let mut response = [0; 10];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(response[1], 0);
    (stream, response)
}
#[tokio::test]
async fn wireguard_flow_inner_outer_peer_meters_survive_live_reset() {
    let (remote, task) = peer::echo_peer().await;
    let port = support::free_port();
    let config=RuntimeConfig::parse(&serde_json::json!({
        "inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":port},"protocol":{"type":"socks5"}}],
        "endpoints":[{"tag":"wg","directions":{"inbound":false,"outbound":true},"protocol":{"type":"wireguard","private_key":STANDARD.encode([121;32]),"addresses":["10.0.0.1/32"],"peers":[{"public_key":peer::public(122),"endpoint":remote.to_string(),"allowed_ips":["0.0.0.0/0"]}]}}],
        "route":{"rules":[],"final":{"type":"route","outbound":"wg"}}
    }).to_string()).unwrap();
    let proxy = Proxy::new(config).unwrap();
    let running = support::spawn_engine(proxy.clone());
    support::wait_for_listener(port).await;
    let (control, response) = connect(port, 3).await;
    let relay = u16::from_be_bytes([response[8], response[9]]);
    let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    let packet =
        support::build_udp_packet(&Address::Ipv4([198, 51, 100, 10]), 80, b"ping").unwrap();
    let mut buffer = [0; 2048];
    client.send_to(&packet, ("127.0.0.1", relay)).await.unwrap();
    let (size, _) = timeout(Duration::from_secs(5), client.recv_from(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        support::parse_udp_packet(&buffer[..size]).unwrap().payload,
        b"ping"
    );
    let scope = TrafficScope::Endpoint {
        endpoint_id: "endpoint:wg".into(),
    };
    let peer_scope = TrafficScope::Peer {
        endpoint_id: "endpoint:wg".into(),
        peer_id: format!("wireguard:{}", peer::public(122)),
    };
    support::wait_for("outbound bytes visible while active", || {
        snapshot(&proxy, TrafficScope::Outbound { tag: "wg".into() }).planes[0]
            .counters
            .rx_bytes
            == Some(4)
    })
    .await;
    let endpoint = snapshot(&proxy, scope.clone());
    let peer_before = snapshot(&proxy, peer_scope.clone());
    assert_eq!(peer_before.planes[0].counters.bytes_up, Some(4));
    assert_eq!(peer_before.planes[0].counters.bytes_down, Some(4));
    assert_eq!(peer_before.activity.active_datagram_flows, Some(1));
    assert_eq!(
        peer_before.planes[0].source_roles,
        vec![TrafficRole::Outbound]
    );
    assert_eq!(
        snapshot(&proxy, TrafficScope::Global).planes[1].counters,
        endpoint.planes[1].counters
    );
    assert_eq!(endpoint.planes[0].counters.bytes_up, Some(4));
    assert_eq!(endpoint.planes[1].counters.tx_bytes, Some(32));
    assert_eq!(endpoint.planes[1].counters.rx_bytes, Some(32));
    assert_eq!(peer_before.planes[1].counters, endpoint.planes[1].counters);
    assert!(endpoint.planes[2].counters.tx_bytes.unwrap() > 32);
    assert!(endpoint.planes[2].counters.rx_bytes.unwrap() > 32);
    let global_before = proxy.stats_snapshot().bytes_up;
    let active = proxy.active_sessions().len();
    let cleared = reset(&proxy, &endpoint);
    assert_eq!(cleared.snapshots[0].planes[1].counters.tx_bytes, Some(0));
    assert_eq!(proxy.stats_snapshot().bytes_up, global_before);
    assert_eq!(proxy.active_sessions().len(), active);
    assert_eq!(
        snapshot(&proxy, peer_scope.clone()).planes[1]
            .counters
            .tx_bytes,
        Some(32)
    );
    client.send_to(&packet, ("127.0.0.1", relay)).await.unwrap();
    timeout(Duration::from_secs(5), client.recv_from(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    support::wait_for("endpoint new period bytes", || {
        snapshot(&proxy, scope.clone()).planes[0].counters.bytes_up == Some(4)
    })
    .await;
    assert_eq!(
        snapshot(&proxy, scope.clone()).planes[1].counters.tx_bytes,
        Some(32)
    );
    assert_eq!(
        snapshot(&proxy, peer_scope.clone()).planes[1]
            .counters
            .tx_bytes,
        Some(64)
    );
    let (mut tcp, _) = timeout(Duration::from_secs(5), connect(port, 1))
        .await
        .unwrap();
    tcp.write_all(b"live-tcp").await.unwrap();
    let mut echoed = [0; 8];
    timeout(Duration::from_secs(5), tcp.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&echoed, b"live-tcp");
    support::wait_for("active tcp bytes", || {
        snapshot(&proxy, TrafficScope::Outbound { tag: "wg".into() }).planes[0]
            .counters
            .rx_bytes
            .unwrap()
            >= 16
    })
    .await;
    let peer_live = snapshot(&proxy, peer_scope.clone());
    assert_eq!(peer_live.activity.active_stream_flows, Some(1));
    assert_eq!(peer_live.activity.active_datagram_flows, Some(1));
    assert_eq!(peer_live.planes[0].counters.bytes_up, Some(16));
    let peer_cleared = reset(&proxy, &peer_live);
    assert_eq!(peer_cleared.snapshots[0].activity, peer_live.activity);
    assert_eq!(
        peer_cleared.snapshots[0].planes[0].counters.bytes_up,
        Some(0)
    );
    let before = proxy
        .engine()
        .traffic_snapshot(&TrafficGetQuery {
            scope: TrafficScope::Outbound { tag: "wg".into() },
        })
        .unwrap();
    reset(&proxy, &before);
    tcp.write_all(b"post-rst").await.unwrap();
    timeout(Duration::from_secs(5), tcp.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&echoed, b"post-rst");
    assert_eq!(
        snapshot(&proxy, TrafficScope::Outbound { tag: "wg".into() }).planes[0]
            .counters
            .rx_bytes,
        Some(8)
    );
    support::wait_for("peer new period bytes", || {
        snapshot(&proxy, peer_scope.clone()).planes[0]
            .counters
            .bytes_up
            == Some(8)
    })
    .await;
    drop(tcp);
    drop(control);
    running.shutdown().await.unwrap();
    task.abort();
}
