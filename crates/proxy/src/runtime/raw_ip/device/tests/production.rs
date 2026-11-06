//! Ordinary SOCKS -> canonical WG roles, real encrypted wire and original joins.
#![cfg(all(feature = "wireguard", feature = "socks5", feature = "dns"))]
#[path = "../../../../../tests/wireguard_traffic/peer.rs"]
mod peer;
use crate::{Proxy, ProxyHandle};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};
use zero_api::{TrafficGetQuery, TrafficScope, TrafficSnapshot};
use zero_config::RuntimeConfig;
fn end() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(5)
}
fn traffic(proxy: &Proxy, scope: TrafficScope) -> TrafficSnapshot {
    proxy
        .engine()
        .traffic_snapshot(&TrafficGetQuery { scope })
        .unwrap()
}
fn configuration(peer: SocketAddr, socks: u16, domain: bool) -> RuntimeConfig {
    let mut config = serde_json::json!({
        "inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":socks},"protocol":{"type":"socks5"}}],
        "endpoints":[{"tag":"wg","directions":{"inbound":true,"outbound":true},"listen":{"address":"127.0.0.1","port":0},"protocol":{
            "type":"wireguard","private_key":STANDARD.encode([121;32]),"addresses":["10.0.0.1/32"],
            "peers":[{"public_key":peer::public(122),"endpoint":peer.to_string(),"allowed_ips":["0.0.0.0/0"]}]
        }}],"route":{"rules":[],"final":{"type":"route","outbound":"wg"}}
    });
    if domain {
        config["runtime"] = serde_json::json!({"dns":{"servers":{
            "node":{"type":"system"},"query":{"type":"udp","host":"10.0.0.2","detour":"wg"}
        },"default_server":"query","policy":{"node_server":"node","timeout_ms":1000}}});
    }
    RuntimeConfig::parse(&config.to_string()).unwrap()
}
async fn connect(port: u16, domain: bool) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    stream.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    let mut request = if domain {
        let host = b"ordinary.packet.example";
        let mut request = vec![5, 1, 0, 3, host.len() as u8];
        request.extend_from_slice(host);
        request
    } else {
        vec![5, 1, 0, 1, 198, 51, 100, 10]
    };
    request.extend_from_slice(&80_u16.to_be_bytes());
    stream.write_all(&request).await.unwrap();
    let mut reply = [0; 10];
    stream.read_exact(&mut reply).await.unwrap();
    assert_eq!(
        reply[1], 0,
        "normal connect ACK follows the actual inner SYN/SYN-ACK"
    );
    stream
}
async fn exercise(domain: bool) {
    let (endpoint, peer_task) = if domain {
        peer::echo_peer_with_dns(zero_traits::IpAddress::V4([198, 51, 100, 10])).await
    } else {
        peer::echo_peer().await
    };
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let socks = reserved.local_addr().unwrap().port();
    drop(reserved);
    let config = configuration(endpoint, socks, domain);
    let inbound = config.endpoints[0].inbound_tag();
    let proxy = Proxy::new(config.clone()).unwrap();
    let packet_memory = proxy
        .protocols
        .runtime_host()
        .original_budgets()
        .packet
        .clone();
    assert_eq!(
        packet_memory.native().occupied.frames,
        0,
        "cold recipes acquire no read workspace"
    );
    let handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    let running = proxy.spawn();
    handle
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    let snapshot = proxy.engine().runtime_snapshot();
    let source = proxy
        .inbound_owners
        .owner(&inbound)
        .expect("the same actual WG source owner");
    source.wait_ready().await.unwrap();
    assert_eq!(
        packet_memory.native().occupied.frames,
        2,
        "one original Source prepays its scratch and decode workspaces"
    );
    assert_eq!(packet_memory.native().occupied.bytes, 2 * 65_536);
    let origins = proxy
        .protocols
        .test_packet_source_origins(snapshot.config());
    assert_eq!(origins.len(), 1);
    let original = origins[0].clone();
    assert_eq!(original.instance(), source.identity());
    assert!(original.parent().is_none());
    assert!(original.closed().is_none());
    assert!(!original.driver().unwrap().is_joined());
    let mut client = tokio::time::timeout_at(end(), connect(socks, domain))
        .await
        .unwrap();
    client.write_all(b"standard-owned-flow").await.unwrap();
    let mut echoed = [0; 19];
    tokio::time::timeout_at(end(), client.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&echoed, b"standard-owned-flow");
    let devices = proxy.protocols.test_outbound_devices(snapshot.config());
    assert_eq!(devices.len(), 1);
    let joins = devices
        .iter()
        .flat_map(|device| device.task.test_original_witnesses())
        .collect::<Vec<_>>();
    assert!(
        joins.len() >= 3,
        "original endpoint, TCP control and actual connection worker"
    );
    assert!(joins.iter().all(|join| !join.is_joined()));
    tokio::time::timeout_at(end(), async {
        while traffic(&proxy, TrafficScope::Outbound { tag: "wg".into() }).planes[0]
            .counters
            .rx_bytes
            != Some(19)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual Flow receive accounting");
    let outbound = traffic(&proxy, TrafficScope::Outbound { tag: "wg".into() });
    let endpoint = traffic(
        &proxy,
        TrafficScope::Endpoint {
            endpoint_id: "endpoint:wg".into(),
        },
    );
    let peer_scope = traffic(
        &proxy,
        TrafficScope::Peer {
            endpoint_id: "endpoint:wg".into(),
            peer_id: format!("wireguard:{}", peer::public(122)),
        },
    );
    assert_eq!(outbound.planes[0].counters.tx_bytes, Some(19));
    assert_eq!(outbound.planes[0].counters.rx_bytes, Some(19));
    assert_eq!(endpoint.planes[0].counters.bytes_up, Some(19));
    assert_eq!(endpoint.planes[0].counters.bytes_down, Some(19));
    assert_eq!(peer_scope.planes[0].counters.bytes_up, Some(19));
    assert_eq!(peer_scope.planes[0].counters.bytes_down, Some(19));
    assert!(outbound.planes[1].counters.tx_bytes.unwrap() > 19);
    assert!(outbound.planes[1].counters.rx_bytes.unwrap() > 19);
    assert!(endpoint.planes[2].counters.tx_bytes.unwrap() > 19);
    assert!(endpoint.planes[2].counters.rx_bytes.unwrap() > 19);
    assert!(
        original.closed().is_none(),
        "a logical stream cannot close its shared native listener"
    );
    // Keep the external client alive: actual host shutdown must stop its real
    // caller and return the role IO before confirming original device closure.
    tokio::time::timeout_at(end(), running.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(joins.iter().all(|join| join.is_joined()));
    assert!(devices.iter().all(|device| device.is_closed()));
    let closed = original
        .closed()
        .expect("same original native listener closed");
    assert!(closed.matches(&original));
    assert!(closed.had_native_acquisition());
    assert!(original.driver().unwrap().is_joined());
    assert_eq!(
        packet_memory.native().occupied.frames,
        0,
        "all actual read payloads and workspace promises returned"
    );
    assert_eq!(packet_memory.native().occupied.bytes, 0);
    let terminal = source
        .terminal_for_test()
        .expect("actual original Source Scope and native terminal");
    assert_eq!(terminal.identity(), source.identity());
    assert!(terminal.joined_tasks() >= 4);
    assert!(terminal.closed_io_receipts() > 0);
    let mut byte = [0];
    let stopped = timeout(Duration::from_secs(5), client.read(&mut byte))
        .await
        .unwrap();
    assert!(
        matches!(stopped, Ok(0))
            || stopped.is_err_and(|source| matches!(
                source.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ))
    );
    peer_task.abort();
    let result = peer_task.await;
    assert!(result.is_err_and(|error| error.is_cancelled()));
}
#[tokio::test]
async fn ordinary_canonical_stream_preserves_inner_peer_traffic_and_same_original_native_scope() {
    exercise(false).await;
}
#[tokio::test]
async fn ordinary_canonical_domain_stream_uses_real_target_dns_and_closes_original_callers() {
    exercise(true).await;
}

#[path = "production/replacement.rs"]
mod replacement;

#[path = "production/packet.rs"]
mod packet;
