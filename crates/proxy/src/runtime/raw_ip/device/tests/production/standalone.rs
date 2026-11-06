//! Ordinary outbound-only WG uses its own original Host Bound socket and wire.
use super::*;
use crate::inventory::DatagramAssociationTarget;
use tokio::net::UdpSocket;
use zero_core::Address;

fn configuration(peer: SocketAddr, domain: bool) -> RuntimeConfig {
    let mut config = serde_json::json!({
        "inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],
        "outbounds":[{"tag":"wg","protocol":{
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
fn socks_port(proxy: &Proxy) -> u16 {
    proxy
        .inbound_owners
        .owner("socks")
        .unwrap()
        .original_listener_address
        .get()
        .expect("same actual bound SOCKS original")
        .port()
}
async fn datagrams(port: u16) -> TcpStream {
    let mut control = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    control.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    control.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    control
        .write_all(&[5, 3, 0, 1, 0, 0, 0, 0, 0, 0])
        .await
        .unwrap();
    let mut reply = [0; 10];
    control.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply[1], 0);
    let relay = u16::from_be_bytes([reply[8], reply[9]]);
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let codec = socks5::udp::Socks5InboundUdpCodec;
    let mut buffer = [0; 2048];
    for payload in [&b""[..], &b"wire"[..], &b"again"[..]] {
        let packet = codec
            .encode_response_to_client(&Address::Ipv4([198, 51, 100, 10]), 80, payload)
            .unwrap();
        client.send_to(&packet, ("127.0.0.1", relay)).await.unwrap();
        let (size, _) = tokio::time::timeout_at(end(), client.recv_from(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        let (address, port, echoed) = codec.decode_response(&buffer[..size]).unwrap().into_parts();
        assert_eq!(address, Address::Ipv4([198, 51, 100, 10]));
        assert_eq!(port, 80);
        assert_eq!(echoed.as_slice(), payload);
    }
    control
}
async fn exercise(domain: bool) {
    let (endpoint, peer_task) = if domain {
        peer::echo_peer_with_dns(zero_traits::IpAddress::V4([198, 51, 100, 10])).await
    } else {
        peer::echo_peer().await
    };
    let config = configuration(endpoint, domain);
    let proxy = Proxy::new(config.clone()).unwrap();
    let memory = proxy
        .protocols
        .runtime_host()
        .original_budgets()
        .packet
        .clone();
    assert_eq!(
        memory.native().occupied.frames,
        0,
        "cold union performs no native acquisition"
    );
    assert!(DatagramAssociationTarget::resolve_endpoint(
        &proxy.tcp_runtime_services().execution(),
        "wg",
        None
    )
    .is_err());
    let handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    let running = proxy.spawn();
    handle
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    let source = proxy.inbound_owners.owner("socks").unwrap();
    source.wait_ready().await.unwrap();
    assert_eq!(
        memory.native().occupied.frames,
        2,
        "same Bound original prepays both real wire workspaces"
    );
    assert_eq!(memory.native().occupied.bytes, 2 * 65_536);
    let execution = proxy.tcp_runtime_services().execution();
    let target = DatagramAssociationTarget::resolve_endpoint(&execution, "wg", None).unwrap();
    assert_eq!(target.endpoint(), None);
    let slot = target.current_slot(&execution).unwrap();
    assert!(execution
        .components
        .as_ref()
        .unwrap()
        .has_device_carrier_provider(slot.role_id()));
    let mut origins = proxy
        .protocols
        .test_packet_source_origins(proxy.engine().config().as_ref());
    assert_eq!(origins.len(), 1);
    let original = origins.remove(0);
    assert!(original.parent().is_none());
    assert!(original.closed().is_none());
    let driver = original.driver().unwrap();
    assert!(!driver.is_joined());
    assert_ne!(
        original.instance(),
        source.identity(),
        "native UDP is the actual outbound root, not its unrelated SOCKS caller"
    );
    let mut client = tokio::time::timeout_at(end(), connect(socks_port(&proxy), domain))
        .await
        .unwrap();
    client.write_all(b"standalone-owned-flow").await.unwrap();
    let mut echoed = [0; 21];
    tokio::time::timeout_at(end(), client.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&echoed, b"standalone-owned-flow");
    let udp_control = datagrams(socks_port(&proxy)).await;
    tokio::time::timeout_at(end(), async {
        while traffic(&proxy, TrafficScope::Outbound { tag: "wg".into() }).planes[0]
            .counters
            .rx_bytes
            != Some(30)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual Stream plus Datagram Inner accounting");
    let counters = traffic(&proxy, TrafficScope::Outbound { tag: "wg".into() });
    assert_eq!(counters.planes[0].counters.tx_bytes, Some(30));
    assert!(
        original.closed().is_none(),
        "logical consumers do not close the Host native original"
    );
    let devices = proxy
        .protocols
        .test_outbound_devices(proxy.engine().config().as_ref());
    assert_eq!(devices.len(), 1);
    let joins = devices[0].task.test_original_witnesses();
    assert!(
        joins.len() >= 3,
        "actual wire, TCP control and connected TCP worker"
    );
    assert!(joins.iter().all(|join| !join.is_joined()));
    tokio::time::timeout_at(end(), running.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(devices[0].is_closed());
    assert!(joins.iter().all(|join| join.is_joined()));
    assert!(driver.try_joined().unwrap().registration_released());
    let closed = original
        .closed()
        .expect("matching native Bound socket close");
    assert!(closed.matches(&original));
    assert!(closed.had_native_acquisition());
    assert_eq!(memory.native().occupied.frames, 0);
    assert_eq!(memory.native().occupied.bytes, 0);
    assert!(target
        .current_slot(&proxy.tcp_runtime_services().execution())
        .is_err());
    drop(client);
    drop(udp_control);
    peer_task.abort();
    assert!(peer_task.await.is_err_and(|error| error.is_cancelled()));
}
#[tokio::test]
async fn ordinary_standalone_wireguard_stream_and_datagram_use_same_original_bound_provider() {
    exercise(false).await;
}
#[tokio::test]
async fn ordinary_standalone_wireguard_domain_stream_resolves_over_same_real_wire() {
    exercise(true).await;
}

#[path = "standalone/replacement.rs"]
mod replacement;
