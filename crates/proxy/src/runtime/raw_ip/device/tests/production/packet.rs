//! Ordinary encrypted Source -> exact Packet role -> same original Source sink.
#[path = "packet/client.rs"]
mod client;
use super::*;
use std::sync::Arc;
use tokio::net::UdpSocket;
fn config(remote: SocketAddr, client: SocketAddr) -> RuntimeConfig {
    RuntimeConfig::parse(&serde_json::json!({
        "endpoints":[
            {"tag":"input","directions":{"inbound":true,"outbound":false},"listen":{"address":"127.0.0.1","port":0},"protocol":{
                "type":"wireguard","private_key":STANDARD.encode([111;32]),"addresses":["10.42.0.1/32"],
                "peers":[{"public_key":peer::public(112),"endpoint":client.to_string(),"allowed_ips":["10.42.0.2/32"]}]}},
            {"tag":"wg","directions":{"inbound":true,"outbound":true},"listen":{"address":"127.0.0.1","port":0},"protocol":{
                "type":"wireguard","private_key":STANDARD.encode([121;32]),"addresses":["10.0.0.1/32"],
                "peers":[{"public_key":peer::public(122),"endpoint":remote.to_string(),"allowed_ips":["0.0.0.0/0"]}]}}
        ],"route":{"final":{"type":"route","outbound":"wg"},"final_mode":"packet"}
    }).to_string()).unwrap()
}
#[tokio::test]
async fn ordinary_packet_roles_return_original_source_and_retire_old_graph_without_joining_shared_main(
) {
    let (remote, remote_task) = peer::echo_peer().await;
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let config = config(remote, socket.local_addr().unwrap());
    let input = config.endpoints[0].inbound().unwrap();
    let proxy = Proxy::new(config.clone()).unwrap();
    let handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    let running = proxy.spawn();
    handle
        .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5))
        .await
        .unwrap();
    let input_owner = proxy.inbound_owners.owner(&input.tag).unwrap();
    input_owner.wait_ready().await.unwrap();
    let main = input_owner
        .task_access()
        .source_main_witness()
        .await
        .unwrap();
    let input_original = proxy
        .inbound_owners
        .packet_source_origin(&input)
        .unwrap()
        .unwrap();
    let output_original = proxy
        .protocols
        .test_packet_source_origins(proxy.engine().config().as_ref())
        .remove(0);
    let mut client = client::Client::from_original(
        socket,
        *input_owner
            .original_listener_address
            .get()
            .expect("same original bound listener address"),
    );
    let reply = client.echo(b"opaque-return-cookie").await;
    assert_eq!(zero_stack::packet::ip_hop_limit(&reply), Some(63));
    let mut previous = proxy
        .protocols
        .test_outbound_devices(proxy.engine().config().as_ref());
    assert_eq!(previous.len(), 1);
    for network in [false, true] {
        let original_joins = previous[0].task.test_original_witnesses();
        assert!(original_joins.iter().all(|join| !join.is_joined()));
        if network {
            proxy.egress_interface_control().invalidate_network();
        } else {
            let mut changed = config.clone();
            changed.runtime.network.udp_receive_queue.capacity =
                if changed.runtime.network.udp_receive_queue.capacity == 64 {
                    32
                } else {
                    64
                };
            handle
                .apply_runtime_config_and_wait(changed, Duration::from_secs(5))
                .await
                .unwrap();
        }
        tokio::time::timeout_at(end(), async {
            loop {
                let current = proxy
                    .protocols
                    .test_outbound_devices(proxy.engine().config().as_ref());
                if current.len() == 1
                    && current[0].is_usable()
                    && current[0].incarnation != previous[0].incarnation
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(previous[0].is_closed());
        assert!(original_joins.iter().all(|join| join.is_joined()));
        assert!(
            !main.is_joined(),
            "taskless Packet views do not join the retained Source main"
        );
        assert!(!input_original.driver().is_joined());
        assert!(input_original.closed().is_none());
        assert!(!output_original.driver().unwrap().is_joined());
        assert!(output_original.closed().is_none());
        assert!(Arc::ptr_eq(
            &input_owner,
            &proxy.inbound_owners.owner(&input.tag).unwrap()
        ));
        client.echo(b"same-original-after-replacement").await;
        previous = proxy
            .protocols
            .test_outbound_devices(proxy.engine().config().as_ref());
    }
    let route = proxy
        .engine()
        .packet_routes_snapshot(&zero_api::PacketRouteListQuery::default());
    assert_eq!(route.total, 1);
    let output_peer = traffic(
        &proxy,
        TrafficScope::Peer {
            endpoint_id: "endpoint:wg".into(),
            peer_id: format!("wireguard:{}", peer::public(122)),
        },
    );
    assert_eq!(output_peer.activity.active_packet_routes, Some(1));
    assert_eq!(
        proxy.stats_snapshot().bytes_up,
        0,
        "L3 frames do not mint Flow traffic"
    );
    tokio::time::timeout_at(end(), running.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(main.is_joined());
    assert!(input_original.driver().is_joined());
    assert!(input_original
        .closed()
        .unwrap()
        .matches(input_original.owner()));
    assert!(output_original.driver().unwrap().is_joined());
    assert!(output_original.closed().unwrap().matches(&output_original));
    assert!(previous.iter().all(|device| device.is_closed()));
    assert!(
        input_owner
            .terminal_for_test()
            .unwrap()
            .closed_io_receipts()
            > 0
    );
    remote_task.abort();
    assert!(remote_task.await.is_err_and(|error| error.is_cancelled()));
}
