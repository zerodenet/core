use super::SharedRawIpDevice;
use crate::runtime::raw_ip::{RawIpStackOptions, RawIpTraffic};
use tokio::sync::{mpsc, watch};
use zero_api::{TrafficDropReason, TrafficGetQuery, TrafficPlane, TrafficScope};

#[tokio::test]
async fn linked_stack_uses_configured_capacity_and_exposes_final_discards_on_existing_meters() {
    let config = zero_config::RuntimeConfig::parse(
        &serde_json::json!({
            "runtime":{"network":{"udp_receive_queue":{"capacity":1,"max_payload_bytes":65527}}},
            "endpoints":[{"tag":"test","protocol":{"type":"wireguard",
                "private_key":"01".repeat(32),"addresses":["10.0.0.1/32"],"peers":[{
                    "public_key":"02".repeat(32),"endpoint":"127.0.0.1:9",
                    "allowed_ips":["0.0.0.0/0"]}]}}],
            "route":{"rules":[],"final":{"type":"direct"}}
        })
        .to_string(),
    )
    .unwrap();
    let engine = zero_engine::Engine::new(config.clone()).unwrap();
    let binding = config.endpoint_bindings().remove(0);
    let prepared = engine
        .prepare_endpoint_traffic(&binding, &["peer-1".into()])
        .unwrap();
    prepared.publish();
    let traffic = RawIpTraffic::new(prepared.endpoint(), prepared.peers().first().cloned())
        .with_global(engine.traffic_meter(&TrafficScope::Global));
    let options = RawIpStackOptions::from_config(&config.runtime.network.udp_receive_queue)
        .with_traffic(traffic);
    let (send, mut packets) = mpsc::channel(4);
    let (_sender, endpoint) = watch::channel(send);
    let device = SharedRawIpDevice::start_on_endpoint_with_options(
        vec!["10.0.0.1".parse().unwrap()],
        1420,
        0,
        endpoint,
        options,
    )
    .unwrap();
    let mut socket = device.bind_udp("10.0.0.1".parse().unwrap()).unwrap();
    let remote = zero_stack::packet::Endpoint {
        ip: "10.0.0.2".parse().unwrap(),
        port: 53,
    };
    socket.send_to(b"request", remote).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), packets.recv())
        .await
        .unwrap()
        .unwrap();
    let reply = zero_stack::packet::build_udp(
        remote.ip,
        socket.local_endpoint().ip,
        remote.port,
        socket.local_endpoint().port,
        b"reply",
    );
    for _ in 0..2 {
        assert!(
            device
                .deliver_decrypted_owned(reply.clone().into(), true)
                .await
                .is_ok(),
            "refused correlated return cannot escape as ingress"
        );
    }
    for scope in [
        TrafficScope::Global,
        TrafficScope::Endpoint {
            endpoint_id: binding.endpoint_id.clone(),
        },
        TrafficScope::Peer {
            endpoint_id: binding.endpoint_id,
            peer_id: "peer-1".into(),
        },
    ] {
        let snap = engine.traffic_snapshot(&TrafficGetQuery { scope }).unwrap();
        let inner = snap
            .planes
            .iter()
            .find(|p| p.plane == TrafficPlane::Inner)
            .unwrap();
        assert_eq!(inner.counters.dropped_packets, Some(1));
        assert_eq!(
            inner
                .drop_reasons
                .iter()
                .find(|r| r.reason == TrafficDropReason::QueueFull)
                .unwrap()
                .packets,
            1
        );
    }
    assert_eq!(socket.recv_from().await.unwrap().payload, b"reply");
    assert!(device
        .deliver_decrypted_owned(reply.into(), true)
        .await
        .is_ok());
    assert_eq!(socket.recv_from().await.unwrap().payload, b"reply");
    device.close_now();
    device.wait_stopped().await;
}
