#![cfg(feature = "wireguard")]
//! Exercise actual inventory preparation/publication, including rollback.
use crate::runtime::Proxy;
use zero_api::EndpointGetQuery;
use zero_config::{RuntimeConfig, UdpReceiveQueueConfig};

async fn publish(proxy: &Proxy, config: &RuntimeConfig) {
    for prepared in proxy
        .protocols
        .prepare_outbound_devices(config, proxy.tcp_runtime_services())
        .await
        .unwrap()
    {
        prepared.publish().await;
    }
}
fn incarnations(proxy: &Proxy, config: &RuntimeConfig) -> Vec<u64> {
    let binding = config.endpoint_bindings().remove(0);
    let mut snapshot = proxy
        .engine()
        .endpoint_snapshot(&EndpointGetQuery {
            endpoint_id: binding.endpoint_id,
        })
        .unwrap();
    proxy
        .protocols
        .observe_endpoint(config, &mut snapshot)
        .unwrap()
        .incarnations
}
#[tokio::test]
async fn unchanged_queue_policy_reuses_linked_devices_and_aborted_policy_change_preserves_them() {
    let config = RuntimeConfig::parse(
        &serde_json::json!({
            "endpoints":[{"tag":"test","directions":{"inbound":true,"outbound":true},
                "listen":{"address":"127.0.0.1","port":51820},
                "protocol":{"type":"wireguard","private_key":"01".repeat(32),
                    "addresses":["10.0.0.1/32"],"peers":[{
                        "public_key":"02".repeat(32),
                        "endpoint":"127.0.0.1:9","allowed_ips":["10.0.0.2/32"]}]}}],
            "route":{"rules":[],"final":{"type":"direct"}}
        })
        .to_string(),
    )
    .unwrap();
    let proxy = Proxy::new(config.clone()).unwrap();
    publish(&proxy, &config).await;
    let initial = incarnations(&proxy, &config);
    assert!(!initial.is_empty());
    publish(&proxy, &config).await;
    assert_eq!(incarnations(&proxy, &config), initial);
    let mut changed = config.clone();
    changed.runtime.network.udp_receive_queue = UdpReceiveQueueConfig {
        capacity: 128,
        max_payload_bytes: Some(65_527),
        max_stack_payload_bytes: Some(131_054),
    };
    let staged = proxy
        .protocols
        .prepare_outbound_devices(&changed, proxy.tcp_runtime_services())
        .await
        .unwrap();
    drop(staged); // Equivalent to a later stage failing before publication.
    assert_eq!(incarnations(&proxy, &config), initial);
    publish(&proxy, &changed).await;
    let replaced = incarnations(&proxy, &changed);
    assert_ne!(
        replaced, initial,
        "new limits must replace the active stack policy"
    );
    publish(&proxy, &changed).await;
    assert_eq!(incarnations(&proxy, &changed), replaced);
    for done in proxy.protocols.shutdown_outbound_devices() {
        done.await;
    }
}
