//! Actual Proxy/Host replacements, not the retired private constructors.
use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use std::{net::SocketAddr, time::Duration};
use zero_platform_tokio::controlled_carrier::CarrierOrigin;

#[cfg(all(feature = "vmess", feature = "vless", feature = "shadowsocks"))]
mod relay;

#[cfg(feature = "hysteria2")]
mod hysteria2;

#[cfg(all(feature = "shadowsocks", not(any(unix, windows))))]
mod datagram_only;

fn config(peer: SocketAddr, outer: Option<SocketAddr>, linked: bool) -> RuntimeConfig {
    let public = STANDARD.encode(PublicKey::from(&StaticSecret::from([2; 32])).as_bytes());
    let mut protocol = serde_json::json!({"type":"wireguard","private_key":STANDARD.encode([1;32]),
        "addresses":["10.0.0.1/24"],"mtu":1420,
        "peers":[{"public_key":public,"endpoint":peer.to_string(),"allowed_ips":["0.0.0.0/0"]}]});
    if outer.is_some() {
        protocol["outer_udp_proxy"] = serde_json::json!("outer");
    }
    if linked {
        protocol["inbound_tag"] = serde_json::json!("wg-in");
    }
    let mut outbounds = vec![serde_json::json!({"tag":"wg","protocol":protocol})];
    if let Some(outer) = outer {
        outbounds.push(serde_json::json!({"tag":"outer",
        "protocol":{"type":"socks5","server":"127.0.0.1","port":outer.port()}}));
    }
    let inbounds = if linked {
        vec![serde_json::json!({"tag":"wg-in",
        "listen":{"address":"127.0.0.1","port":0},
        "protocol":{"type":"wireguard","private_key":STANDARD.encode([1;32]),
            "addresses":["10.0.0.1/24"],"mtu":1420,
            "peers":[{"public_key":public,"allowed_ips":["0.0.0.0/0"]}]}})]
    } else {
        vec![]
    };
    RuntimeConfig::parse(
        &serde_json::json!({"inbounds":inbounds,"outbounds":outbounds,
        "route":{"final":{"type":"direct"}}})
        .to_string(),
    )
    .unwrap()
}
#[cfg(any(
    feature = "hysteria2",
    all(feature = "vmess", feature = "vless", feature = "shadowsocks")
))]
fn active_outer_config(peer: SocketAddr, linked: bool) -> serde_json::Value {
    let mut candidate = serde_json::to_value(config(peer, None, linked)).unwrap();
    if linked {
        // The linked Source starts as a listener. An explicitly active peer
        // uses the protocol timer to originate the real initial handshake.
        candidate["inbounds"][0]["protocol"]["peers"][0]["keepalive_secs"] = 25.into();
        candidate["outbounds"][0]["protocol"]["peers"][0]["keepalive_secs"] = 25.into();
    }
    candidate
}
fn handle(proxy: &crate::Proxy) -> crate::ProxyHandle {
    crate::ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    )
}
fn originals(proxy: &crate::Proxy) -> Vec<CarrierOrigin> {
    proxy
        .protocols
        .test_packet_source_origins(proxy.engine().config().as_ref())
}
fn devices(proxy: &crate::Proxy) -> Vec<Arc<SharedRawIpDevice>> {
    proxy
        .protocols
        .test_outbound_devices(proxy.engine().config().as_ref())
}
fn worker_census(
    device: &SharedRawIpDevice,
) -> Vec<zero_platform_tokio::contract::scope::TaskJoinWitness> {
    assert!(!device.is_closed());
    let tasks = device.test_original_worker_witnesses();
    let driver = device
        .test_original_driver_witness()
        .expect("the actual endpoint driver was installed");
    assert!(tasks.iter().any(|task| task.same_registration(&driver)));
    assert!(tasks.iter().all(|task| !task.is_joined()));
    tasks
}
fn joined(
    device: &SharedRawIpDevice,
    tasks: &[zero_platform_tokio::contract::scope::TaskJoinWitness],
) {
    assert!(device.is_closed());
    let driver = device
        .test_original_driver_witness()
        .expect("the actual wire driver was installed");
    assert!(
        tasks.iter().any(|task| task.same_registration(&driver)),
        "the census must include that exact original wire registration"
    );
    // Capture the actual census before retirement reclaims current records.
    // TCP workers start on demand; every captured registration must join and ACK.
    for task in tasks {
        let receipt = task.try_joined().expect("same original worker exit");
        assert!(task.matches_joined(&receipt) && receipt.registration_released());
        receipt.result().unwrap();
    }
}
fn changed(mut config: RuntimeConfig) -> RuntimeConfig {
    if let OutboundProtocolConfig::Wireguard { mtu, .. } = &mut config.outbounds[0].protocol {
        *mtu = 1380;
    }
    if let Some(inbound) = config.inbounds.first_mut() {
        if let zero_config::InboundProtocolConfig::Wireguard { mtu, .. } = &mut inbound.protocol {
            *mtu = 1380;
        }
    }
    config
}

#[tokio::test]
async fn standalone_actual_host_reuses_unchanged_original_then_replaces_and_restores_without_grace()
{
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let config = config(peer.local_addr().unwrap(), None, false);
    let proxy = crate::Proxy::new(config.clone()).unwrap();
    let running = proxy.spawn();
    handle(&proxy)
        .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5))
        .await
        .unwrap();
    let old = devices(&proxy).remove(0);
    let old_workers = worker_census(&old);
    let original = originals(&proxy).remove(0);
    assert!(original.closed().is_none());
    handle(&proxy)
        .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5))
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&old, &devices(&proxy)[0]));
    handle(&proxy)
        .apply_runtime_config_and_wait(changed(config.clone()), Duration::from_secs(5))
        .await
        .unwrap();
    joined(&old, &old_workers);
    assert!(original.closed().unwrap().matches(&original));
    let replacement = devices(&proxy).remove(0);
    let replacement_workers = worker_census(&replacement);
    assert!(!Arc::ptr_eq(&old, &replacement));
    handle(&proxy)
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    joined(&replacement, &replacement_workers);
    assert!(!Arc::ptr_eq(&old, &devices(&proxy)[0]));
    running.shutdown().await.unwrap();
}

#[cfg(feature = "socks5")]
#[tokio::test]
async fn linked_and_standalone_outer_replacement_consumes_old_scope_and_same_native_originals() {
    for linked in [false, true] {
        let outer_config = RuntimeConfig::parse(r#"{"inbounds":[{"tag":"outer-in","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"route":{"final":{"type":"direct"}}}"#).unwrap();
        let outer = crate::Proxy::new(outer_config.clone()).unwrap();
        let outer_running = outer.spawn();
        handle(&outer)
            .apply_runtime_config_and_wait(outer_config, Duration::from_secs(5))
            .await
            .unwrap();
        let source = outer
            .system_tun_source_registry()
            .owner("outer-in")
            .unwrap();
        source.wait_ready().await.unwrap();
        let endpoint = *source.original_listener_address.get().unwrap();
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let config = config(peer.local_addr().unwrap(), Some(endpoint), linked);
        let proxy = crate::Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        handle(&proxy)
            .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5))
            .await
            .unwrap();
        let old = devices(&proxy).remove(0);
        let old_workers = worker_census(&old);
        let original = originals(&proxy).remove(0);
        let plan = proxy.tcp_runtime_services().components.clone().unwrap();
        let lower = plan.configured("outer").unwrap();
        let physical = (0..lower.recipe.spec().resources.len())
            .filter_map(|ordinal| lower.original_carrier_owner(ordinal))
            .flat_map(|owner| {
                owner
                    .observe()
                    .unwrap()
                    .slots()
                    .iter()
                    .map(|slot| slot.original().clone())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert!(!physical.is_empty());
        assert!(physical.iter().all(|origin| origin.closed().is_none()));
        handle(&proxy)
            .apply_runtime_config_and_wait(changed(config), Duration::from_secs(5))
            .await
            .unwrap();
        joined(&old, &old_workers);
        assert!(!original.same_original(&originals(&proxy)[0]));
        for origin in physical {
            let closed = origin
                .closed()
                .expect("same original outer native provider");
            assert!(origin.matches_closed(&closed) && closed.had_native_acquisition());
        }
        assert!(!Arc::ptr_eq(&old, &devices(&proxy)[0]));
        running.shutdown().await.unwrap();
        outer_running.shutdown().await.unwrap();
    }
}

#[cfg(feature = "socks5")]
#[tokio::test]
async fn actual_source_reconnect_keeps_the_wire_and_closes_previous_native_before_new_delivery() {
    let outer_config = RuntimeConfig::parse(r#"{"inbounds":[{"tag":"outer-in","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"route":{"final":{"type":"direct"}}}"#).unwrap();
    let outer = crate::Proxy::new(outer_config.clone()).unwrap();
    let outer_running = outer.spawn();
    handle(&outer)
        .apply_runtime_config_and_wait(outer_config.clone(), Duration::from_secs(5))
        .await
        .unwrap();
    let source = outer
        .system_tun_source_registry()
        .owner("outer-in")
        .unwrap();
    source.wait_ready().await.unwrap();
    let endpoint = *source.original_listener_address.get().unwrap();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let config = config(peer.local_addr().unwrap(), Some(endpoint), false);
    let proxy = crate::Proxy::new(config.clone()).unwrap();
    let running = proxy.spawn();
    handle(&proxy)
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    let original = originals(&proxy).remove(0);
    let device = devices(&proxy).remove(0);
    let device_workers = worker_census(&device);
    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
    let lower = plan.configured("outer").unwrap();
    let owners = (0..lower.recipe.spec().resources.len())
        .filter_map(|ordinal| lower.original_carrier_owner(ordinal))
        .collect::<Vec<_>>();
    let previous = owners
        .iter()
        .flat_map(|owner| {
            owner
                .observe()
                .unwrap()
                .slots()
                .iter()
                .map(|slot| slot.original().clone())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert!(!previous.is_empty());
    let mut bytes = [0; 2048];
    tokio::time::timeout(Duration::from_secs(5), peer.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    outer_running.shutdown().await.unwrap();
    let terminal = source.terminal_for_test().unwrap();
    assert!(terminal.failure().is_none() && terminal.closed_io_receipts() > 0);
    // This is the real remote's original close report, before reusing its
    // address. No free-port probe or elapsed grace substitutes for it.
    while peer.try_recv_from(&mut bytes).is_ok() {}
    let mut restart = outer_config;
    restart.inbounds[0].listen.port = endpoint.port();
    let replacement = crate::Proxy::new(restart.clone()).unwrap();
    let replacement_running = replacement.spawn();
    handle(&replacement)
        .apply_runtime_config_and_wait(restart, Duration::from_secs(5))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(12), peer.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&device, &devices(&proxy)[0]));
    assert!(original.same_original(&originals(&proxy)[0]));
    assert!(device
        .test_original_worker_witnesses()
        .iter()
        .all(|task| !task.is_joined()));
    for origin in &previous {
        let closed = origin
            .closed()
            .expect("old native before new wire delivery");
        assert!(origin.matches_closed(&closed) && closed.had_native_acquisition());
    }
    let next = owners
        .iter()
        .flat_map(|owner| {
            owner
                .observe()
                .unwrap()
                .slots()
                .iter()
                .map(|slot| slot.original().clone())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert!(next
        .iter()
        .any(|fresh| fresh.closed().is_none()
            && previous.iter().all(|old| !old.same_original(fresh))));
    running.shutdown().await.unwrap();
    joined(&device, &device_workers);
    replacement_running.shutdown().await.unwrap();
}
