use super::super::pending_map::PendingEndpoints;
use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use zero_platform_tokio::contract::{scope::TaskScope, CancellationToken};

fn inputs(endpoint: &str) -> (OutboundConfig, InboundConfig) {
    let peer = STANDARD.encode(PublicKey::from(&StaticSecret::from([2; 32])).as_bytes());
    let inbound = serde_json::from_value(serde_json::json!({
        "tag":"wg-in", "listen":{"address":"127.0.0.1","port":51820},
        "protocol":{"type":"wireguard", "private_key":STANDARD.encode([1;32]),
        "peers":[{"public_key":peer,"allowed_ips":["10.0.0.2/32"]}]}
    }))
    .unwrap();
    let outbound = serde_json::from_value(serde_json::json!({
        "tag":"wg-out", "protocol":{"type":"wireguard", "private_key":STANDARD.encode([1;32]),
        "inbound_tag":"wg-in", "addresses":["10.0.0.1/24"], "mtu":1420,
        "peers":[{"public_key":peer,"endpoint":endpoint,"allowed_ips":["0.0.0.0/0"]}]}
    }))
    .unwrap();
    (outbound, inbound)
}
// Software-only packet Source memory; no listener/device IO or close claim.
fn software_options(adapter: &WireguardAdapter) -> RawIpStackOptions {
    RawIpStackOptions::from_config_with_packet_root(
        &Default::default(),
        adapter
            .packet_return_totals
            .get_or_init(Default::default)
            .clone(),
    )
    .with_packet_wire_memory(zero_stack::packet_output::memory::PacketWireMemory::Native)
}
fn software_source(adapter: &WireguardAdapter) -> zero_tun::packet_memory::NativeReadGrant {
    adapter
        .packet_return_totals
        .get()
        .expect("original software Source root")
        .native_grant(1420)
        .unwrap()
}
fn recipe(adapter: &WireguardAdapter, endpoint: &str, generation: u64) -> LinkedEndpointRecipe {
    let (outbound, inbound) = inputs(endpoint);
    let plan = Arc::new(WireguardRawIpPlan::from_protocol(&outbound.protocol).unwrap());
    let identity = super::super::super::protocol_identity(&outbound.protocol).unwrap();
    LinkedEndpointRecipe::prepare(
        adapter,
        &outbound,
        &inbound,
        plan,
        LinkedRecipeSettings {
            outer: None,
            identity,
            network_generation: NetworkGeneration(generation),
            stacks: vec![software_options(adapter)],
            dependencies: Vec::new(),
        },
    )
    .unwrap()
}
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + std::time::Duration::from_secs(3)
}
fn demand(recipe: &LinkedEndpointRecipe) -> LinkedBuildContext {
    LinkedBuildContext {
        deadline: Some(deadline()),
        cancel: CancellationToken::new(),
        binding: recipe.binding(),
    }
}
fn proxy_config() -> zero_config::RuntimeConfig {
    zero_config::RuntimeConfig::parse(r#"{"outbounds":[{"tag":"direct","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}}}"#).unwrap()
}
fn install(adapter: &WireguardAdapter, recipe: &LinkedEndpointRecipe) -> PendingEndpoints {
    PendingEndpoints::install(
        adapter.pending_endpoints.clone(),
        std::collections::HashMap::from([("wg-in".into(), recipe.pending())]),
    )
    .unwrap()
}
#[test]
fn cold_recipe_does_not_retain_its_original_candidate_map() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let adapter = WireguardAdapter::default();
    let map = Arc::downgrade(&adapter.pending_endpoints);
    let retained = recipe(&adapter, "127.0.0.1:51821", 0);
    drop(adapter);
    assert!(map.upgrade().is_none());
    assert!(retained.check_staged().is_err());
    assert!(!retained.pending().needs_cleanup());
}

#[test]
fn linked_recipe_is_cold_and_preserves_actual_crypto_seed_across_port_move() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let adapter = WireguardAdapter::default();
    let egress = zero_platform_tokio::EgressInterfaceControl::default();
    let first = recipe(&adapter, "127.0.0.1:51821", egress.generation());
    assert!(first.pending().ready().is_none());
    assert!(!first.pending().needs_cleanup());
    let live = first.pending().live();
    let (outbound, mut inbound) = inputs("127.0.0.1:51821");
    inbound.listen.port += 1;
    let plan = Arc::new(WireguardRawIpPlan::from_protocol(&outbound.protocol).unwrap());
    let moved = LinkedEndpointRecipe::prepare(
        &adapter,
        &outbound,
        &inbound,
        plan,
        LinkedRecipeSettings {
            outer: None,
            identity: first.identity,
            network_generation: NetworkGeneration(egress.generation()),
            stacks: vec![software_options(&adapter)],
            dependencies: Vec::new(),
        },
    )
    .unwrap();
    assert!(Arc::ptr_eq(&live, &moved.pending().live()));
    assert!(!Arc::ptr_eq(&first.pending(), &moved.pending()));
    assert!(adapter.pending_endpoints.lock().unwrap().is_none());
}
#[tokio::test]
async fn actual_candidate_must_be_installed_before_build_and_cannot_revive_retired_cells() {
    let proxy = crate::Proxy::new(proxy_config()).unwrap();
    let upstream = proxy.tcp_runtime_services().upstream();
    let adapter = WireguardAdapter::default();
    let uninstalled = recipe(&adapter, "127.0.0.1:51821", upstream.egress_generation());
    let request = demand(&uninstalled);
    let mut scope = TaskScope::suspended(
        tokio::runtime::Handle::current(),
        uninstalled.task_limit().unwrap(),
    );
    scope.enable_registration().unwrap();
    let registrar = scope.registrar();
    let source = software_source(&adapter);
    assert!(uninstalled
        .start(
            &upstream,
            &registrar,
            request,
            source.memory_view().unwrap()
        )
        .await
        .is_err());
    let recipe = recipe(&adapter, "127.0.0.1:51821", upstream.egress_generation());
    let pending = recipe.pending();
    let live = pending.live();
    let guard = install(&adapter, &recipe);
    let request = demand(&recipe);
    let link = recipe
        .start(
            &upstream,
            &registrar,
            request,
            source.memory_view().unwrap(),
        )
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&link.live, &live));
    assert!(Arc::ptr_eq(&pending.ready().unwrap(), &link));
    let device = link.state.lock().unwrap().devices[0].clone();
    assert!(pending.peer_stack(0).unwrap().owns(&device));
    assert!(device.is_usable());
    assert!(pending.needs_cleanup());
    let (closed, joined) = tokio::join!(
        pending.stop_peer_resources(deadline()),
        scope.shutdown(deadline())
    );
    assert!(closed.is_complete());
    assert!(joined.is_complete());
    assert!(joined.failed_tasks.is_empty());
    assert!(device.is_closed());
    assert!(PendingLinkedEndpoint::from_ready(link).is_err());
    drop(guard);
    assert!(adapter.pending_endpoints.lock().unwrap().is_none());
}
#[tokio::test]
async fn cancel_while_real_node_dns_is_pending_cannot_spawn_late_candidate_stacks() {
    pending_dns_cancel(false).await;
}
#[tokio::test]
async fn actual_linked_caller_cancellation_retains_candidate_until_its_own_release() {
    pending_dns_cancel(true).await;
}
async fn pending_dns_cancel(cancel_caller: bool) {
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut config = serde_json::json!({"outbounds":[{"tag":"direct","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}},"runtime":{}});
    config["runtime"]["dns"] = serde_json::json!({
        "servers":{"node":{"type":"udp","host":"127.0.0.1","port":peer.local_addr().unwrap().port()}},
        "default_server":"node", "policy":{"node_server":"node","timeout_ms":2000}
    });
    let proxy =
        crate::Proxy::new(zero_config::RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap();
    // The node resolver now consumes its real registered host Service owner.
    // A Proxy::new cold plan alone grants no DNS socket or background driver.
    let snapshot = proxy.engine().runtime_snapshot();
    let running = proxy.spawn();
    tokio::time::timeout_at(deadline(), async {
        while proxy
            .protocols
            .current_components(snapshot.config())
            .is_none()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let upstream = proxy.tcp_runtime_services().upstream();
    let adapter = WireguardAdapter::default();
    let recipe = recipe(
        &adapter,
        "pending-node.invalid:51821",
        upstream.egress_generation(),
    );
    let pending = recipe.pending();
    let guard = install(&adapter, &recipe);
    let request = demand(&recipe);
    let caller = request.cancel.clone();
    let mut scope = TaskScope::suspended(
        tokio::runtime::Handle::current(),
        recipe.task_limit().unwrap(),
    );
    scope.enable_registration().unwrap();
    let registrar = scope.registrar();
    let source = software_source(&adapter);
    let mut starting = Box::pin(recipe.start(
        &upstream,
        &registrar,
        request,
        source.memory_view().unwrap(),
    ));
    let mut query = [0; 512];
    tokio::select! {
        received = peer.recv_from(&mut query) => { assert!(received.unwrap().0 >= 12); },
        result = &mut starting => { panic!("node DNS completed before its actual pending query: {}", match result { Ok(_) => "unexpected link".to_owned(), Err(error) => error.to_string() }); },
        _ = tokio::time::sleep_until(deadline()) => panic!("actual node DNS query was not emitted"),
    }
    use std::{future::Future, task::Poll};
    std::future::poll_fn(|cx| {
        assert!(starting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let cancellation = pending.cancellation();
    if cancel_caller {
        caller.cancel();
        assert!(starting.await.is_err());
        assert!(
            !cancellation.is_cancelled(),
            "the caller cannot revoke the actual candidate owner"
        );
        assert!(pending.is_usable());
        assert!(pending.peer_stack(0).unwrap().device().is_none());
        assert!(adapter.pending_endpoints.lock().unwrap().is_some());
        drop(guard);
    } else {
        drop(guard);
        assert!(cancellation.is_cancelled());
        assert!(starting.await.is_err());
    }
    assert!(cancellation.is_cancelled());
    assert!(pending.ready().is_none());
    assert!(!pending.needs_cleanup());
    assert!(!pending.peer_stack(0).unwrap().is_open());
    assert!(adapter.pending_endpoints.lock().unwrap().is_none());
    // Only the existing global query workers are joined here. This is no claim
    // about global DNS native UDP closure or the endpoint listener's native I/O.
    assert!(scope.shutdown(deadline()).await.is_complete());
    running.shutdown().await.unwrap();
}

mod canonical;
