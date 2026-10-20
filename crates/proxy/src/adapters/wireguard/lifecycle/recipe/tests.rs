use super::super::pending_map::PendingEndpoints;
use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use zero_core::request::{ServiceOrigin, ServicePurpose, ServiceRequestFacts};
use zero_platform_tokio::contract::{network::CarrierContext, CancellationToken};

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
            identity,
            network_generation: NetworkGeneration(generation),
            stacks: vec![Default::default()],
        },
    )
    .unwrap()
}
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + std::time::Duration::from_secs(3)
}
fn demand(recipe: &LinkedEndpointRecipe) -> PacketCarrierDemand {
    PacketCarrierDemand {
        identity: RequestIdentity::Service(Arc::new(ServiceRequestFacts {
            origin: ServiceOrigin::Runtime,
            purpose: ServicePurpose::BackgroundControl,
        })),
        context: CarrierContext {
            session: None,
            deadline: Some(deadline()),
            cancel: CancellationToken::new(),
        },
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
            identity: first.identity,
            network_generation: NetworkGeneration(egress.generation()),
            stacks: vec![Default::default()],
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
    assert!(uninstalled.start(&upstream, request).await.is_err());
    let recipe = recipe(&adapter, "127.0.0.1:51821", upstream.egress_generation());
    let pending = recipe.pending();
    let live = pending.live();
    let guard = install(&adapter, &recipe);
    let request = demand(&recipe);
    let link = recipe.start(&upstream, request).await.unwrap();
    assert!(Arc::ptr_eq(&link.live, &live));
    assert!(Arc::ptr_eq(&pending.ready().unwrap(), &link));
    let device = link.state.lock().unwrap().devices[0].clone();
    assert!(pending.peer_stack(0).unwrap().owns(&device));
    assert!(device.is_usable());
    assert!(pending.needs_cleanup());
    assert!(pending.stop_peer_resources(deadline()).await.is_complete());
    assert!(device.is_closed());
    assert!(PendingLinkedEndpoint::from_ready(link).is_err());
    drop(guard);
    assert!(adapter.pending_endpoints.lock().unwrap().is_none());
}
#[tokio::test]
async fn cancel_while_real_node_dns_is_pending_cannot_spawn_late_candidate_stacks() {
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut config = serde_json::json!({"outbounds":[{"tag":"direct","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}},"runtime":{}});
    config["runtime"]["dns"] = serde_json::json!({
        "servers":{"node":{"type":"udp","host":"127.0.0.1","port":peer.local_addr().unwrap().port()}},
        "default_server":"node", "policy":{"node_server":"node","timeout_ms":2000}
    });
    let proxy =
        crate::Proxy::new(zero_config::RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap();
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
    let mut starting = Box::pin(recipe.start(&upstream, request));
    let mut query = [0; 512];
    tokio::select! {
        received = peer.recv_from(&mut query) => { assert!(received.unwrap().0 >= 12); },
        result = &mut starting => { panic!("node DNS completed before its actual pending query: {}", match result { Ok(_) => "unexpected link".to_owned(), Err(error) => error.to_string() }); },
        _ = tokio::time::sleep_until(deadline()) => panic!("actual node DNS query was not emitted"),
    }
    drop(guard);
    let result = starting.await;
    assert!(result.is_err());
    assert!(pending.ready().is_none());
    assert!(!pending.needs_cleanup());
    assert!(!pending.peer_stack(0).unwrap().is_open());
    assert!(adapter.pending_endpoints.lock().unwrap().is_none());
    // Only the existing global query workers are joined here. This is no claim
    // about global DNS native UDP closure or the endpoint listener's native I/O.
    proxy.resolver.shutdown_queries(deadline()).await.unwrap();
}
