use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use tokio::sync::{mpsc, watch};

fn candidate(peers: usize) -> Arc<PendingLinkedEndpoint> {
    let config: zero_config::InboundConfig = serde_json::from_value(serde_json::json!({
        "tag":"wg-in", "listen":{"address":"127.0.0.1","port":51820},
        "protocol":{"type":"wireguard", "private_key":STANDARD.encode([1;32]),
        "peers":[{"public_key":STANDARD.encode(PublicKey::from(&StaticSecret::from([2;32])).as_bytes()),"allowed_ips":["10.0.0.2/32"]}]}
    })).unwrap();
    let adapter = super::super::super::WireguardAdapter::default();
    let live = adapter.passive_inbound_live(&config).unwrap();
    Arc::new(PendingLinkedEndpoint::with_peer_slots(config.listen, live, peers).unwrap())
}
fn install(owner: &PendingEndpointMap, pending: Arc<PendingLinkedEndpoint>) -> PendingEndpoints {
    PendingEndpoints::install(owner.clone(), HashMap::from([("wg-in".into(), pending)])).unwrap()
}
fn recipe() -> crate::runtime::raw_ip::EndpointStackRecipe {
    let (sender, _packets) = mpsc::channel(4);
    let (_, endpoint) = watch::channel(sender);
    crate::runtime::raw_ip::EndpointStackRecipe::prepare(
        vec!["10.0.0.1".parse().unwrap()],
        1420,
        0,
        endpoint,
        Default::default(),
    )
    .unwrap()
}
#[test]
fn abandoned_unstarted_candidate_is_fenced_before_its_map_is_released() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let owner = PendingEndpointMap::default();
    let pending = candidate(1);
    let guard = install(&owner, pending.clone());
    assert!(PendingEndpoints::ensure_available(&owner).is_err());
    drop(guard);
    assert!(owner.lock().unwrap().is_none());
    assert!(!pending.peer_stack(0).unwrap().is_open());
    assert!(!pending.peer_carrier(0).unwrap().is_open());
    assert!(pending.peer_stack(0).unwrap().start(recipe()).is_err());
}
#[tokio::test]
async fn cancelled_candidate_keeps_original_tasks_until_exact_cleanup_and_blocks_overwrite() {
    let owner = PendingEndpointMap::default();
    let pending = candidate(1);
    let guard = install(&owner, pending.clone());
    let expected = guard.expected.clone();
    let mut scope = zero_platform_tokio::contract::scope::TaskScope::new(
        tokio::runtime::Handle::current(),
        std::num::NonZeroUsize::new(crate::runtime::raw_ip::EndpointStackOwner::TASK_LIMIT)
            .unwrap(),
    );
    pending
        .peer_stack(0)
        .unwrap()
        .bind_scope(scope.registrar().downgrade())
        .unwrap();
    let device = pending.peer_stack(0).unwrap().start(recipe()).unwrap();
    drop(guard);
    assert!(Arc::ptr_eq(
        owner.lock().unwrap().as_ref().unwrap(),
        &expected
    ));
    assert!(PendingEndpoints::ensure_available(&owner).is_err());
    assert!(PendingEndpoints::release_closed(&owner, &expected).is_err());
    assert!(PendingEndpoints::install(owner.clone(), HashMap::new()).is_err());
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    let (closed, joined) = tokio::join!(
        pending.stop_peer_resources(deadline),
        scope.shutdown(deadline)
    );
    assert!(closed.is_complete());
    assert!(joined.is_complete());
    assert!(joined.failed_tasks.is_empty());
    assert!(device.is_closed());
    PendingEndpoints::release_closed(&owner, &expected).unwrap();
    assert!(owner.lock().unwrap().is_none());
    assert!(PendingEndpoints::ensure_available(&owner).is_ok());
}
#[test]
fn old_guard_drop_and_cleanup_cannot_release_a_new_actual_candidate() {
    let owner = PendingEndpointMap::default();
    let first = install(&owner, candidate(1));
    PendingEndpoints::release_closed(&owner, &first.expected).unwrap();
    let second = install(&owner, candidate(1));
    let current = second.expected.clone();
    assert!(PendingEndpoints::release_closed(&owner, &first.expected).is_err());
    drop(first);
    assert!(Arc::ptr_eq(
        owner.lock().unwrap().as_ref().unwrap(),
        &current
    ));
    drop(second);
    assert!(owner.lock().unwrap().is_none());
}
#[test]
fn successful_link_holds_same_original_graph_after_pending_release_without_an_arc_cycle() {
    use super::super::super::inbound::LinkedEndpoint;
    let owner = PendingEndpointMap::default();
    let pending = candidate(0);
    let mut guard = install(&owner, pending.clone());
    let (sender, packets) = mpsc::channel(1);
    let (sender, _) = watch::channel(sender);
    let link = Arc::new(LinkedEndpoint::new(
        [3; 32],
        7,
        pending.listen().clone(),
        pending.live(),
        vec![],
        vec![],
        vec![],
        sender,
        packets,
    ));
    pending.install(link.clone()).unwrap();
    let graph = link.owned_resources().unwrap();
    let weak = Arc::downgrade(&graph);
    guard.published();
    drop(guard);
    assert!(owner.lock().unwrap().is_none());
    drop(graph);
    drop(pending);
    assert!(weak.upgrade().is_some()); // actual Link, not a global/history map.
    drop(link);
    assert!(weak.upgrade().is_none()); // no Link->pending->Link ownership cycle.
}
