//! Actual host slots, scopes and native originals; group IDs prove no closure.
use super::*;
#[path = "lifecycle/fixture.rs"]
mod fixture;
use fixture::coupled;

#[tokio::test]
async fn coupled_pool_and_carrier_cleanup_are_polled_together_and_keep_both_original_receipts() {
    let catalog = ComponentCatalog::default();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (old, state) = coupled(&catalog, peer.local_addr().unwrap(), false);
    catalog.bind_plan_ownership(&old);
    let host = TestContext::new(old.order.iter().map(AsRef::as_ref));
    host.drive(old.start(deadline())).await.unwrap();
    let originals = state.originals.lock().unwrap().clone();
    assert!(originals
        .iter()
        .flatten()
        .all(|origin| origin.closed().is_none()));
    host.drive(old.shutdown(deadline())).await.unwrap();
    for origin in originals.into_iter().flatten() {
        let closed = origin.closed().expect("actual original native receipt");
        assert!(closed.matches(&origin) && closed.had_native_acquisition());
        assert!(origin.registration_observation().released().is_some());
    }
    assert!(old.confirmed(&old.order.iter().map(|slot| slot.instance_id()).collect()));
    assert_eq!(old.close_evidence.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn a_transferred_native_view_keeps_its_group_unconfirmed_after_the_other_member_closes() {
    let catalog = ComponentCatalog::default();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (old, state) = coupled(&catalog, peer.local_addr().unwrap(), true);
    catalog.bind_plan_ownership(&old);
    let host = TestContext::new(old.order.iter().map(AsRef::as_ref));
    host.drive(old.start(deadline())).await.unwrap();
    let originals = state.originals.lock().unwrap().clone();
    catalog.state.lock().unwrap().retired = Some(old.clone());
    let target = config();
    let candidate = catalog.prepare(target.clone()).unwrap();
    assert!(host
        .drive(catalog.start(target.clone(), deadline()))
        .await
        .is_err());
    assert!(!candidate.started.load(Ordering::Acquire));
    let batch = catalog.state.lock().unwrap().replacement.clone().unwrap();
    assert!(!batch.released());
    assert_eq!(batch.order.len(), 1, "the two owners share one lifecycle group");
    assert_eq!(batch.order[0].len(), 2);
    let first = originals[0].as_ref().unwrap();
    let second = originals[1].as_ref().unwrap();
    assert!(first.closed().is_none());
    assert!(second.closed().unwrap().matches(second));
    assert!(!old.confirmed(&old.order.iter().map(|slot| slot.instance_id()).collect()));
    drop(state.transferred.lock().unwrap().take());
    host.drive(catalog.start(target, deadline())).await.unwrap();
    assert!(batch.released());
    assert_eq!(batch.terminal.get().unwrap().instances().len(), 2);
    assert!(candidate.started.load(Ordering::Acquire));
    assert!(first.closed().unwrap().matches(first));
    assert!(first.registration_observation().released().is_some());
    assert!(old.confirmed(&old.order.iter().map(|slot| slot.instance_id()).collect()));
}
