//! Cold allocation identity only; no native acquisition or close is claimed.
use super::*;

#[test]
fn cold_slot_views_share_the_original_and_allow_empty_slot_reclamation() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let registry = Registry::default();
    let endpoint = Destination::new(zero_core::Address::Ipv4([127, 0, 0, 1]), 443);
    let first = registry.access(b"same-owner", endpoint.clone());
    let second = registry.access(b"same-owner", endpoint.clone());
    assert!(Arc::ptr_eq(&first.slot, &second.slot));
    assert!(first.slot.owner().is_none());
    let original = registry
        .0
        .lock()
        .unwrap()
        .get(b"same-owner".as_slice())
        .unwrap()
        .clone();
    let old = Arc::downgrade(&original);
    let view: Arc<dyn SlotAccess> = original.clone();
    assert!(Arc::ptr_eq(&first.slot, &view));
    drop((first, second, view, original));
    let next = registry.access(b"same-owner", endpoint);
    assert!(old.upgrade().is_none());
    assert!(next.slot.owner().is_none());
}
