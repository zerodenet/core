use super::*;
use shadowsocks::{
    component::ReplayFactory, transport::ShadowsocksTransportState, validation::ReplayPolicy,
};

fn request(
    preparation: &mut ResourcePreparation<'_>,
    state: &ShadowsocksTransportState,
    capacity: usize,
) -> ComponentId {
    preparation
        .share(
            "replay",
            Arc::new(ReplayFactory),
            Arc::new(state.replay_component_options(ReplayPolicy::Reject, Some(capacity))),
        )
        .unwrap()
}

#[test]
fn successful_canonical_identity_is_stable_across_passes_without_reallocation() {
    let ids = ComponentCatalog::default();
    let state = ShadowsocksTransportState::default();
    let calls = AtomicUsize::new(0);
    let mut allocate = || {
        calls.fetch_add(1, Ordering::SeqCst);
        ids.allocate_id()
    };
    let mut first = ResourcePreparation::new(&mut allocate);
    let id = request(&mut first, &state, 2);
    first.validate([]).unwrap();
    let identities = first.identities().unwrap();
    drop(first);
    let mut second = ResourcePreparation::with_identities(&mut allocate, &identities);
    assert_eq!(request(&mut second, &state, 2), id);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(second.validate([]).unwrap(), [id]);
}

#[test]
fn removed_resource_identities_do_not_resurrect_when_readded() {
    let ids = ComponentCatalog::default();
    let state = ShadowsocksTransportState::default();
    let mut allocate = || ids.allocate_id();
    let mut first = ResourcePreparation::new(&mut allocate);
    let removed = request(&mut first, &state, 2);
    let kept = request(&mut first, &state, 3);
    let original = first.identities().unwrap();
    drop(first);
    let mut second = ResourcePreparation::with_identities(&mut allocate, &original);
    assert_eq!(request(&mut second, &state, 3), kept);
    let current = second.identities().unwrap();
    drop(second);
    let mut third = ResourcePreparation::with_identities(&mut allocate, &current);
    assert_ne!(request(&mut third, &state, 2), removed);
    assert_eq!(request(&mut third, &state, 3), kept);
}

#[test]
fn an_identity_index_cannot_retain_the_previous_factory_or_recipe() {
    let ids = ComponentCatalog::default();
    let state = ShadowsocksTransportState::default();
    let factory = Arc::new(ReplayFactory);
    let weak = Arc::downgrade(&factory);
    let mut allocate = || ids.allocate_id();
    let mut first = ResourcePreparation::new(&mut allocate);
    first
        .share(
            "replay",
            factory.clone(),
            Arc::new(state.replay_component_options(ReplayPolicy::Reject, Some(2))),
        )
        .unwrap();
    let metadata = first.identities().unwrap();
    drop(first);
    drop(factory);
    assert!(weak.upgrade().is_none());
    let mut next = ResourcePreparation::with_identities(&mut allocate, &metadata);
    request(&mut next, &state, 2);
}

#[test]
fn allocated_and_reused_ids_share_the_same_collision_guard() {
    let ids = ComponentCatalog::default();
    let state = ShadowsocksTransportState::default();
    let mut allocate = || ids.allocate_id();
    let mut first = ResourcePreparation::new(&mut allocate);
    let id = request(&mut first, &state, 2);
    let metadata = first.identities().unwrap();
    drop(first);
    let mut faulty_host_allocator = || Ok(id);
    let mut next = ResourcePreparation::with_identities(&mut faulty_host_allocator, &metadata);
    assert_eq!(request(&mut next, &state, 3), id);
    let error = next
        .share(
            "replay",
            Arc::new(ReplayFactory),
            Arc::new(state.replay_component_options(ReplayPolicy::Reject, Some(2))),
        )
        .unwrap_err();
    assert_eq!(error.component, Some(id));
    assert!(next.identities().is_err());
    assert!(next.into_resources().is_err());
}

#[tokio::test]
async fn unchanged_shared_identity_reuses_the_running_replay_owner_and_its_one_scope() {
    use crate::component::{generations::order_and_reuse, instance::ComponentSlot};
    let ids = ComponentCatalog::default();
    let state = ShadowsocksTransportState::default();
    let mut allocate = || ids.allocate_id();
    let mut first = ResourcePreparation::new(&mut allocate);
    let id = request(&mut first, &state, 2);
    first.validate([]).unwrap();
    let metadata = first.identities().unwrap();
    let resource = first.into_resources().unwrap().pop().unwrap();
    let old = Arc::new(ComponentSlot::from_recipe(
        id,
        ResourceGeneration(1),
        resource.recipe().clone(),
        Default::default(),
        "resource_start",
    ));
    let previous = order_and_reuse(vec![old.clone()], &[]).unwrap();
    previous[0].start(deadline()).await.unwrap();
    let mut next = ResourcePreparation::with_identities(&mut allocate, &metadata);
    assert_eq!(request(&mut next, &state, 2), id);
    next.validate([]).unwrap();
    let resource = next.into_resources().unwrap().pop().unwrap();
    let candidate = Arc::new(ComponentSlot::from_recipe(
        id,
        ResourceGeneration(1),
        resource.recipe().clone(),
        Default::default(),
        "resource_start",
    ));
    let weak_candidate = Arc::downgrade(&candidate);
    let selected = order_and_reuse(vec![candidate], &previous).unwrap();
    assert!(Arc::ptr_eq(&selected[0], &old));
    assert!(weak_candidate.upgrade().is_none());
    selected[0].start(deadline()).await.unwrap();
    let report = selected[0].shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.exited_tasks.len(), 1);
    assert!(report.failed_tasks.is_empty());
}
