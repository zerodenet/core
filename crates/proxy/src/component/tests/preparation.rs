use crate::component::{
    catalog::ComponentCatalog, ComponentRegistry, RegistrationError, ResourcePreparation,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::time::{Duration, Instant};
use zero_core::component::{ComponentId, ComponentRoleIds, OutboundId, ResourceGeneration};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::{BuildError, TaskClass},
    scope::{SpawnError, TaskScope},
    CancellationToken,
};
use zero_traits::ProtocolMetadata;

mod fixtures;
mod validation;
use fixtures::registry;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}
fn context(id: ComponentId, scope: &TaskScope, stream: bool) -> BuildContext {
    BuildContext {
        component: id,
        generation: ResourceGeneration(1),
        roles: ComponentRoleIds {
            outbound: stream.then_some(OutboundId(id.0)),
            ..Default::default()
        },
        resources: Arc::new(crate::component::instance::NoResources),
        tasks: scope.registrar(),
        cancel: CancellationToken::new(),
        background_carrier: None,
    }
}

#[tokio::test]
async fn two_typed_consumers_share_one_real_replay_owner_with_host_assigned_identity() {
    let ids = ComponentCatalog::default();
    let first = ids.allocate_id().unwrap();
    let second = ids.allocate_id().unwrap();
    let conversions = Arc::new(AtomicUsize::new(0));
    let builds = Arc::new(AtomicUsize::new(0));
    let registry = registry(conversions.clone(), builds.clone());
    let mut allocate = || ids.allocate_id();
    let mut resources = ResourcePreparation::new(&mut allocate);
    let a = registry
        .prepare_with_resources("direct", &(), &mut resources)
        .unwrap();
    let b = registry
        .prepare_with_resources("direct", &(), &mut resources)
        .unwrap();
    assert_eq!(conversions.load(Ordering::SeqCst), 2);
    assert_eq!(
        builds.load(Ordering::SeqCst),
        0,
        "prepare must not build/start"
    );
    assert_eq!(resources.resources().count(), 1);
    let resource = resources.resources().next().unwrap();
    let shared = resource.component();
    assert_ne!(shared, first);
    assert_ne!(shared, second);
    assert_eq!(a.spec().dependencies, [shared]);
    assert_eq!(b.spec().dependencies, [shared]);
    assert_eq!(
        resources
            .validate([(first, a.spec()), (second, b.spec())])
            .unwrap(),
        [shared, first, second]
    );
    let resource = resources.into_resources().unwrap().pop().unwrap();
    let runtime = tokio::runtime::Handle::current();
    let mut scope = TaskScope::new(
        runtime.clone(),
        resource.recipe().spec().task_limit.unwrap(),
    );
    let mut owner = resource
        .recipe()
        .build(context(shared, &scope, false))
        .unwrap();
    assert!(
        owner.roles.stream.is_none()
            && owner.roles.datagram.is_none()
            && owner.roles.packet.is_none()
            && owner.roles.inbound.is_none()
    );
    owner.lifecycle.start(deadline()).await.unwrap();
    let mut first_scope = TaskScope::empty(runtime.clone());
    let mut second_scope = TaskScope::empty(runtime);
    let mut first_owner = a.build(context(first, &first_scope, true)).unwrap();
    let mut second_owner = b.build(context(second, &second_scope, true)).unwrap();
    assert!(first_owner.roles.stream.is_some() && second_owner.roles.stream.is_some());
    first_owner.lifecycle.start(deadline()).await.unwrap();
    second_owner.lifecycle.start(deadline()).await.unwrap();
    assert_eq!(builds.load(Ordering::SeqCst), 2);
    assert_eq!(
        conversions.load(Ordering::SeqCst),
        2,
        "runtime build/start cannot decode input again"
    );
    assert!(second_owner
        .lifecycle
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(first_owner
        .lifecycle
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(first_scope.shutdown(deadline()).await.is_complete());
    assert!(second_scope.shutdown(deadline()).await.is_complete());
    assert_eq!(
        scope
            .registrar()
            .spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    let end = deadline();
    let (protocol, tasks) = tokio::join!(owner.lifecycle.shutdown(end), scope.shutdown(end));
    assert!(protocol.unwrap().is_complete());
    assert!(tasks.is_complete());
    assert_eq!(tasks.exited_tasks.len(), 1);
    assert!(tasks.failed_tasks.is_empty());
}

#[test]
fn standalone_registration_cannot_invent_resource_ids_without_a_host_allocator() {
    let conversions = Arc::new(AtomicUsize::new(0));
    let builds = Arc::new(AtomicUsize::new(0));
    let registry = registry(conversions, builds.clone());
    let error = registry.prepare("direct", &()).err().unwrap();
    let RegistrationError::Planning(error) = error else {
        panic!("expected actual planning error")
    };
    assert!(error.component.is_none());
    assert!(std::error::Error::source(&error)
        .unwrap()
        .to_string()
        .contains("host ComponentId allocator"));
    assert_eq!(builds.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn shared_recipe_uses_persistent_slot_and_cannot_replace_a_running_replay_owner() {
    use crate::component::{instance::ComponentSlot, ComponentRecipe};
    use shadowsocks::{
        component::ReplayFactory, transport::ShadowsocksTransportState, validation::ReplayPolicy,
    };
    use zero_platform_tokio::contract::Reason;
    let ids = ComponentCatalog::default();
    let id = ids.allocate_id().unwrap();
    let state = ShadowsocksTransportState::default();
    let recipe = ComponentRecipe::from_validated(
        Arc::new(ReplayFactory),
        Arc::new(state.replay_component_options(ReplayPolicy::Reject, Some(2))),
    );
    let owner = |generation| {
        ComponentSlot::from_recipe(
            id,
            ResourceGeneration(generation),
            recipe.clone(),
            Default::default(),
            "resource_start",
        )
    };
    let old = owner(1);
    old.start(deadline()).await.unwrap();
    let premature = owner(2);
    let error = premature.start(deadline()).await.unwrap_err();
    assert_eq!(error.reason, Reason::StalePlan);
    assert_eq!(error.component, Some(id));
    let failed = premature.shutdown(deadline()).await.unwrap();
    assert!(failed.is_complete());
    assert!(failed.exited_tasks.is_empty());
    old.start(deadline()).await.unwrap();
    let closed = old.shutdown(deadline()).await.unwrap();
    assert!(closed.is_complete());
    assert_eq!(closed.exited_tasks.len(), 1);
    assert!(closed.failed_tasks.is_empty());
    let replacement = owner(2);
    replacement.start(deadline()).await.unwrap();
    let closed = replacement.shutdown(deadline()).await.unwrap();
    assert!(closed.is_complete());
    assert_eq!(closed.exited_tasks.len(), 1);
    assert!(closed.failed_tasks.is_empty());
}
