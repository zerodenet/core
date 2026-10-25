use crate::component::{
    catalog::ComponentCatalog,
    generations::order_and_reuse,
    instance::{ComponentSlot, NoResources},
    ComponentRecipe, DirectFactory, DirectOptions,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::time::{Duration, Instant};
use zero_core::component::{
    ComponentId, ComponentInstanceId, ComponentRoleIds, OutboundId, ResourceGeneration,
};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
    scope::TaskScope,
    CancellationToken, DeliveryState, Reason,
};
use zero_traits::{AddressFamily, DialPolicy};

type Observed = Arc<Mutex<Vec<(ComponentInstanceId, Vec<ComponentInstanceId>)>>>;
struct Options {
    direct: DirectOptions,
    dependencies: Vec<ComponentId>,
}
struct Factory {
    builds: Arc<AtomicUsize>,
    observed: Observed,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        let mut spec = DirectFactory.describe(&options.direct);
        spec.dependencies = options.dependencies.clone();
        spec
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        self.builds.fetch_add(1, Ordering::SeqCst);
        self.observed
            .lock()
            .unwrap()
            .push((context.instance(), context.dependencies.to_vec()));
        DirectFactory.build(Arc::new(options.direct.clone()), context)
    }
}
fn recipe(
    dependencies: Vec<ComponentId>,
    builds: Arc<AtomicUsize>,
    observed: Observed,
) -> ComponentRecipe {
    ComponentRecipe::from_validated(
        Arc::new(Factory { builds, observed }),
        Arc::new(Options {
            direct: DirectOptions::validate(Default::default()).unwrap(),
            dependencies,
        }),
    )
}
fn root(id: ComponentId, family: AddressFamily) -> Arc<ComponentSlot> {
    let policy = DialPolicy {
        address_family: family,
        ..Default::default()
    };
    Arc::new(ComponentSlot::from_recipe(
        id,
        ResourceGeneration(1),
        ComponentRecipe::from_validated(
            Arc::new(DirectFactory),
            Arc::new(DirectOptions::validate(policy.clone()).unwrap()),
        ),
        policy,
        "connect_direct",
    ))
}
fn context(
    id: ComponentId,
    dependencies: Vec<ComponentInstanceId>,
    scope: &TaskScope,
) -> BuildContext {
    BuildContext {
        component: id,
        generation: ResourceGeneration(1),
        dependencies: dependencies.into(),
        roles: ComponentRoleIds {
            outbound: Some(OutboundId(id.0)),
            ..Default::default()
        },
        resources: Arc::new(NoResources),
        tasks: scope.registrar(),
        cancel: CancellationToken::new(),
        background_carrier: None,
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}

#[tokio::test]
async fn missing_extra_duplicate_and_wrong_dependency_instances_fail_before_factory_build() {
    let ids = ComponentCatalog::default();
    let owner = ids.allocate_id().unwrap();
    let a = root(ids.allocate_id().unwrap(), AddressFamily::Auto).instance_id();
    let b = root(ids.allocate_id().unwrap(), AddressFamily::Auto).instance_id();
    let other = root(ids.allocate_id().unwrap(), AddressFamily::Auto).instance_id();
    let builds = Arc::new(AtomicUsize::new(0));
    let recipe = recipe(
        vec![a.component, b.component],
        builds.clone(),
        Default::default(),
    );
    let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
    for dependencies in [
        vec![],
        vec![a],
        vec![a, b, other],
        vec![a, a],
        vec![a, other],
    ] {
        let error = recipe
            .build(context(owner, dependencies, &scope))
            .err()
            .unwrap();
        assert_eq!(error.component, Some(owner));
        assert_eq!(error.reason, Reason::Internal);
        assert_eq!(error.delivery, DeliveryState::NotSubmitted);
    }
    assert_eq!(builds.load(Ordering::SeqCst), 0);
    assert!(scope.shutdown(deadline()).await.is_complete());
}

#[tokio::test]
async fn validated_build_receives_the_exact_instances_and_lookup_does_not_require_role_ids() {
    let ids = ComponentCatalog::default();
    let owner = ids.allocate_id().unwrap();
    let a = root(ids.allocate_id().unwrap(), AddressFamily::Auto).instance_id();
    let b = root(ids.allocate_id().unwrap(), AddressFamily::Auto).instance_id();
    let missing = ids.allocate_id().unwrap();
    let builds = Arc::new(AtomicUsize::new(0));
    let observed: Observed = Default::default();
    let recipe = recipe(
        vec![a.component, b.component],
        builds.clone(),
        observed.clone(),
    );
    let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
    let context = context(owner, vec![b, a], &scope);
    assert_eq!(context.dependency(a.component), Some(a));
    assert_eq!(context.dependency(b.component), Some(b));
    assert_eq!(context.dependency(missing), None);
    let instance = context.instance();
    let mut built = recipe.build(context).unwrap();
    assert!(built.roles.stream.is_some());
    assert_eq!(*observed.lock().unwrap(), [(instance, vec![b, a])]);
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    assert!(built
        .lifecycle
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
}

#[tokio::test]
async fn a_rebuilt_consumer_factory_gets_the_new_actual_dependency_generation() {
    let ids = ComponentCatalog::default();
    let resource = ids.allocate_id().unwrap();
    let consumer = ids.allocate_id().unwrap();
    let builds = Arc::new(AtomicUsize::new(0));
    let observed: Observed = Default::default();
    let consumer_recipe = recipe(vec![resource], builds.clone(), observed.clone());
    let dependent = Arc::new(ComponentSlot::from_recipe(
        consumer,
        ResourceGeneration(1),
        consumer_recipe,
        Default::default(),
        "connect_direct",
    ));
    let old = order_and_reuse(vec![dependent, root(resource, AddressFamily::Auto)], &[]).unwrap();
    for slot in &old {
        slot.start(deadline()).await.unwrap();
    }
    let old_resource = old
        .iter()
        .find(|slot| slot.id == resource)
        .unwrap()
        .instance_id();
    assert_eq!(observed.lock().unwrap()[0].1, [old_resource]);
    let old_consumer = old.iter().find(|slot| slot.id == consumer).unwrap().clone();
    let next = order_and_reuse(
        vec![old_consumer, root(resource, AddressFamily::OnlyIpv4)],
        &old,
    )
    .unwrap();
    assert_eq!(
        builds.load(Ordering::SeqCst),
        1,
        "cold preparation must not build/start"
    );
    for slot in old.iter().rev() {
        assert!(slot.shutdown(deadline()).await.unwrap().is_complete());
    }
    for slot in &next {
        slot.start(deadline()).await.unwrap();
    }
    let new_resource = next
        .iter()
        .find(|slot| slot.id == resource)
        .unwrap()
        .instance_id();
    let new_consumer = next
        .iter()
        .find(|slot| slot.id == consumer)
        .unwrap()
        .instance_id();
    assert_eq!(
        old_resource.generation.checked_next(),
        Some(new_resource.generation)
    );
    assert_eq!(builds.load(Ordering::SeqCst), 2);
    assert_eq!(
        observed.lock().unwrap()[1],
        (new_consumer, vec![new_resource])
    );
    for slot in next.iter().rev() {
        assert!(slot.shutdown(deadline()).await.unwrap().is_complete());
    }
}
