use super::*;
use crate::component::{DirectFactory, DirectOptions};
use shadowsocks::{
    component::{ReplayFactory, ReplayOptions},
    transport::ShadowsocksTransportState,
    validation::ReplayPolicy,
};
use zero_platform_tokio::contract::{ChannelError, DeliveryState, Impact, Reason, Stage};

fn options(state: &ShadowsocksTransportState, capacity: usize) -> Arc<ReplayOptions> {
    Arc::new(state.replay_component_options(ReplayPolicy::Reject, Some(capacity)))
}

#[test]
fn allocator_failure_preserves_original_source_and_invalidates_the_candidate() {
    let state = ShadowsocksTransportState::default();
    let mut allocate = || {
        Err(
            ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Component)
                .with_source(std::io::Error::from_raw_os_error(24)),
        )
    };
    let mut resources = ResourcePreparation::new(&mut allocate);
    let error = resources
        .share("replay", Arc::new(ReplayFactory), options(&state, 2))
        .unwrap_err();
    assert_eq!(error.reason, Reason::Capacity);
    assert_eq!(
        std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .raw_os_error(),
        Some(24)
    );
    assert_eq!(resources.resources().count(), 0);
    assert!(resources.validate([]).is_err());
    assert!(resources.into_resources().is_err());
}

#[test]
fn repeated_host_identity_for_distinct_resources_rejects_the_whole_pass() {
    let ids = ComponentCatalog::default();
    let id = ids.allocate_id().unwrap();
    let state = ShadowsocksTransportState::default();
    let mut allocate = || Ok(id);
    let mut resources = ResourcePreparation::new(&mut allocate);
    assert_eq!(
        resources
            .share("replay", Arc::new(ReplayFactory), options(&state, 2))
            .unwrap(),
        id
    );
    let error = resources
        .share("replay", Arc::new(ReplayFactory), options(&state, 3))
        .unwrap_err();
    assert_eq!(error.component, Some(id));
    assert_eq!(error.delivery, DeliveryState::NotSubmitted);
    assert!(resources.into_resources().is_err());
}

struct WithDependency(ComponentId);
impl ComponentFactory for WithDependency {
    type Options = ReplayOptions;
    fn describe(&self, options: &ReplayOptions) -> ComponentSpec {
        let mut spec = ReplayFactory.describe(options);
        spec.dependencies.push(self.0);
        spec
    }
    fn build(
        &self,
        options: Arc<ReplayOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        ReplayFactory.build(options, context)
    }
}

#[test]
fn equal_identity_with_conflicting_dependency_rejects_without_a_second_allocation() {
    let ids = ComponentCatalog::default();
    let dependency = ids.allocate_id().unwrap();
    let state = ShadowsocksTransportState::default();
    let calls = AtomicUsize::new(0);
    let mut allocate = || {
        calls.fetch_add(1, Ordering::SeqCst);
        ids.allocate_id()
    };
    let mut resources = ResourcePreparation::new(&mut allocate);
    let first = resources
        .share("replay", Arc::new(ReplayFactory), options(&state, 2))
        .unwrap();
    let error = resources
        .share(
            "replay",
            Arc::new(WithDependency(dependency)),
            options(&state, 2),
        )
        .unwrap_err();
    assert_eq!(error.component, Some(first));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(resources.into_resources().is_err());
}

#[test]
fn independently_owned_protocol_states_are_isolated_by_assembly_namespace() {
    let ids = ComponentCatalog::default();
    let a = ShadowsocksTransportState::default();
    let b = ShadowsocksTransportState::default();
    let mut allocate = || ids.allocate_id();
    let mut resources = ResourcePreparation::new(&mut allocate);
    let first = resources
        .share("assembly-a.replay", Arc::new(ReplayFactory), options(&a, 2))
        .unwrap();
    let second = resources
        .share("assembly-b.replay", Arc::new(ReplayFactory), options(&b, 2))
        .unwrap();
    assert_ne!(first, second);
    assert_eq!(resources.validate([]).unwrap(), [first, second]);
    assert_eq!(resources.into_resources().unwrap().len(), 2);
}

#[test]
fn callable_role_cannot_be_disguised_as_a_shared_roleless_owner() {
    let mut allocate = || panic!("invalid resource must be rejected before allocating");
    let mut resources = ResourcePreparation::new(&mut allocate);
    let error = resources
        .share(
            "direct",
            Arc::new(DirectFactory),
            Arc::new(DirectOptions::validate(Default::default()).unwrap()),
        )
        .unwrap_err();
    assert_eq!(error.reason, Reason::InvalidConfig);
    assert!(resources.into_resources().is_err());
}

#[test]
fn combined_primary_resource_cycle_is_rejected_before_any_build() {
    let ids = ComponentCatalog::default();
    let primary = ids.allocate_id().unwrap();
    let state = ShadowsocksTransportState::default();
    let mut allocate = || ids.allocate_id();
    let mut resources = ResourcePreparation::new(&mut allocate);
    let shared = resources
        .share(
            "replay",
            Arc::new(WithDependency(primary)),
            options(&state, 2),
        )
        .unwrap();
    let mut spec = DirectFactory.describe(&DirectOptions::validate(Default::default()).unwrap());
    spec.dependencies.push(shared);
    let error = resources.validate([(primary, &spec)]).unwrap_err();
    assert_eq!(error.reason, Reason::InvalidConfig);
    assert!(std::error::Error::source(&error)
        .unwrap()
        .to_string()
        .contains("cycle"));
    assert!(resources.into_resources().is_err());
}

#[test]
fn failed_consumer_prevents_extracting_prior_successful_resource_requests() {
    let ids = ComponentCatalog::default();
    let conversions = Arc::new(AtomicUsize::new(0));
    let builds = Arc::new(AtomicUsize::new(0));
    let registry = registry(conversions.clone(), builds.clone());
    let mut allocate = || ids.allocate_id();
    let mut resources = ResourcePreparation::new(&mut allocate);
    registry
        .prepare_with_resources("direct", &(), &mut resources)
        .unwrap();
    assert!(matches!(
        registry.prepare_with_resources("unknown", &(), &mut resources),
        Err(RegistrationError::Unknown(_))
    ));
    assert_eq!(resources.resources().count(), 1);
    assert!(resources.identities().is_err());
    assert!(registry
        .prepare_with_resources("direct", &(), &mut resources)
        .is_err());
    assert!(resources.validate([]).is_err());
    assert!(resources.into_resources().is_err());
    assert_eq!(conversions.load(Ordering::SeqCst), 1);
    assert_eq!(builds.load(Ordering::SeqCst), 0);
}
