use super::*;
use std::{num::NonZeroUsize, time::Duration};
use zero_core::{component::NetworkGeneration, request::Destination, Address};
use zero_platform_tokio::contract::{component::ComponentFactory, network::UpstreamBinding};
fn binding() -> UpstreamBinding {
    UpstreamBinding {
        prefix: vec![],
        dial: Default::default(),
        network_generation: NetworkGeneration(1),
    }
}
fn endpoint() -> Destination {
    Destination::new(Address::Ipv4([127, 0, 0, 1]), 443)
}
fn describe(pool: &PoolControl, profile: &str) -> PoolOptions {
    pool.options(
        profile.into(),
        endpoint(),
        binding(),
        NonZeroUsize::new(1).unwrap(),
        Duration::from_secs(2),
    )
    .unwrap()
}
#[test]
fn cold_pool_identity_tracks_the_actual_pool_and_complete_private_profile_without_debug_secrets() {
    let actual = PoolControl::default().scoped("owner".into());
    let first = describe(&actual, "private credential one");
    let same = describe(&actual.clone(), "private credential one");
    let second = describe(&actual, "private credential two");
    let foreign = describe(
        &PoolControl::default().scoped("owner".into()),
        "private credential one",
    );
    assert_eq!(PoolFactory.describe(&first), PoolFactory.describe(&same));
    assert_ne!(
        PoolFactory.describe(&first).static_identity,
        PoolFactory.describe(&second).static_identity
    );
    assert_ne!(
        PoolFactory.describe(&first).static_identity,
        PoolFactory.describe(&foreign).static_identity
    );
    let debug = format!("{first:?}");
    assert!(!debug.contains("private credential"));
    assert!(
        actual.managed().is_err(),
        "a cold recipe must not make a live owner"
    );
}
#[test]
fn native_pool_rejects_unowned_root_unbounded_budget_and_prior_legacy_exposure() {
    let actual = PoolControl::default().scoped("owner".into());
    let limit = NonZeroUsize::new(1).unwrap();
    assert!(PoolControl::default()
        .options(
            "p".into(),
            endpoint(),
            binding(),
            limit,
            Duration::from_secs(1)
        )
        .is_err());
    assert!(actual
        .options("p".into(), endpoint(), binding(), limit, Duration::ZERO)
        .is_err());
    assert!(actual
        .options("p".into(), endpoint(), binding(), limit, Duration::MAX)
        .is_err());
    actual.enter_legacy().unwrap();
    assert!(
        actual
            .options(
                "p".into(),
                endpoint(),
                binding(),
                limit,
                Duration::from_secs(1)
            )
            .is_err(),
        "cache eviction is not native adoption of already exposed legacy resources"
    );
}

#[test]
fn scoped_legacy_exposure_survives_view_drop_and_keeps_other_outbounds_eligible() {
    let registry = PoolControl::default();
    let legacy = registry.scoped("old".into());
    legacy.enter_legacy().unwrap();
    assert!(!legacy.native_eligible());
    drop(legacy);
    let old = registry.scoped("old".into());
    assert!(
        !old.native_eligible(),
        "view eviction cannot certify native close"
    );
    assert!(old.installed().unwrap().is_none());
    let fresh = registry.scoped("fresh".into());
    assert!(fresh.native_eligible());
    let _recipe = describe(&fresh, "another actual scoped outbound");
    registry.enter_legacy().unwrap();
    assert!(
        !fresh.native_eligible(),
        "unscoped legacy access overlaps every key"
    );
}

#[test]
fn domain_pool_keeps_the_real_endpoint_and_exact_node_dependency_in_its_cold_recipe() {
    let actual = PoolControl::default().scoped("domain-owner".into());
    let endpoint = Destination::new(Address::Domain("actual.example".into()), 443);
    let base = actual
        .options(
            "complete protocol profile".into(),
            endpoint.clone(),
            binding(),
            NonZeroUsize::new(1).unwrap(),
            Duration::from_secs(2),
        )
        .unwrap();
    // These values are only cold graph identities; no provider, IO or TaskId
    // is fabricated by describing a recipe. Build still requires the resolved
    // actual dependency instance and original controlled carrier provider.
    let first = base
        .clone()
        .with_node_dependency(zero_core::component::ComponentId(11));
    let same = first
        .clone()
        .with_node_dependency(zero_core::component::ComponentId(11));
    let other = base.with_node_dependency(zero_core::component::ComponentId(12));
    let described = PoolFactory.describe(&first);
    assert_eq!(
        described.dependencies,
        vec![zero_core::component::ComponentId(11)]
    );
    assert!(
        matches!(&described.resources[0],zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool{endpoint:actual,..} if actual==&endpoint)
    );
    assert_eq!(described, PoolFactory.describe(&same));
    assert_ne!(
        described.static_identity,
        PoolFactory.describe(&other).static_identity
    );
    assert!(actual.managed().is_err());
}
