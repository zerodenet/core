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
fn native_pool_rejects_unowned_root_domain_unbounded_budget_and_prior_legacy_exposure() {
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
        .options(
            "p".into(),
            Destination::new(Address::Domain("localhost".into()), 443),
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
