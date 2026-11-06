//! The actual production factory opts in; the generic library factory does not.
use super::*;
use std::{num::NonZeroUsize, time::Duration};
use zero_core::{request::Destination, Address};
use zero_platform_tokio::contract::network::UpstreamBinding;
#[test]
fn owning_factory_describes_the_same_scoped_preparation_it_builds_without_an_extra_quota() {
    let network = Arc::new(BackgroundNetwork::new(Default::default()));
    let factory = Factory::new(network);
    let pool = zero_transport::pooled_driver::PoolControl::default()
        .scoped("node".into())
        .options(
            "full-profile".into(),
            Destination::new(Address::Ipv4([127, 0, 0, 1]), 443),
            UpstreamBinding {
                prefix: vec![],
                dial: Default::default(),
                network_generation: factory.generation,
            },
            NonZeroUsize::new(1).unwrap(),
            Duration::from_secs(2),
        )
        .unwrap();
    let expected = PoolFactory.describe(&pool.clone().with_scoped_preparation());
    let options = Options {
        pool: Arc::new(pool),
        tag: "node".into(),
        wire_endpoint: Destination::new(Address::Ipv4([127, 0, 0, 1]), 443),
    };
    let actual = factory.describe(&options);
    assert!(actual
        .static_identity
        .starts_with(&expected.static_identity));
    assert_eq!(actual.resources, expected.resources);
    assert_eq!(actual.dependencies, expected.dependencies);
    assert_eq!(actual.task_limit, NonZeroUsize::new(1));
}
