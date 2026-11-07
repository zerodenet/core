//! Cold mode is explicit; neither choice invents another task quota or IO grant.
use super::*;
use crate::pooled_driver::{Driver, ManagedPool, PoolControl, PoolFactory};
use std::{future::Future, num::NonZeroUsize, sync::Arc, time::Duration};
use zero_platform_tokio::{
    contract::{component::ComponentFactory, network::UpstreamBinding},
    controlled_carrier::CarrierIo,
};
#[test]
fn fixed_library_and_actual_host_preparation_keep_one_scope_limit_and_order_independent_identity() {
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: Default::default(),
        network_generation: zero_core::component::NetworkGeneration(1),
    };
    let base = PoolControl::default()
        .scoped("actual-key".into())
        .options(
            "private-profile".into(),
            Destination::new(zero_core::Address::Ipv4([127, 0, 0, 1]), 443),
            binding,
            NonZeroUsize::new(1).unwrap(),
            Duration::from_secs(2),
        )
        .unwrap();
    assert!(!base.scoped_preparation);
    let scoped = base.clone().with_scoped_preparation();
    assert!(scoped.scoped_preparation);
    let library = PoolFactory.describe(&base);
    let actual = PoolFactory.describe(&scoped);
    assert_ne!(library.static_identity, actual.static_identity);
    assert_eq!(library.resources, actual.resources);
    assert_eq!(library.task_limit, actual.task_limit);
    assert_eq!(actual.task_limit, NonZeroUsize::new(1));
    let dependency = zero_core::component::ComponentId(9);
    let first = base
        .clone()
        .with_node_dependency(dependency)
        .with_scoped_preparation();
    let second = base
        .with_scoped_preparation()
        .with_node_dependency(dependency)
        .with_scoped_preparation();
    assert_eq!(PoolFactory.describe(&first), PoolFactory.describe(&second));
    // Compile the original borrowed/non-Send wrapping API. This takes no native
    // grant and does not poll or claim success from a fabricated wire operation.
    let _original_borrowed_api = borrowed_fixed;
}
fn borrowed_fixed<'a>(
    owner: &'a Arc<ManagedPool>,
    binding: &'a UpstreamBinding,
    cell: &'a std::cell::Cell<u8>,
) -> impl Future<
    Output = Result<(CarrierIo, Arc<crate::pooled_driver::Physical>), crate::RuntimeError>,
> + 'a {
    owner.open("private-profile", binding, move |io| async move {
        cell.set(cell.get() + 1);
        Ok((io, Box::pin(std::future::pending()) as Driver))
    })
}
