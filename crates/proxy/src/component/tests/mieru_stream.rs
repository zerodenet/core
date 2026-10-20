//! Pure canonical preparation uses the actual host allocator; it grants no I/O.
use crate::component::{catalog::ComponentCatalog, ComponentRecipe, ResourcePreparation};
use mieru::{
    client::ClientPool,
    component::{PoolFactory, StreamFactory, TcpPreparation},
    transport::MieruOutboundOptionsRef,
};
use std::{num::NonZeroUsize, sync::Arc};
use zero_platform_tokio::contract::component::{ComponentFactory, ResourceDeclaration, RoleKind};

#[test]
fn two_native_mieru_consumers_bind_one_actual_canonical_roleless_pool_identity() {
    let pool = Arc::new(ClientPool::default());
    let ids = ComponentCatalog::default();
    let mut allocate = || ids.allocate_id();
    let mut preparation = ResourcePreparation::new(&mut allocate);
    let mut recipes = Vec::new();
    let mut dependencies = Vec::new();
    for _ in 0..2 {
        let pool_recipe = pool.component_options(NonZeroUsize::new(2).unwrap());
        let prepared = pool_recipe
            .prepare_tcp(TcpPreparation {
                tag: "mieru-native",
                server: "127.0.0.1",
                port: 443,
                protocol: MieruOutboundOptionsRef {
                    username: "u",
                    password: "p",
                    udp: false,
                    options: &Default::default(),
                },
                dial: Default::default(),
            })
            .unwrap();
        let owner = preparation
            .share(
                "mieru.tcp.pool",
                Arc::new(PoolFactory),
                Arc::new(prepared.pool_options().clone()),
            )
            .unwrap();
        dependencies.push(owner);
        let options = Arc::new(prepared.bind(owner));
        let spec = StreamFactory.describe(&options);
        assert_eq!(spec.roles, vec![RoleKind::Stream]);
        assert_eq!(spec.dependencies, vec![owner]);
        assert!(spec.resources.is_empty() && spec.task_limit.is_none());
        recipes.push((
            ids.allocate_id().unwrap(),
            ComponentRecipe::from_validated(Arc::new(StreamFactory), options),
        ));
    }
    assert_eq!(dependencies[0], dependencies[1]);
    let order = preparation
        .validate(recipes.iter().map(|(id, recipe)| (*id, recipe.spec())))
        .unwrap();
    let resources = preparation.into_resources().unwrap();
    assert_eq!(resources.len(), 1);
    let owner = &resources[0];
    assert_eq!(owner.component(), dependencies[0]);
    assert_eq!(order[0], owner.component());
    assert!(owner.recipe().spec().roles.is_empty());
    assert_eq!(
        owner.recipe().spec().resources,
        vec![ResourceDeclaration::BackgroundCarrier]
    );
    assert_eq!(owner.recipe().spec().task_limit, NonZeroUsize::new(2));
    assert!(pool.snapshot().connections == 0 && pool.snapshot().dialing_identities == 0);
}
