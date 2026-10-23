use crate::component::{
    catalog::ComponentCatalog, generations::order_and_reuse, instance::ComponentSlot,
    ComponentRecipe, DirectFactory, DirectOptions,
};
use std::sync::Arc;
use zero_core::component::{ComponentId, ResourceGeneration};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
    Reason,
};
use zero_traits::{AddressFamily, DialPolicy};

struct Dependent(Vec<ComponentId>);
impl ComponentFactory for Dependent {
    type Options = DirectOptions;
    fn describe(&self, options: &DirectOptions) -> ComponentSpec {
        let mut spec = DirectFactory.describe(options);
        spec.dependencies = self.0.clone();
        spec
    }
    fn build(
        &self,
        options: Arc<DirectOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        DirectFactory.build(options, context)
    }
}
fn slot(
    id: ComponentId,
    generation: u64,
    dependencies: Vec<ComponentId>,
    family: AddressFamily,
) -> Arc<ComponentSlot> {
    let policy = DialPolicy {
        address_family: family,
        ..Default::default()
    };
    let recipe = ComponentRecipe::from_validated(
        Arc::new(Dependent(dependencies)),
        Arc::new(DirectOptions::validate(policy.clone()).unwrap()),
    );
    Arc::new(ComponentSlot::from_recipe(
        id,
        ResourceGeneration(generation),
        recipe,
        policy,
        "connect_direct",
    ))
}

#[test]
fn a_changed_dependency_rebuilds_each_transitive_consumer_once_and_preserves_other_roles() {
    let ids = ComponentCatalog::default();
    let root = ids.allocate_id().unwrap();
    let middle = ids.allocate_id().unwrap();
    let final_node = ids.allocate_id().unwrap();
    let separate = ids.allocate_id().unwrap();
    let old = order_and_reuse(
        vec![
            slot(root, 1, vec![], AddressFamily::Auto),
            slot(middle, 1, vec![root], AddressFamily::Auto),
            slot(final_node, 1, vec![middle], AddressFamily::Auto),
            slot(separate, 1, vec![], AddressFamily::Auto),
        ],
        &[],
    )
    .unwrap();
    let mut candidate = old.clone();
    candidate[0] = slot(root, 1, vec![], AddressFamily::OnlyIpv4);
    let next = order_and_reuse(candidate, &old).unwrap();
    assert_eq!(
        next.iter()
            .find(|node| node.id == middle)
            .unwrap()
            .dependency_instances(),
        &[next
            .iter()
            .find(|node| node.id == root)
            .unwrap()
            .instance_id()]
    );
    assert_eq!(
        next.iter()
            .find(|node| node.id == final_node)
            .unwrap()
            .dependency_instances(),
        &[next
            .iter()
            .find(|node| node.id == middle)
            .unwrap()
            .instance_id()]
    );
    for id in [root, middle, final_node] {
        let before = old.iter().find(|slot| slot.id == id).unwrap();
        let after = next.iter().find(|slot| slot.id == id).unwrap();
        assert_eq!(after.generation, ResourceGeneration(2));
        assert!(!Arc::ptr_eq(before, after));
    }
    assert!(Arc::ptr_eq(
        &old[3],
        next.iter().find(|slot| slot.id == separate).unwrap()
    ));
    let repeated = order_and_reuse(next.clone(), &next).unwrap();
    for (before, after) in next.iter().zip(repeated.iter()) {
        assert!(Arc::ptr_eq(before, after));
        assert_eq!(before.generation, after.generation);
    }
}

#[test]
fn a_local_change_already_assigned_next_generation_is_not_incremented_twice() {
    let ids = ComponentCatalog::default();
    let id = ids.allocate_id().unwrap();
    let old = slot(id, 1, vec![], AddressFamily::Auto);
    let changed = slot(id, 2, vec![], AddressFamily::OnlyIpv4);
    let next = order_and_reuse(vec![changed.clone()], &[old]).unwrap();
    assert!(Arc::ptr_eq(&changed, &next[0]));
    assert_eq!(next[0].generation, ResourceGeneration(2));
}

#[test]
fn cold_generation_propagation_rejects_invalid_graphs_before_reusing_slots() {
    let ids = ComponentCatalog::default();
    let id = ids.allocate_id().unwrap();
    let missing = ids.allocate_id().unwrap();
    let error = order_and_reuse(vec![slot(id, 1, vec![missing], AddressFamily::Auto)], &[])
        .err()
        .unwrap();
    assert_eq!(error.component, Some(id));
    assert_eq!(error.reason, Reason::InvalidConfig);
}

#[test]
fn exhausted_component_generation_cannot_wrap_or_publish_a_replacement() {
    let ids = ComponentCatalog::default();
    let id = ids.allocate_id().unwrap();
    let old = slot(id, u64::MAX, vec![], AddressFamily::Auto);
    let candidate = slot(id, u64::MAX, vec![], AddressFamily::OnlyIpv4);
    let error = order_and_reuse(vec![candidate], &[old]).err().unwrap();
    assert_eq!(error.component, Some(id));
    assert_eq!(error.reason, Reason::Internal);
}

#[test]
fn issued_floor_and_revocation_advance_transitive_dependencies_once_without_rebuilding_other_roles()
{
    use crate::component::generations::order_with_floor;
    use std::collections::BTreeMap;
    let ids = ComponentCatalog::default();
    let root = ids.allocate_id().unwrap();
    let middle = ids.allocate_id().unwrap();
    let leaf = ids.allocate_id().unwrap();
    let separate = ids.allocate_id().unwrap();
    let old = order_and_reuse(
        vec![
            slot(root, 1, vec![], AddressFamily::Auto),
            slot(middle, 1, vec![root], AddressFamily::Auto),
            slot(leaf, 1, vec![middle], AddressFamily::Auto),
            slot(separate, 1, vec![], AddressFamily::Auto),
        ],
        &[],
    )
    .unwrap();
    old.iter().find(|slot| slot.id == root).unwrap().revoke();
    // This is the generation algorithm's bounded input, not a user-close proof.
    let issued = BTreeMap::from([
        (root, ResourceGeneration(2)),
        (middle, ResourceGeneration(2)),
        (leaf, ResourceGeneration(2)),
        (separate, ResourceGeneration(1)),
    ]);
    let next = order_with_floor(old.clone(), &old, &issued).unwrap();
    for id in [root, middle, leaf] {
        let owner = next.iter().find(|slot| slot.id == id).unwrap();
        assert_eq!(owner.generation, ResourceGeneration(3));
        for dependency in owner.dependency_instances() {
            assert_eq!(dependency.generation, ResourceGeneration(3));
        }
    }
    assert!(Arc::ptr_eq(
        old.iter().find(|slot| slot.id == separate).unwrap(),
        next.iter().find(|slot| slot.id == separate).unwrap()
    ));
    let repeated = order_with_floor(next.clone(), &next, &issued).unwrap();
    for (before, after) in next.iter().zip(&repeated) {
        assert!(Arc::ptr_eq(before, after));
    }
}
