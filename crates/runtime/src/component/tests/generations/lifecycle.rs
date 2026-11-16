//! Planning only; lifecycle membership never substitutes for original closure.
use super::*;
fn grouped(slot: Arc<ComponentSlot>, owner: ComponentId) -> Arc<ComponentSlot> {
    Arc::new(ComponentSlot::from_recipe(
        slot.id,
        slot.generation,
        slot.recipe.clone().with_lifecycle_owner(owner),
        slot.dial.clone(),
        slot.legacy_connect_stage(),
    ))
}

#[test]
fn upper_pool_change_rebuilds_its_real_lower_carrier_but_preserves_external_donors() {
    let ids = ComponentCatalog::default();
    let donor = ids.allocate_id().unwrap();
    let lower = ids.allocate_id().unwrap();
    let upper = ids.allocate_id().unwrap();
    let role = ids.allocate_id().unwrap();
    let old = order_and_reuse(
        vec![
            slot(donor, 1, vec![], AddressFamily::Auto),
            grouped(slot(lower, 1, vec![donor], AddressFamily::Auto), lower),
            grouped(slot(upper, 1, vec![lower], AddressFamily::Auto), lower),
            grouped(slot(role, 1, vec![upper], AddressFamily::Auto), lower),
        ],
        &[],
    )
    .unwrap();
    let mut candidate = old.clone();
    let at = candidate.iter().position(|slot| slot.id == upper).unwrap();
    candidate[at] = grouped(slot(upper, 1, vec![lower], AddressFamily::OnlyIpv4), lower);
    let next = order_and_reuse(candidate, &old).unwrap();
    for id in [lower, upper, role] {
        let before = old.iter().find(|slot| slot.id == id).unwrap();
        let after = next.iter().find(|slot| slot.id == id).unwrap();
        assert!(!Arc::ptr_eq(before, after));
        assert_eq!(after.generation, ResourceGeneration(2));
    }
    assert!(Arc::ptr_eq(&old[0], &next[0]));
    let repeated = order_and_reuse(next.clone(), &next).unwrap();
    assert!(next.iter().zip(repeated).all(|(a, b)| Arc::ptr_eq(a, &b)));
}

#[test]
fn removing_or_splitting_one_member_cannot_reuse_the_old_coupled_carrier() {
    let ids = ComponentCatalog::default();
    let lower = ids.allocate_id().unwrap();
    let upper = ids.allocate_id().unwrap();
    let old = order_and_reuse(
        vec![
            grouped(slot(lower, 1, vec![], AddressFamily::Auto), lower),
            grouped(slot(upper, 1, vec![lower], AddressFamily::Auto), lower),
        ],
        &[],
    )
    .unwrap();
    let removed = order_and_reuse(vec![old[0].clone()], &old).unwrap();
    assert_eq!(removed[0].generation, ResourceGeneration(2));
    let split = order_and_reuse(
        vec![
            old[0].clone(),
            grouped(slot(upper, 1, vec![lower], AddressFamily::Auto), upper),
        ],
        &old,
    )
    .unwrap();
    assert!(split
        .iter()
        .all(|slot| slot.generation == ResourceGeneration(2)));
}

#[test]
fn lifecycle_membership_cannot_legalize_a_cyclic_factory_graph() {
    let ids = ComponentCatalog::default();
    let a = ids.allocate_id().unwrap();
    let b = ids.allocate_id().unwrap();
    let error = order_and_reuse(
        vec![
            grouped(slot(a, 1, vec![b], AddressFamily::Auto), a),
            grouped(slot(b, 1, vec![a], AddressFamily::Auto), a),
        ],
        &[],
    )
    .err()
    .unwrap();
    assert_eq!(error.reason, Reason::InvalidConfig);
}
