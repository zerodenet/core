use super::*;
use crate::component::generations::order_with_loans;
use std::collections::{BTreeMap, BTreeSet};
#[test]
fn old_generation_loan_edges_cannot_invent_a_cycle_in_the_new_declared_graph() {
    let ids = ComponentCatalog::default();
    let a = ids.allocate_id().unwrap();
    let b = ids.allocate_id().unwrap();
    let old = order_and_reuse(
        vec![
            slot(a, 1, vec![], AddressFamily::Auto),
            slot(b, 1, vec![], AddressFamily::Auto),
        ],
        &[],
    )
    .unwrap();
    let edges = BTreeSet::from([(old[0].instance_id(), old[1].instance_id())]);
    let next = order_with_loans(
        vec![
            slot(a, 1, vec![], AddressFamily::Auto),
            slot(b, 1, vec![a], AddressFamily::Auto),
        ],
        &old,
        &BTreeMap::new(),
        &edges,
    )
    .unwrap();
    assert_eq!(
        next.iter().map(|node| node.id).collect::<Vec<_>>(),
        vec![a, b]
    );
    assert!(next
        .iter()
        .all(|node| node.generation == ResourceGeneration(2)));
    assert_eq!(next[1].dependency_instances(), &[next[0].instance_id()]);
    // The complete old instance graph is still required for retirement. It was
    // not rewritten into the opposite new-generation factory dependency.
    assert_eq!(
        edges,
        BTreeSet::from([(old[0].instance_id(), old[1].instance_id())])
    );
}
#[test]
fn actual_cross_owner_dependencies_propagate_once_without_becoming_factory_dependencies() {
    let ids = ComponentCatalog::default();
    let root = ids.allocate_id().unwrap();
    let pool = ids.allocate_id().unwrap();
    let role = ids.allocate_id().unwrap();
    let other = ids.allocate_id().unwrap();
    let old = order_and_reuse(
        vec![
            slot(root, 1, vec![], AddressFamily::Auto),
            slot(pool, 1, vec![], AddressFamily::Auto),
            slot(role, 1, vec![pool], AddressFamily::Auto),
            slot(other, 1, vec![], AddressFamily::Auto),
        ],
        &[],
    )
    .unwrap();
    let find = |id| old.iter().find(|node| node.id == id).unwrap().clone();
    let edges = BTreeSet::from([(find(pool).instance_id(), find(root).instance_id())]);
    let unchanged = order_with_loans(old.clone(), &old, &BTreeMap::new(), &edges).unwrap();
    for node in &old {
        assert!(Arc::ptr_eq(
            node,
            unchanged.iter().find(|next| next.id == node.id).unwrap()
        ));
    }
    let mut candidate = old.clone();
    let index = candidate.iter().position(|node| node.id == root).unwrap();
    candidate[index] = slot(root, 1, vec![], AddressFamily::OnlyIpv4);
    let next = order_with_loans(candidate, &old, &BTreeMap::new(), &edges).unwrap();
    for id in [root, pool, role] {
        assert_eq!(
            next.iter().find(|node| node.id == id).unwrap().generation,
            ResourceGeneration(2)
        );
    }
    assert!(Arc::ptr_eq(
        &find(other),
        next.iter().find(|node| node.id == other).unwrap()
    ));
    assert!(
        next.iter()
            .find(|node| node.id == pool)
            .unwrap()
            .dependency_instances()
            .is_empty(),
        "runtime retirement edges are not fabricated factory BuildContext inputs"
    );
    let remaining: Vec<_> = old.iter().filter(|node| node.id != root).cloned().collect();
    let removed = order_with_loans(remaining, &old, &BTreeMap::new(), &edges).unwrap();
    for id in [pool, role] {
        assert_eq!(
            removed
                .iter()
                .find(|node| node.id == id)
                .unwrap()
                .generation,
            ResourceGeneration(2)
        );
    }
}
