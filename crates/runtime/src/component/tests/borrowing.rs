//! The real host admission graph fences actual generation tokens before I/O.
use super::super::admission::HostAdmission;
use std::{collections::BTreeSet, sync::Arc};
use zero_core::component::{ComponentId, ComponentInstanceId, ResourceGeneration};
use zero_platform_tokio::contract::Reason;
fn owner(id: u64) -> (ComponentInstanceId, Arc<()>) {
    (
        ComponentInstanceId {
            component: ComponentId(id),
            generation: ResourceGeneration(1),
        },
        Arc::new(()),
    )
}
#[test]
fn actual_loan_edges_deduplicate_reject_cycles_and_foreign_owner_tokens() {
    let host = Arc::new(HostAdmission::default());
    let a = owner(11);
    let b = owner(12);
    let c = owner(13);
    host.publish(vec![a.clone(), b.clone(), c.clone()].into_iter());
    let empty = BTreeSet::new();
    host.borrow_from(a.clone(), vec![b.clone()], &empty)
        .unwrap();
    let first = host.loan_snapshot();
    host.borrow_from(a.clone(), vec![b.clone()], &empty)
        .unwrap();
    assert_eq!(host.loan_snapshot(), first);
    host.borrow_from(b.clone(), vec![c.clone()], &empty)
        .unwrap();
    let before = host.loan_snapshot();
    assert_eq!(
        host.borrow_from(c.clone(), vec![a.clone()], &empty)
            .unwrap_err()
            .reason,
        Reason::InvalidConfig
    );
    assert_eq!(
        host.loan_snapshot(),
        before,
        "failed graph admission is atomic"
    );
    assert_eq!(
        host.borrow_from((a.0, Arc::new(())), vec![c.clone()], &empty)
            .unwrap_err()
            .reason,
        Reason::StalePlan
    );
    let fence = host.freeze_loans();
    assert_eq!(
        host.borrow_from(a.clone(), vec![c.clone()], &empty)
            .unwrap_err()
            .reason,
        Reason::StalePlan
    );
    drop(fence);
    host.borrow_from(a.clone(), vec![c], &empty).unwrap();
    let prior_revision = host.loan_snapshot().0;
    host.publish(vec![a, b].into_iter());
    assert!(
        host.loan_snapshot().0 > prior_revision,
        "pruning invalidates cold snapshots too"
    );
    assert_eq!(
        host.loan_snapshot().1,
        first.1,
        "only retired generation metadata is reclaimed"
    );
}
#[test]
fn cancelled_replacement_keeps_the_loan_fence_until_the_host_stages_actual_owners() {
    let host = Arc::new(HostAdmission::default());
    let a = owner(11);
    let b = owner(12);
    host.publish(vec![a.clone(), b.clone()].into_iter());
    host.freeze_loans().retain();
    assert!(host
        .borrow_from(a.clone(), vec![b.clone()], &Default::default())
        .is_err());
    host.stage(vec![a.clone()].into_iter());
    assert!(
        host.check_installed(b.0, &b.1).is_ok(),
        "ordinary admission is still switched by publication"
    );
    assert!(
        host.borrow_from(a.clone(), vec![b], &Default::default())
            .is_err(),
        "stale old donor cannot be acquired after staging"
    );
    host.borrow_from(a, vec![], &Default::default()).unwrap();
}
