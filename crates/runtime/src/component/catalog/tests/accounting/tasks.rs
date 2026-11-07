//! A task-total-only policy change follows actual catalog StopOld/BuildNew.
use super::*;
#[tokio::test]
async fn task_total_only_change_preserves_old_admitted_io_until_actual_retirement() {
    let catalog = ComponentCatalog::default();
    let initial = Arc::new(
        RuntimeConfig::parse(
            r#"{"runtime":{"component_total_max_tasks":2},"route":{"final":{"type":"direct"}}}"#,
        )
        .unwrap(),
    );
    catalog.start(initial.clone(), end()).await.unwrap();
    catalog.commit(&initial, || Ok(())).unwrap();
    let old = catalog.current(&initial).unwrap().direct(None).unwrap();
    let root = catalog.original_budgets();
    assert_eq!(root.component_tasks.usage().limit, 2);
    assert_eq!(old.recipe.component_task_total(), Some(2));
    let user = old.own_user_root();
    let lease =
        crate::component::instance::users::Lease::enter(old.id, std::slice::from_ref(&user), None)
            .unwrap()
            .unwrap();
    let (io, mut peer) = tokio::io::duplex(16);
    let mut admitted = lease.stream(Box::pin(io));
    let mut next = (*initial).clone();
    next.runtime.component_total_max_tasks = Some(1);
    let next = Arc::new(next);
    let candidate = catalog.prepare(next.clone()).unwrap();
    let new = candidate.direct(None).unwrap();
    assert_eq!(old.recipe.spec(), new.recipe.spec());
    assert_eq!(new.recipe.component_task_total(), Some(1));
    assert!(!Arc::ptr_eq(&old, &new));
    assert_eq!(
        catalog.start(next.clone(), end()).await.unwrap_err().reason,
        Reason::Capacity
    );
    assert_eq!(root.component_tasks.usage().limit, 2);
    peer.write_all(b"alive!").await.unwrap();
    let mut bytes = [0; 6];
    admitted.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"alive!");
    drop(admitted);
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
    let external = catalog.try_pause_replacement_users(&candidate).unwrap();
    let prepared = catalog
        .prepare_resource_start(next.clone(), end())
        .await
        .unwrap();
    assert!(Arc::ptr_eq(prepared.plan(), &candidate));
    let original = old.original_carrier_owner(0).unwrap();
    assert!(original.closed().unwrap().matches(&original));
    assert!(!original.users_closed().unwrap().had_native_acquisition());
    assert!(new.original_carrier_owner(0).is_none());
    assert_eq!(root.component_tasks.usage().limit, 1);
    assert_eq!(root.component_tasks.usage().charged, 0);
    assert!(Arc::ptr_eq(&root, &catalog.original_budgets()));
    drop(external);
    prepared.start(end()).await.unwrap();
    catalog.commit(&next, || Ok(())).unwrap();
    assert!(new.original_carrier_owner(0).is_some());
    catalog.shutdown(end()).await.unwrap();
    assert_eq!(root.component_tasks.usage().charged, 0);
}
