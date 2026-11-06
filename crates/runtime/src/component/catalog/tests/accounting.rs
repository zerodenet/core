//! Direct catalog callers use the same real Users, not a proxy-only preflight.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
fn end() -> tokio::time::Instant {
    tokio::time::Instant::now() + std::time::Duration::from_secs(3)
}
#[tokio::test]
async fn actual_host_total_change_preserves_admitted_transferred_io_and_commits_after_original_retirement(
) {
    let catalog = ComponentCatalog::default();
    let initial = Arc::new(
        RuntimeConfig::parse(
            r#"{"runtime":{"carrier_max_total_originals":2},"route":{"final":{"type":"direct"}}}"#,
        )
        .unwrap(),
    );
    catalog.start(initial.clone(), end()).await.unwrap();
    catalog.commit(&initial, || Ok(())).unwrap();
    let original_plan = catalog.current(&initial).unwrap();
    let original = original_plan.direct(None).unwrap();
    let roots = catalog.original_budgets();
    assert_eq!(roots.carrier.snapshot().limit.get(), 2);
    let users = original.own_user_root();
    let lease = crate::component::instance::users::Lease::enter(
        original.id,
        std::slice::from_ref(&users),
        None,
    )
    .unwrap()
    .unwrap();
    let (io, mut peer) = tokio::io::duplex(16);
    let mut transferred = lease.stream(Box::pin(io));
    transferred.write_all(b"before").await.unwrap();
    let mut bytes = [0; 6];
    peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"before");
    let mut next = (*initial).clone();
    next.runtime.carrier_max_total_originals = Some(1);
    let next = Arc::new(next);
    let candidate = catalog.prepare(next.clone()).unwrap();
    let pending = candidate.direct(None).unwrap();
    assert_eq!(
        original.recipe.spec(),
        pending.recipe.spec(),
        "protocol private identity/options are unchanged"
    );
    assert_eq!(original.recipe.carrier_total_limit(), Some(2));
    assert_eq!(pending.recipe.carrier_total_limit(), Some(1));
    assert!(!Arc::ptr_eq(&original, &pending));
    let refused = catalog.start(next.clone(), end()).await.unwrap_err();
    assert_eq!(refused.reason, Reason::Capacity);
    assert!(Arc::ptr_eq(
        &original,
        &catalog.current(&initial).unwrap().direct(None).unwrap()
    ));
    assert_eq!(roots.carrier.snapshot().limit.get(), 2);
    peer.write_all(b"alive!").await.unwrap();
    transferred.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"alive!");
    drop(transferred);
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
    let external = catalog.try_pause_replacement_users(&candidate).unwrap();
    catalog.start(next.clone(), end()).await.unwrap();
    drop(external);
    let actual = catalog.state.lock().unwrap().replacement.clone().unwrap();
    assert!(actual.retains_budget_pause());
    let owner = original.original_carrier_owner(0).unwrap();
    assert!(owner.closed().unwrap().matches(&owner));
    assert!(
        !owner.users_closed().unwrap().had_native_acquisition(),
        "real empty original owner, not a fabricated FD close"
    );
    assert_eq!(roots.carrier.snapshot().limit.get(), 1);
    assert!(Arc::ptr_eq(&roots, &catalog.original_budgets()));
    catalog.commit(&next, || Ok(())).unwrap();
    assert!(catalog.state.lock().unwrap().replacement.is_none());
    drop(actual);
    assert!(Arc::ptr_eq(
        &pending,
        &catalog.current(&next).unwrap().direct(None).unwrap()
    ));
    catalog.shutdown(end()).await.unwrap();
}
