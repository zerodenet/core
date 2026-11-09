use super::*;
#[tokio::test]
async fn independent_event_epochs_share_host_n1_until_the_actual_scope_join() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let preparation = zero_proxy::ProxyPreparation::new();
    let budget = preparation.application_task_budget();
    budget.install_limit(1).unwrap();
    let (one, totals) = preparation.application_network_originals().unwrap();
    let (two, same_totals) = preparation.application_network_originals().unwrap();
    assert!(Arc::ptr_eq(&totals, &same_totals));
    let first = Epoch::new(1, EpochLimits::default(), one, totals, budget.clone()).unwrap();
    let second = Epoch::new(2, EpochLimits::default(), two, same_totals, budget.clone()).unwrap();
    let (done, completed) = tokio::sync::oneshot::channel();
    let original = first
        .spawn(Box::pin(async move {
            let _ = done.send(());
        }))
        .unwrap();
    completed.await.unwrap();
    assert_eq!(budget.usage().charged, 1);
    assert!(
        !original.is_joined(),
        "body completion has not consumed the original Scope join"
    );
    let invoked = Arc::new(AtomicBool::new(false));
    let observed = invoked.clone();
    assert!(second
        .spawn(Box::pin(async move {
            observed.store(true, Ordering::Release);
        }))
        .is_err());
    assert!(
        !invoked.load(Ordering::Acquire),
        "local Epoch room cannot bypass the same Host total"
    );
    first.retire(end()).await.unwrap();
    assert!(first.closed());
    assert!(original.is_joined());
    assert_eq!(budget.usage().charged, 0);
    let next = second.spawn(Box::pin(std::future::pending())).unwrap();
    assert_eq!(budget.usage().charged, 1);
    second.retire(end()).await.unwrap();
    assert!(next.is_joined());
    assert_eq!(budget.usage().charged, 0);
}
