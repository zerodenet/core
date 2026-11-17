use super::*;
use std::future::Future;

#[tokio::test]
async fn resuming_a_paused_source_wakes_its_actual_admission_waiter() {
    let budget = SourceBudget::new(1, 16);
    budget.set_paused(true);
    let admission = budget.acquire(8);
    tokio::pin!(admission);
    std::future::poll_fn(|cx| {
        assert!(admission.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    budget.set_paused(false);
    let original = tokio::time::timeout(Duration::from_secs(1), admission)
        .await
        .expect("resumed source must wake without an unrelated event")
        .unwrap();
    assert_eq!(budget.slots.available_permits(), 0);
    assert_eq!(budget.permits.available_permits(), 8);
    drop(original);
    assert_eq!(budget.slots.available_permits(), 1);
    assert_eq!(budget.permits.available_permits(), 16);
}
