use super::*;
#[tokio::test]
async fn worker_panic_after_actual_connect_pending_drains_the_same_overlapped_before_std_and_scope_join(
) {
    use std::{num::NonZeroUsize, time::Duration};
    let mut scope = crate::contract::scope::TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let path = PathBuf::from(format!(
        r"\\.\pipe\zero-original-panic-{}",
        std::process::id()
    ));
    let owner = ControlledPipeOwner::new(path, scope.registrar());
    let view = owner.open().await.unwrap();
    let end = Instant::now() + Duration::from_secs(3);
    tokio::time::timeout_at(end, async {
        while !owner.pending_io().0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    owner.0.panic_pending.store(true, Ordering::Release);
    owner.0.wake_worker();
    let exit = tokio::time::timeout_at(end, scope.next_exit())
        .await
        .unwrap()
        .unwrap();
    let error = exit.result.unwrap_err();
    assert_eq!(error.reason, Reason::Internal);
    assert_eq!(error.impact, Impact::Component);
    let witness = owner.worker().unwrap();
    assert!(witness.is_joined() && owner.std_worker_joined());
    assert!(witness.try_joined().unwrap().result().is_err());
    assert_eq!(owner.pending_io(), (false, false, false));
    assert!(owner.closed().is_none());
    assert!(owner.close(Instant::now()).await.is_err());
    drop(view);
    let closed = owner.close(end).await.unwrap();
    assert!(owner.matches_closed(&closed) && closed.had_native_acquisition());
}
