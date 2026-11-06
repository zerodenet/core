use super::*;
use std::{
    future::Future,
    io,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};
use zero_platform_tokio::contract::scope::{HostedScope, TaskScope};
fn hosted(limit: usize) -> HostedScope {
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(limit));
    scope.enable_registration().unwrap();
    scope
}
async fn pending(future: impl Future) {
    tokio::pin!(future);
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}
struct ReadOnce {
    read: tokio::io::DuplexStream,
    observed: Arc<Notify>,
}
impl Future for ReadOnce {
    type Output = io::Result<()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut byte = [0];
        let mut bytes = ReadBuf::new(&mut byte);
        match Pin::new(&mut this.read).poll_read(cx, &mut bytes) {
            Poll::Pending => {
                this.observed.notify_one();
                Poll::Pending
            }
            Poll::Ready(Err(source)) => Poll::Ready(Err(source)),
            Poll::Ready(Ok(())) if bytes.filled().is_empty() => {
                Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()))
            }
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
        }
    }
}
#[tokio::test]
async fn real_pending_io_and_completed_body_cannot_refund_original_scope_before_join() {
    let mut scope = hosted(2);
    let owner = ScopedSourceTasks::new("actual Source IO", 1, TaskClass::Critical);
    owner.bind(scope.registrar().downgrade()).unwrap();
    let mut guard = owner.lock().await.unwrap();
    assert!(owner.quiescent(), "the sole consumer lease holds no work");
    let (read, mut write) = tokio::io::duplex(1);
    let observed = Arc::new(Notify::new());
    let id = guard
        .spawn(ReadOnce {
            read,
            observed: observed.clone(),
        })
        .unwrap();
    observed.notified().await; // notification follows the original I/O's Pending poll
    assert_eq!(guard.len(), 1);
    assert!(!owner.quiescent(), "the original Pending IO remains owned");
    write.write_u8(42).await.unwrap();
    loop {
        let changed = owner.inner.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if owner.inner.state.lock().unwrap().records[0]
            .output
            .lock()
            .unwrap()
            .is_some()
        {
            break;
        }
        changed.await;
    }
    assert_eq!(guard.len(), 1); // actual body output is not a consumed TaskExit
    assert!(
        !owner.quiescent(),
        "body completion cannot erase the record"
    );
    assert_eq!(
        guard.spawn(async { Ok(()) }).unwrap_err().reason,
        Reason::Capacity
    );
    let witness = owner.inner.state.lock().unwrap().records[0]
        .witness
        .get()
        .unwrap()
        .clone();
    assert!(!witness.is_joined());
    pending(guard.join_next()).await;
    let receipt = scope.next_receipt().await.unwrap();
    assert_eq!(receipt.id(), id);
    guard.join_next().await.unwrap().unwrap();
    assert_eq!(
        guard.len(),
        1,
        "delivered output still owes actual Host ACK"
    );
    assert!(!receipt.joined().registration_released());
    scope.registrar().check_capacity(NonZeroUsize::MIN).unwrap();
    assert_eq!(
        guard.spawn(async { Ok(()) }).unwrap_err().reason,
        Reason::Capacity,
        "the same local group remains full despite spare total Scope capacity"
    );
    assert!(!owner.quiescent());
    pending(guard.join_next()).await;
    assert!(!scope.is_drained());
    scope.acknowledge(&receipt).unwrap();
    assert_eq!(guard.len(), 0);
    assert!(
        owner.quiescent(),
        "same consumer delivered the result and the real Host ACK refunded it"
    );
    let (read, mut write) = tokio::io::duplex(1);
    let observed = Arc::new(Notify::new());
    let second = guard
        .spawn(ReadOnce {
            read,
            observed: observed.clone(),
        })
        .unwrap();
    observed.notified().await;
    assert_eq!(guard.len(), 1);
    write.write_u8(43).await.unwrap();
    let receipt = scope.next_receipt().await.unwrap();
    assert_eq!(receipt.id(), second);
    guard.join_next().await.unwrap().unwrap();
    assert_eq!(guard.len(), 1);
    scope.acknowledge(&receipt).unwrap();
    assert_eq!(guard.len(), 0);
    drop(guard);
    owner
        .close(tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(owner.is_drained());
    scope.begin_shutdown();
    assert!(scope.next_receipt().await.is_none());
}
#[tokio::test]
async fn cancelled_cleanup_retains_same_actual_io_witness_and_critical_typed_error() {
    let mut scope = hosted(1);
    let owner = ScopedSourceTasks::new("actual failing Source IO", 1, TaskClass::Critical);
    owner.bind(scope.registrar().downgrade()).unwrap();
    let mut guard = owner.lock().await.unwrap();
    let (read, write) = tokio::io::duplex(1);
    let observed = Arc::new(Notify::new());
    guard
        .spawn(ReadOnce {
            read,
            observed: observed.clone(),
        })
        .unwrap();
    observed.notified().await;
    drop(write); // original read yields the actual EOF failure
    let receipt = scope.next_receipt().await.unwrap();
    let original = receipt.result().unwrap_err();
    assert!(!receipt.local_failure_accepted());
    let failure = guard.join_next().await.unwrap().unwrap_err();
    let retained = owner.remember(failure);
    let SourceJoinFailure::Scope {
        source: Some(source),
        ..
    } = retained.as_ref()
    else {
        panic!("original Scope cause")
    };
    assert!(Arc::ptr_eq(source, &original));
    assert!(receipt.local_failure_accepted());
    let mut source: &(dyn std::error::Error + 'static) = original.as_ref();
    let mut found = false;
    while let Some(next) = source.source() {
        if let Some(source) = next.downcast_ref::<io::Error>() {
            found |= source.kind() == io::ErrorKind::UnexpectedEof;
        }
        source = next;
    }
    assert!(found);
    pending(owner.close(tokio::time::Instant::now() + Duration::from_secs(2))).await; // consumer guard retained
    assert!(!owner.is_drained());
    drop(guard);
    let mut closing = Box::pin(owner.close(tokio::time::Instant::now() + Duration::from_secs(2)));
    pending(&mut closing).await;
    assert!(!receipt.joined().registration_released());
    assert!(
        !owner.is_drained(),
        "local fault reception cannot fabricate Host ACK"
    );
    scope.acknowledge(&receipt).unwrap();
    let report = closing.await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.failed_tasks.len(), 1);
    assert_eq!(report.failed_tasks[0].id, receipt.id());
    assert!(owner.is_drained());
}
