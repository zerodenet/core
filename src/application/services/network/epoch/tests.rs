use super::*;
use std::future::Future;
use tokio::{io::AsyncReadExt, sync::Notify};
fn end() -> Instant {
    Instant::now() + std::time::Duration::from_secs(3)
}
async fn connected(epoch: &Arc<Epoch>) -> (Box<dyn EventSinkTcpStream>, tokio::net::TcpStream) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = listener.local_addr().unwrap();
    let stream = tokio::select! {
        result=epoch.connect(peer,&EgressInterfaceControl::default(),end()) => result.unwrap(),
        _=epoch.drive()=>panic!("original scope unexpectedly ended"),
    };
    (stream, listener.accept().await.unwrap().0)
}
#[tokio::test]
async fn joined_connect_and_cancellation_do_not_close_a_held_transfer_view() {
    let epoch = Epoch::new(1);
    let (stream, mut peer) = connected(&epoch).await;
    let original = epoch.originals.lock().unwrap()[0].clone();
    assert!(original
        .custody
        .users
        .lock()
        .unwrap()
        .iter()
        .all(TaskJoinWitness::is_joined));
    assert!(epoch.retire(end()).await.is_err());
    assert!(!epoch.closed());
    assert!(original.receipt.lock().unwrap().is_none());
    let mut byte = [0];
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), peer.read(&mut byte))
            .await
            .is_err(),
        "actual peer remains open before native receipt"
    );
    assert!(epoch.spawn(Box::pin(async {})).is_err());
    drop(stream);
    epoch.retire(end()).await.unwrap();
    assert!(epoch.closed());
    assert!(
        !original
            .receipt
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .closed_io
            .is_empty(),
        "same original native close report retained"
    );
    assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
}
#[tokio::test]
async fn pending_http_user_requires_real_scope_join_before_original_close() {
    let epoch = Epoch::new(1);
    let (mut stream, mut peer) = connected(&epoch).await;
    let original = epoch.originals.lock().unwrap()[0].clone();
    let entered = Arc::new(Notify::new());
    let notice = entered.clone();
    let witness = epoch
        .spawn(Box::pin(async move {
            let mut byte = [0];
            let read = stream.read(&mut byte);
            tokio::pin!(read);
            std::future::poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if result.is_pending() {
                    notice.notify_one();
                }
                result
            })
            .await
            .ok();
        }))
        .unwrap();
    entered.notified().await;
    epoch.stop.cancel();
    // The original supervisor has not consumed this task's exit yet.
    assert!(!witness.is_joined());
    epoch.close_returned(end()).await.unwrap();
    assert!(original.receipt.lock().unwrap().is_none());
    let report = epoch.scope.lock().await.shutdown(end()).await;
    assert!(report.is_complete());
    assert!(witness.is_joined());
    assert!(witness.try_joined().unwrap().result().is_ok());
    assert!(
        original.receipt.lock().unwrap().is_none(),
        "TaskJoined alone is not native close"
    );
    epoch.retire(end()).await.unwrap();
    assert!(!original
        .receipt
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .closed_io
        .is_empty());
    let mut byte = [0];
    assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
}
