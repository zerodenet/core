use super::*;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use zero_platform_tokio::contract::scope::{TaskClass, TaskScope};
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
fn end() -> tokio::time::Instant {
    crate::ipc::owner::close_deadline()
}
#[tokio::test]
async fn actual_unix_pending_caller_drop_keeps_driver_until_native_scope_and_raw_join() {
    let _serial = SERIAL.lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.sock");
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    let caller = Caller::begin(Limits::default()).await.unwrap();
    let original = caller.original.clone();
    let held_peer;
    {
        let request = caller.request(
            path.to_str().unwrap(),
            &crate::ipc::protocol::IpcRequest::Ping { id: None },
        );
        tokio::pin!(request);
        let accepted = listener.accept();
        tokio::pin!(accepted);
        let (peer, _) = tokio::select! {biased;result=&mut request=>panic!("actual reply must remain pending: {}",result.is_ok()),accepted=&mut accepted=>accepted.unwrap()};
        let mut peer = BufReader::new(peer);
        let mut line = String::new();
        peer.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&line).unwrap(),
            serde_json::json!({"type":"ping","id":null})
        );
        tokio::time::timeout_at(end(),async{loop{tokio::select!{biased;result=&mut request=>panic!("real unanswered read returned: {}",result.is_ok()),_=async{if !original.read_pending.load(Ordering::Acquire){original.changed.notified().await;}}=>{}}if original.read_pending.load(Ordering::Acquire){break;}}}).await.unwrap();
        assert_eq!(original.book.usage().connections, 1);
        assert!(!original.retired.load(Ordering::Acquire));
        held_peer = peer;
    }
    drop(caller);
    // No new dial is needed to drive this abandoned original.
    tokio::time::timeout_at(end(), async {
        while !original.retired.load(Ordering::Acquire) {
            tokio::task::yield_now().await
        }
    })
    .await
    .unwrap();
    assert_eq!(
        original.book.usage().connections,
        1,
        "raw driver join is still independent"
    );
    let _ = original.driver.join().await;
    drop(held_peer);
    let scope_guard = original.scope.lock().await;
    let scope = scope_guard.as_ref().unwrap();
    assert!(scope.is_drained());
    assert!(lock(&original.faults.joined)
        .iter()
        .all(TaskJoined::registration_released));
    drop(scope_guard);
    match &*original.native.lock().await {
        native::Native::Unix(control) => assert!(control.is_cancelled()),
        _ => panic!("must retain exact native controller"),
    }
    // Real cancellation error reaches the next actual receiver once; then the
    // same application book can admit a new operation without clearing debt.
    assert!(Caller::begin(Limits::default()).await.is_err());
    let next = Caller::begin(Limits::default()).await.unwrap();
    assert!(Arc::ptr_eq(&next.original.book, &original.book));
    next.finish().await.unwrap();
    assert_eq!(original.book.usage().connections, 0);
}
#[tokio::test]
async fn actual_reply_payload_outlives_native_and_scope_without_escaping_the_same_book() {
    let _serial = SERIAL.lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("reply.sock");
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut peer = BufReader::new(stream);
        let mut line = String::new();
        peer.read_line(&mut line).await.unwrap();
        peer.get_mut()
            .write_all(b"{\"ok\":true,\"result\":\"pong\"}\n")
            .await
            .unwrap();
        peer.get_mut().flush().await.unwrap();
    });
    let caller = Caller::begin(Limits::default()).await.unwrap();
    let original = caller.original.clone();
    let reply = caller
        .request(
            path.to_str().unwrap(),
            &crate::ipc::protocol::IpcRequest::Ping { id: None },
        )
        .await
        .unwrap()
        .ensure_ok("failure")
        .unwrap();
    caller.finish().await.unwrap();
    peer.await.unwrap();
    assert!(original.retired.load(Ordering::Acquire));
    assert_eq!(original.book.usage().connections, 0);
    assert!(original.book.usage().response_bytes > 0);
    drop(reply);
    assert_eq!(original.book.usage().response_bytes, 0);
}
#[tokio::test]
async fn actual_held_scope_delivery_is_not_native_close_or_host_ack() {
    let _serial = SERIAL.lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("held.sock");
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    let caller = Caller::begin(Limits::default()).await.unwrap();
    let original = caller.original.clone();
    let mut scope = TaskScope::suspended_hosted(
        tokio::runtime::Handle::current(),
        std::num::NonZeroUsize::new(1),
    );
    scope.enable_registration().unwrap();
    let pending = original
        .native
        .lock()
        .await
        .prepare(
            path.to_str().unwrap(),
            scope.registrar(),
            original.stop.clone(),
        )
        .unwrap();
    let (stream, accepted) = tokio::join!(pending.connect(), listener.accept());
    let mut stream = stream.unwrap();
    let (peer, _) = accepted.unwrap();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async move {
            let _ = stream.read_u8().await;
            Ok(())
        })
        .unwrap();
    original.stop.cancel();
    drop(peer);
    scope.begin_shutdown();
    let receipt = tokio::time::timeout_at(end(), scope.next_receipt())
        .await
        .unwrap()
        .unwrap();
    lock(&original.faults.joined).push(receipt.joined().clone());
    assert!(witness.is_joined() && !receipt.joined().registration_released());
    assert!(original
        .native
        .lock()
        .await
        .close(end(), &original.faults)
        .await
        .unwrap());
    assert!(!scope.is_drained());
    assert_eq!(original.book.usage().connections, 1);
    scope.acknowledge(&receipt).unwrap();
    assert!(scope.is_drained() && receipt.joined().registration_released());
    *original.scope.lock().await = Some(scope);
    caller.finish().await.unwrap();
    assert_eq!(original.book.usage().connections, 0);
}

#[tokio::test]
async fn actual_pending_io_panic_retains_original_task_error_but_positive_cleanup_allows_recovery()
{
    let _serial = SERIAL.lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("panic.sock");
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    let caller = Caller::begin(Limits::default()).await.unwrap();
    let original = caller.original.clone();
    let weak = Arc::downgrade(&original);
    original.panic_pending.store(true, Ordering::Release);
    let request = caller.request(
        path.to_str().unwrap(),
        &crate::ipc::protocol::IpcRequest::Ping { id: None },
    );
    let (result, accepted) = tokio::join!(request, listener.accept());
    let (peer, _) = accepted.unwrap();
    assert!(result.is_err());
    assert!(
        original.read_pending.load(Ordering::Acquire),
        "actual underlying OS read must be Pending before the real Scope task panics"
    );
    let error = caller
        .finish()
        .await
        .expect_err("original task panic must reach real cleanup receiver");
    assert!(original.retired.load(Ordering::Acquire));
    assert_eq!(original.book.usage().connections, 0);
    let rows = lock(&original.faults.joined).clone();
    assert_eq!(rows.len(), 1);
    let same = rows[0].result().unwrap_err();
    assert!(rows[0].registration_released());
    let root = error
        .get_ref()
        .unwrap()
        .downcast_ref::<Arc<io::Error>>()
        .expect("same real ControlTask result");
    let faults = root
        .get_ref()
        .unwrap()
        .downcast_ref::<ClientFaults>()
        .expect("same actual Scope error rows");
    assert!(Arc::ptr_eq(&faults.errors[0], &same));
    let next = Caller::begin(Limits::default()).await.unwrap();
    assert!(Arc::ptr_eq(&next.original.book, &original.book));
    next.finish().await.unwrap();
    drop(peer);
    drop(rows);
    drop(original);
    assert!(
        weak.upgrade().is_none(),
        "returned fault payload must not retain the IO/controller/attempt"
    );
    assert!(std::error::Error::source(&error).is_some());
    drop(error);
    drop(same);
}
