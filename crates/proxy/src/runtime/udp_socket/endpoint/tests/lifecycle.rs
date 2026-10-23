use super::*;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};
use zero_platform_tokio::contract::lifecycle::TaskClass;

#[tokio::test]
async fn actual_scope_pending_exit_retains_original_socket_across_cancelled_shutdown_wait() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let fixture = Fixture::new().await;
    let scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(2).unwrap(),
    );
    let gate = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut peer = tokio::net::TcpStream::connect(gate.local_addr().unwrap())
        .await
        .unwrap();
    let (mut reader, _) = gate.accept().await.unwrap();
    let pending = Arc::new(Notify::new());
    let observed = pending.clone();
    let gate_id = scope
        .registrar()
        .spawn(TaskClass::Critical, async move {
            let mut byte = [0; 1];
            let mut read = tokio::io::ReadBuf::new(&mut byte);
            poll_fn(|cx| {
                assert!(tokio::io::AsyncRead::poll_read(
                    std::pin::Pin::new(&mut reader),
                    cx,
                    &mut read
                )
                .is_pending());
                Poll::Ready(())
            })
            .await;
            observed.notify_one();
            reader.read_exact(&mut byte).await.map_err(|source| {
                error(Stage::Close, Reason::Io, DeliveryState::Unknown).with_source(source)
            })?;
            Ok(())
        })
        .unwrap();
    pending.notified().await;
    let mut owner = fixture.owner(scope, UdpReceiveQueueConfig::default(), None);
    owner.start(deadline()).await.unwrap();
    let endpoint = owner.endpoint();
    let target = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (mut tx, _rx, mut control) = fixture.open(
        &endpoint,
        target.local_addr().unwrap(),
        CancellationToken::new(),
    );
    let original = send(&mut tx, &target, b"original fd").await;
    let same_state = owner.shared.clone();
    owner.shared.scope_cancel.cancel();
    let io_guard = same_state.sockets.lock().await;
    let mut close = Box::pin(owner.shutdown(deadline()));
    poll_fn(|cx| {
        assert!(close.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(close);
    assert!(tokio::net::UdpSocket::bind(original).await.is_err());
    let timed = owner.shutdown(Instant::now()).await.unwrap();
    assert!(!timed.is_complete());
    assert!(timed.unfinished.iter().any(|entry| matches!(entry, zero_platform_tokio::contract::lifecycle::Unfinished::Task(id) if *id == gate_id)));
    assert!(Arc::ptr_eq(&same_state, &owner.shared));
    assert!(tokio::net::UdpSocket::bind(original).await.is_err());
    peer.write_all(b"x").await.unwrap();
    drop(io_guard);
    let complete = owner.shutdown(deadline()).await.unwrap();
    assert!(complete.is_complete());
    assert!(complete.exited_tasks.contains(&gate_id));
    assert_eq!(
        complete.closed_io,
        vec!["explicitly closed 1 original Direct UDP sockets"]
    );
    tokio::net::UdpSocket::bind(original).await.unwrap();
    assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
}

#[tokio::test]
async fn actual_scope_panic_retains_source_and_same_native_state_for_explicit_close() {
    let fixture = Fixture::new().await;
    let scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(2).unwrap(),
    );
    let (trigger, ready) = tokio::sync::oneshot::channel::<Arc<Shared>>();
    let panic_id = scope
        .registrar()
        .spawn(TaskClass::Critical, async move {
            let shared = ready.await.unwrap();
            let _guard = shared.state.lock().unwrap();
            panic!("real critical task panic while retaining shared Direct metadata");
        })
        .unwrap();
    let mut owner = fixture.owner(scope, UdpReceiveQueueConfig::default(), None);
    owner.start(deadline()).await.unwrap();
    let target = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (mut tx, _rx, mut control) = fixture.open(
        &owner.endpoint(),
        target.local_addr().unwrap(),
        CancellationToken::new(),
    );
    let original = send(&mut tx, &target, b"original before panic").await;
    assert!(trigger.send(owner.shared.clone()).is_ok());
    let report = owner.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert!(report.exited_tasks.contains(&panic_id));
    assert_eq!(report.failed_tasks.len(), 1);
    assert_eq!(report.failed_tasks[0].id, panic_id);
    let mut source: &(dyn std::error::Error + 'static) = &report.failed_tasks[0].error;
    loop {
        if let Some(join) = source.downcast_ref::<tokio::task::JoinError>() {
            assert!(join.is_panic());
            break;
        }
        source = source
            .source()
            .expect("original scoped panic source retained");
    }
    assert_eq!(
        report.closed_io,
        vec!["explicitly closed 1 original Direct UDP sockets"]
    );
    tokio::net::UdpSocket::bind(original).await.unwrap();
    assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
    // Delivering the first report does not discard the original critical error.
    let repeated = owner.shutdown(deadline()).await.unwrap();
    assert_eq!(repeated.failed_tasks.len(), 1);
}

#[tokio::test]
async fn actual_scope_task_waiting_for_logical_cleanup_cannot_deadlock_native_close() {
    let fixture = Fixture::new().await;
    let scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(2).unwrap(),
    );
    let mut owner = fixture.owner(scope, UdpReceiveQueueConfig::default(), None);
    owner.start(deadline()).await.unwrap();
    let target = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (mut tx, _rx, mut control) = fixture.open(
        &owner.endpoint(),
        target.local_addr().unwrap(),
        CancellationToken::new(),
    );
    let original = send(&mut tx, &target, b"actual pending cleanup").await;
    let mut waiting = Box::pin(control.wait_closed(deadline()));
    poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(waiting);
    assert!(tokio::net::UdpSocket::bind(original).await.is_err());
    let cleanup_id = owner
        .scope
        .as_ref()
        .unwrap()
        .registrar()
        .spawn(TaskClass::Critical, async move {
            let report = control.wait_closed(deadline()).await?;
            if !report.is_complete() {
                return Err(error(Stage::Close, Reason::Timeout, DeliveryState::Unknown));
            }
            Ok(())
        })
        .unwrap();
    let report = owner.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert!(report.exited_tasks.contains(&cleanup_id));
    assert!(report.failed_tasks.is_empty());
    assert_eq!(
        report.closed_io,
        vec!["explicitly closed 1 original Direct UDP sockets"]
    );
    tokio::net::UdpSocket::bind(original).await.unwrap();
}
