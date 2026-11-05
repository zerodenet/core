use super::*;
use std::{
    future::{poll_fn, Future},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::atomic::{AtomicBool, Ordering},
    task::Poll,
};

#[tokio::test]
async fn running_blocking_child_is_not_closed_by_abort_or_a_cancelled_shutdown_wait() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut writer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut reader, _) = listener.accept().unwrap();
    let (started, entered) = tokio::sync::oneshot::channel();
    let mut scope = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
    let (id, witness) = scope
        .registrar()
        .spawn_blocking_tracked(TaskClass::Critical, move || {
            let _ = started.send(());
            reader.read_exact(&mut [0u8; 1]).map_err(|source| {
                ChannelError::new(Stage::Receive, Reason::Io, Impact::Component).with_source(source)
            })?;
            Ok(())
        })
        .unwrap();
    entered.await.unwrap();
    let mut actual = Box::pin(witness.wait());
    poll_fn(|cx| {
        assert!(actual.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(actual);
    let report = scope.shutdown(Instant::now()).await;
    assert!(!report.is_complete());
    assert!(!witness.is_joined());
    writer.write_all(&[1]).unwrap();
    let report = scope
        .shutdown(Instant::now() + std::time::Duration::from_secs(2))
        .await;
    assert!(report.is_complete());
    let joined = witness.wait().await;
    assert_eq!(joined.id(), id);
    assert!(witness.matches_joined(&joined));
    joined.result().unwrap();
}

#[tokio::test]
async fn taskless_original_cannot_start_a_blocking_child() {
    let scope = TaskScope::empty(Handle::current());
    let ran = Arc::new(AtomicBool::new(false));
    let called = ran.clone();
    assert!(matches!(
        scope
            .registrar()
            .spawn_blocking_tracked(TaskClass::Critical, move || {
                called.store(true, Ordering::Release);
                Ok(())
            }),
        Err(SpawnError::Capacity)
    ));
    assert!(!ran.load(Ordering::Acquire));
}
