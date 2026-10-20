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
