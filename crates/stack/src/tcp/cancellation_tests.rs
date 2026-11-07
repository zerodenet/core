use std::time::Duration;

use tokio::time::timeout;
use zero_traits::TcpStack;

use super::*;

#[tokio::test]
async fn cancelled_accept_preserves_dequeued_connection() {
    let (outbound, mut responses) = mpsc::channel(256);
    let stack = UserTcpStack::new(outbound, 1_500);
    let client_ip = "10.0.0.2".parse().unwrap();
    let server_ip = "10.0.0.1".parse().unwrap();
    let client_port = 54_321;
    let server_port = 443;

    let syn = packet::build_tcp_with_mss(
        client_ip,
        server_ip,
        client_port,
        server_port,
        1_000,
        0,
        tcp_flags::SYN,
        1_460,
    );
    stack.feed(&syn).await;
    let syn_ack = responses.recv().await.expect("SYN-ACK response");
    let parsed = packet::parse_tcp(&syn_ack).expect("parse SYN-ACK");
    let ack = packet::build_tcp(
        client_ip,
        server_ip,
        client_port,
        server_port,
        1_001,
        parsed.seq.wrapping_add(1),
        tcp_flags::ACK,
        &[],
    );
    stack.feed(&ack).await;

    // Force accept() to dequeue the connection and then wait while validating
    // it. Cancelling at that exact await used to drop the ReadyConn and emit RST.
    let connections = stack.connections.lock().await;
    let mut interrupted = Box::pin(stack.accept());
    tokio::select! {
        biased;
        _ = &mut interrupted => panic!("accept unexpectedly completed while connection state was locked"),
        _ = tokio::time::sleep(Duration::from_millis(10)) => {}
    }
    assert_eq!(
        stack.accept_tx.capacity(),
        64,
        "the interrupted accept must have dequeued the ready connection"
    );
    drop(interrupted);
    drop(connections);

    let (_, source, destination) = timeout(Duration::from_millis(100), stack.accept())
        .await
        .expect("preserved connection accept timed out")
        .expect("preserved connection disappeared");
    assert_eq!(source.port, client_port);
    assert_eq!(destination.port, server_port);
}

#[tokio::test]
async fn idle_accept_keeps_the_actual_queue_observable_for_source_pause() {
    let (outbound, _responses) = mpsc::channel(256);
    let stack = UserTcpStack::new(outbound, 1_500);
    let mut accepting = Box::pin(stack.accept());
    std::future::poll_fn(|cx| {
        assert!(accepting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    // This is the same real accept consumer, still Pending. Its consumer lease
    // is not queued work; no fabricated empty flag substitutes for the queue.
    let mut census = Box::pin(stack.quiescent());
    std::future::poll_fn(|cx| {
        assert!(
            matches!(census.as_mut().poll(cx), Poll::Ready(true)),
            "idle Source census must read its actual queue without awaiting its own Pending accept"
        );
        Poll::Ready(())
    })
    .await;
    drop(accepting);
}

#[tokio::test]
async fn actual_ready_accept_wakes_after_queue_and_connection_locks_return() {
    struct Reentrant {
        stack: std::sync::Weak<UserTcpStack>,
        woke: AtomicBool,
        locked: AtomicBool,
    }
    impl std::task::Wake for Reentrant {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            let Some(stack) = self.stack.upgrade() else {
                return; // The actual stack has finished dropping, not a join/close claim.
            };
            if stack.connections.try_lock().is_err() || stack.accept_queue.try_lock().is_err() {
                self.locked.store(true, Ordering::Release);
            }
            self.woke.store(true, Ordering::Release);
        }
    }
    let (outbound, mut responses) = mpsc::channel(256);
    let stack = Arc::new(UserTcpStack::new(outbound, 1_500));
    let observer = Arc::new(Reentrant {
        stack: Arc::downgrade(&stack),
        woke: AtomicBool::new(false),
        locked: AtomicBool::new(false),
    });
    let wake = std::task::Waker::from(observer.clone());
    let mut context = Context::from_waker(&wake);
    let mut accepting = Box::pin(stack.accept());
    assert!(accepting.as_mut().poll(&mut context).is_pending());
    let client_ip = "10.0.0.2".parse().unwrap();
    let server_ip = "10.0.0.1".parse().unwrap();
    stack
        .feed(&packet::build_tcp_with_mss(
            client_ip,
            server_ip,
            54_321,
            443,
            1_000,
            0,
            tcp_flags::SYN,
            1_460,
        ))
        .await;
    let response = responses.recv().await.expect("real SYN-ACK");
    let sequence = packet::parse_tcp(&response).unwrap().seq;
    stack
        .feed(&packet::build_tcp(
            client_ip,
            server_ip,
            54_321,
            443,
            1_001,
            sequence.wrapping_add(1),
            tcp_flags::ACK,
            &[],
        ))
        .await;
    assert!(observer.woke.load(Ordering::Acquire));
    assert!(
        !observer.locked.load(Ordering::Acquire),
        "actual accept wake must leave both original owner locks before external reentry"
    );
    let Poll::Ready(Some((_stream, source, destination))) = accepting.as_mut().poll(&mut context)
    else {
        panic!("same original Pending accept did not receive the real completed handshake");
    };
    assert_eq!(source.port, 54_321);
    assert_eq!(destination.port, 443);
}
