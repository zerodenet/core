use super::*;
use std::{
    future::Future,
    task::{Context, Poll},
    time::Duration,
};

mod allocation;

async fn two_sockets() -> (
    crate::runtime::Proxy,
    DirectUdpSockets,
    tokio::net::UdpSocket,
    Vec<SocketAddr>,
) {
    let proxy = proxy();
    let mut sockets = socket_set(&proxy, None);
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut bytes = [0; 32];
    let mut locals = Vec::new();
    for (tag, session) in [("first", 1), ("second", 2)] {
        sockets
            .send_to_addr(
                b"request",
                peer.local_addr().unwrap(),
                session,
                &policy(&proxy, Some(tag)),
            )
            .await
            .unwrap();
        locals.push(peer.recv_from(&mut bytes).await.unwrap().1);
    }
    (proxy, sockets, peer, locals)
}

#[tokio::test]
async fn full_buffer_receives_large_and_empty_datagrams_without_scratch_storage() {
    let (_proxy, sockets, peer, locals) = two_sockets().await;
    let payload = vec![0x5a; 8_192];
    let mut output = vec![0; 65_535];
    peer.send_to(&payload, locals[0]).await.unwrap();
    let (size, source) =
        tokio::time::timeout(Duration::from_secs(2), sockets.recv_from_addr(&mut output))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(&output[..size], &payload);
    assert_eq!(source.session_id, Some(1));
    assert!(source.is_current());
    assert_eq!(sockets.receive.lock().await.scratch.capacity(), 0);
    peer.send_to(&[], locals[1]).await.unwrap();
    let (size, source) = sockets.recv_from_addr(&mut output).await.unwrap();
    assert_eq!(size, 0);
    assert_eq!(source.session_id, Some(2));
    assert_eq!(sockets.receive.lock().await.scratch.capacity(), 0);
}

#[tokio::test]
async fn short_receive_drains_datagrams_and_shares_one_scratch_across_sockets() {
    let (_proxy, mut sockets, peer, locals) = two_sockets().await;
    let payload = vec![0x5a; 8_192];
    let mut output = [0; 4];
    for (index, session) in [(0, 1), (1, 2)] {
        peer.send_to(&payload, locals[index]).await.unwrap();
        let (size, source) = sockets.recv_from_addr(&mut output).await.unwrap();
        assert_eq!(size, 4);
        assert_eq!(source.session_id, Some(session));
        assert_eq!(output, [0x5a; 4]);
        assert_eq!(sockets.receive.lock().await.scratch.len(), 65_535);
    }
    peer.send_to(b"next", locals[0]).await.unwrap();
    let (size, _) = sockets.recv_from_addr(&mut output).await.unwrap();
    assert_eq!(
        &output[..size],
        b"next",
        "no remainder of the truncated datagram"
    );
    sockets.retire_session(1);
    assert_eq!(sockets.receive.lock().await.scratch.len(), 65_535);
    sockets.retire_session(2);
    assert_eq!(sockets.receive.lock().await.scratch.capacity(), 0);
}

#[tokio::test]
async fn cancelling_receive_and_queued_reader_does_not_steal_the_next_reply() {
    let (_proxy, sockets, peer, locals) = two_sockets().await;
    let mut output = [0; 8];
    let mut second = [0; 8];
    let waker = futures_util::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    {
        let mut owner = std::pin::pin!(sockets.recv_from_addr(&mut output));
        assert!(owner.as_mut().poll(&mut cx).is_pending());
        let mut waiter = std::pin::pin!(sockets.recv_from_addr(&mut second));
        assert!(waiter.as_mut().poll(&mut cx).is_pending());
    }
    peer.send_to(b"reply", locals[1]).await.unwrap();
    let (size, source) =
        tokio::time::timeout(Duration::from_secs(2), sockets.recv_from_addr(&mut output))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(&output[..size], b"reply");
    assert_eq!(source.session_id, Some(2));
}

#[tokio::test]
async fn concurrent_readers_receive_distinct_datagrams_without_losing_wakeups() {
    let (_proxy, sockets, peer, locals) = two_sockets().await;
    let mut first = [0; 8];
    let mut second = [0; 8];
    let receive = async {
        tokio::join!(
            sockets.recv_from_addr(&mut first),
            sockets.recv_from_addr(&mut second)
        )
    };
    let send = async {
        tokio::task::yield_now().await;
        peer.send_to(b"one", locals[0]).await.unwrap();
        peer.send_to(b"two", locals[1]).await.unwrap();
    };
    let ((a, b), ()) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(receive, send)
    })
    .await
    .unwrap();
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_ne!(a.1.session_id, b.1.session_id);
    assert_eq!(a.0, 3);
    assert_eq!(b.0, 3);
}

#[tokio::test]
async fn noisy_unregistered_socket_does_not_starve_a_valid_reply() {
    let (_proxy, sockets, peer, locals) = two_sockets().await;
    let stranger = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    for _ in 0..32 {
        stranger.send_to(b"unknown", locals[0]).await.unwrap();
    }
    peer.send_to(b"valid", locals[1]).await.unwrap();
    let mut output = [0; 8];
    let (size, source) =
        tokio::time::timeout(Duration::from_secs(2), sockets.recv_from_addr(&mut output))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(&output[..size], b"valid");
    assert_eq!(source.session_id, Some(2));
    assert_eq!(sockets.receive.lock().await.cursor, 0);
}

#[tokio::test]
async fn warm_receive_polling_does_not_allocate_a_future_set() {
    let (_proxy, sockets, peer, locals) = two_sockets().await;
    let mut output = vec![0; 65_535];
    let waker = futures_util::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    // Warm platform mutex/readiness bookkeeping before measuring this thread.
    for _ in 0..2 {
        let mut receive = std::pin::pin!(sockets.recv_from_addr(&mut output));
        assert!(receive.as_mut().poll(&mut cx).is_pending());
    }
    let (_, counts) = allocation::measure(65_535, || {
        for _ in 0..100 {
            let mut receive = std::pin::pin!(sockets.recv_from_addr(&mut output));
            assert!(matches!(receive.as_mut().poll(&mut cx), Poll::Pending));
        }
    });
    assert_eq!(counts.allocations, 0);
    peer.send_to(b"ready", locals[1]).await.unwrap();
    let mut ready = std::pin::pin!(sockets.recv_from_addr(&mut output));
    let (size, source) = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let (result, counts) = allocation::measure(65_535, || ready.as_mut().poll(&mut cx));
            assert_eq!(counts.allocations, 0, "ready I/O also uses caller storage");
            if let Poll::Ready(result) = result {
                break result.unwrap();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(size, 5);
    assert_eq!(source.session_id, Some(2));
    // Reproduce only the removed waiting-set bookkeeping, without packet I/O.
    let (_, reference) = allocation::measure(65_535, || {
        for _ in 0..100 {
            let set = futures_util::stream::FuturesUnordered::new();
            for _ in &sockets.sockets {
                set.push(std::future::pending::<()>());
            }
            drop(set);
        }
    });
    assert!(reference.allocations >= 200);
    println!("Direct UDP waiting bookkeeping, 100 cancelled polls / 2 sockets: new={} allocations, old-set reference={} allocations", counts.allocations, reference.allocations);
}
