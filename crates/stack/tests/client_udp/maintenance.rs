use super::*;
use crate::packet;
use std::{net::IpAddr, time::Duration};
use tokio::sync::mpsc;

#[tokio::test]
async fn udp_maintenance_releases_fragment_and_port_bursts_without_retiring_the_live_socket() {
    let local: IpAddr = "10.0.0.1".parse().unwrap();
    let remote = packet::Endpoint {
        ip: "10.0.0.2".parse().unwrap(),
        port: 53,
    };
    let (sender, mut packets) = mpsc::channel(1);
    let stack = ClientUdpStack::new(vec![local], sender, 1420).unwrap();
    let mut burst: Vec<_> = (0..512).map(|_| stack.bind(local).unwrap()).collect();
    let mut live = burst.pop().unwrap();
    let peak = stack.inner.sockets.lock().unwrap().capacity();
    drop(burst);
    live.send_to(b"request", remote).await.unwrap();
    packets.recv().await.unwrap();
    let original = packet::build_udp(
        remote.ip,
        local,
        remote.port,
        live.local_endpoint().port,
        &[0x42; 192],
    );
    let fragments = packet::fragment_ip_packet(&original, 100, 1);
    assert!(!stack.feed_correlated(&fragments[0]));
    assert!(stack.inner.fragments.lock().unwrap().buffered_bytes() > 0);
    assert_eq!(
        stack.cleanup_expired(Instant::now() + Duration::from_secs(31)),
        1
    );
    assert_eq!(stack.inner.fragments.lock().unwrap().buffered_bytes(), 0);
    assert!(stack.inner.sockets.lock().unwrap().capacity() < peak / 4);
    let reply = packet::build_udp(
        remote.ip,
        local,
        remote.port,
        live.local_endpoint().port,
        b"still-live",
    );
    assert!(stack.feed_correlated(&reply));
    assert_eq!(live.recv_from().await.unwrap().payload, b"still-live");
    drop(live);
    stack.cleanup_expired(Instant::now() + Duration::from_secs(32));
    // A tiny table retains ordinary churn capacity; a burst with no live
    // sockets releases all of its buckets.
    let empty = ClientUdpStack::new(vec![local], mpsc::channel(1).0, 1420).unwrap();
    let sockets: Vec<_> = (0..512).map(|_| empty.bind(local).unwrap()).collect();
    drop(sockets);
    empty.cleanup_expired(Instant::now());
    assert_eq!(empty.inner.sockets.lock().unwrap().capacity(), 0);
}

#[tokio::test]
async fn shutdown_wakes_the_actual_pending_socket_and_fences_all_future_bindings_and_sends() {
    use std::{future::Future, task::Poll};
    let local: IpAddr = "10.0.0.1".parse().unwrap();
    let (sender, _packets) = mpsc::channel(1);
    let stack = ClientUdpStack::new(vec![local], sender, 1420).unwrap();
    let mut socket = stack.bind(local).unwrap();
    let mut receiving = Box::pin(socket.recv_event());
    std::future::poll_fn(|cx| {
        assert!(receiving.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    stack.shutdown();
    assert!(receiving.await.is_none());
    assert!(matches!(
        stack.bind(local),
        Err(super::super::ClientUdpStackError::OutboundClosed)
    ));
    assert_eq!(
        socket
            .send_to(
                b"retired",
                packet::Endpoint {
                    ip: "10.0.0.2".parse().unwrap(),
                    port: 53
                }
            )
            .await,
        Err(super::super::ClientUdpStackError::OutboundClosed)
    );
    assert!(stack.inner.sockets.lock().unwrap().is_empty());
}

#[tokio::test]
async fn shutdown_does_not_deliver_a_reply_queued_by_the_retired_stack() {
    let local: IpAddr = "10.0.0.1".parse().unwrap();
    let (sender, _packets) = mpsc::channel(1);
    let stack = ClientUdpStack::new(vec![local], sender, 1420).unwrap();
    let mut socket = stack.bind(local).unwrap();
    let reply = packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        local,
        53,
        socket.local_endpoint().port,
        b"old-generation",
    );
    assert!(stack.feed(&reply));
    stack.shutdown();
    assert!(socket.recv_event().await.is_none());
    assert!(!stack.feed(&reply));
}
