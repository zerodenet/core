use super::*;
use std::{future::Future, task::Poll};
use tokio::sync::oneshot;

fn endpoints() -> (IpAddr, Endpoint) {
    (
        "10.0.0.1".parse().unwrap(),
        Endpoint {
            ip: "10.0.0.2".parse().unwrap(),
            port: 53,
        },
    )
}
async fn pending(
    sender: ClientUdpSender,
    target: Endpoint,
    payload: Vec<u8>,
) -> tokio::task::JoinHandle<Result<(), ClientUdpSendError>> {
    let (observed, ready) = oneshot::channel();
    let worker = tokio::spawn(async move {
        let mut sending = Box::pin(sender.send_to(&payload, target));
        std::future::poll_fn(|cx| {
            assert!(sending.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        observed.send(()).unwrap();
        sending.await
    });
    ready.await.unwrap();
    worker
}
#[tokio::test]
async fn logical_close_fences_the_same_pending_output_and_keeps_already_accepted_data() {
    let (local, target) = endpoints();
    let (output, mut received) = mpsc::channel(1);
    let stack = ClientUdpStack::new(vec![local], output, 1500).unwrap();
    let mut socket = stack.bind(local).unwrap();
    socket.send_to(b"already-accepted", target).await.unwrap();
    let sender = socket.sender();
    let alias = sender.clone();
    let worker = pending(sender, target, b"never-submitted".to_vec()).await;
    let original = socket.close();
    assert!(
        !original.io_quiescent(),
        "the actual pending sender still holds IO custody"
    );
    let failure = worker.await.unwrap().unwrap_err();
    assert_eq!(
        failure,
        ClientUdpSendError {
            kind: ClientUdpStackError::OutboundClosed,
            accepted_fragments: 0
        }
    );
    original.wait_io_quiescent().await;
    assert!(original.same_original(&socket.close()));
    let queued = received.recv().await.unwrap();
    assert_eq!(
        packet::parse_udp(&queued).unwrap().payload,
        b"already-accepted"
    );
    assert!(received.try_recv().is_err());
    let mut next = stack.bind(local).unwrap();
    assert_eq!(
        alias
            .send_to(b"retired-alias", target)
            .await
            .unwrap_err()
            .accepted_fragments,
        0
    );
    next.send_to(b"new-original", target).await.unwrap();
    assert_eq!(
        packet::parse_udp(&received.recv().await.unwrap())
            .unwrap()
            .payload,
        b"new-original"
    );
    assert!(!original.same_original(&next.close()));
}
#[tokio::test]
async fn a_real_partial_fragment_submission_remains_non_replayable_after_logical_close() {
    let (local, target) = endpoints();
    let (output, mut received) = mpsc::channel(1);
    let stack = ClientUdpStack::new(vec![local], output, 68).unwrap();
    let mut socket = stack.bind(local).unwrap();
    let worker = pending(socket.sender(), target, vec![0x5a; 200]).await;
    let original = socket.close();
    assert!(!original.io_quiescent());
    let failure = worker.await.unwrap().unwrap_err();
    assert_eq!(failure.kind, ClientUdpStackError::OutboundClosed);
    assert_eq!(failure.accepted_fragments, 1);
    assert!(original.io_quiescent());
    let fragment = received.recv().await.unwrap();
    assert!(fragment.len() <= 68);
    assert_ne!(u16::from_be_bytes([fragment[6], fragment[7]]) & 0x2000, 0);
    assert!(received.try_recv().is_err());
}
#[tokio::test]
async fn whole_stack_shutdown_wakes_the_actual_retained_sender_without_an_output_receiver_exit() {
    let (local, target) = endpoints();
    let (output, mut received) = mpsc::channel(1);
    let stack = ClientUdpStack::new(vec![local], output, 1500).unwrap();
    let mut socket = stack.bind(local).unwrap();
    socket.send_to(b"charged-original", target).await.unwrap();
    let worker = pending(socket.sender(), target, b"pending-original".to_vec()).await;
    stack.shutdown();
    let failure = worker.await.unwrap().unwrap_err();
    assert_eq!(failure.accepted_fragments, 0);
    assert!(socket.close().io_quiescent());
    assert_eq!(
        packet::parse_udp(&received.recv().await.unwrap())
            .unwrap()
            .payload,
        b"charged-original"
    );
}
