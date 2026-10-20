use super::*;
use std::{future::Future, task::Poll};
use tokio::sync::{mpsc, oneshot, watch};
use zero_stack::TcpTaskOwner;

#[tokio::test]
async fn original_endpoint_driver_and_two_tcp_workers_are_retained_until_actual_join() {
    let (sender, mut packets) = mpsc::channel(4);
    let (_updates, endpoint) = watch::channel(sender);
    let device = super::super::SharedRawIpDevice::start_on_endpoint(
        vec!["10.0.0.1".parse().unwrap()],
        1420,
        0,
        endpoint,
    )
    .unwrap();
    let owner = device.task.clone();
    let weak = Arc::downgrade(&owner);
    let mut connecting = Box::pin(device.open_tcp_observed(
        "10.0.0.1".parse().unwrap(),
        "192.0.2.2:443".parse().unwrap(),
        None,
    ));
    std::future::poll_fn(|cx| {
        assert!(connecting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let syn = packets.recv().await.unwrap();
    assert!(
        zero_stack::packet::parse_tcp(&syn.packet).unwrap().flags
            & zero_stack::packet::tcp_flags::SYN
            != 0
    );
    // The driver has really consumed and submitted SYN, and connect is still
    // waiting for an actual handshake. None of the three original task handles
    // is silently replaced by an abort-only or Drop completion witness.
    assert_eq!(owner.tasks.lock().await.unwrap().len(), 3);
    drop(connecting);
    device.close_now();
    device.wait_workers_stopped().await.unwrap();
    assert!(!TcpTaskOwner::spawn(owner.as_ref(), Box::pin(async {})));
    // A second waiter uses the same consumed JoinSet and preserves the fence.
    device.wait_workers_stopped().await.unwrap();
    drop(device);
    drop(owner);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn original_driver_failure_survives_repeated_worker_join_results() {
    let owner = DeviceWorkers::new();
    let (done, ready) = oneshot::channel();
    owner
        .spawn_driver(async move {
            done.send(()).unwrap();
            Err(EngineError::Io(std::io::Error::from_raw_os_error(10013)))
        })
        .unwrap();
    ready.await.unwrap();
    for _ in 0..2 {
        let error = owner.join().await.unwrap_err();
        let mut source: &(dyn std::error::Error + 'static) = &error;
        let mut actual_os_error = None;
        while let Some(next) = source.source() {
            if let Some(EngineError::Io(error)) = next.downcast_ref::<EngineError>() {
                actual_os_error = error.raw_os_error();
            }
            source = next;
        }
        assert_eq!(actual_os_error, Some(10013));
    }
    assert!(!TcpTaskOwner::spawn(owner.as_ref(), Box::pin(async {})));
}
