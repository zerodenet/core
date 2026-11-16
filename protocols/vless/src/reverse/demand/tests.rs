//! Demand timers/status are protocol state only, not simulated native receipts.
use super::*;
use std::{future::Future, task::Poll};
fn demand() -> WorkerDemand {
    let protocol = crate::outbound::PreparedVlessOutboundRequestBundle::from_config_with_transport_hints_mux_policy_and_testseed(
        "11111111-2222-3333-4444-555555555555", None, &[], None, None, None, None, None, Default::default()).unwrap();
    WorkerDemand::from_protocol(&protocol)
}
async fn pending<F: Future>(next: &mut std::pin::Pin<Box<F>>) {
    std::future::poll_fn(|cx| {
        assert!(next.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}
#[tokio::test(start_paused = true)]
async fn interrupted_handshake_retains_retry_pacing_without_registering_a_worker() {
    let demand = demand();
    drop(demand.next().await);
    assert!(demand.ownership.workers.lock().unwrap().is_empty());
    let mut next = Box::pin(demand.next());
    pending(&mut next).await;
    tokio::time::advance(MONITOR_INTERVAL - std::time::Duration::from_millis(1)).await;
    pending(&mut next).await;
    tokio::time::advance(std::time::Duration::from_millis(1)).await;
    drop(next.await);
    assert!(demand.ownership.workers.lock().unwrap().is_empty());
}
#[tokio::test(start_paused = true)]
async fn demand_is_serialized_and_drained_worker_does_not_keep_it_closed() {
    let demand = demand();
    let first = demand.next().await;
    let mut second = Box::pin(demand.next());
    pending(&mut second).await;
    let server = first.server();
    drop(first);
    tokio::time::advance(MONITOR_INTERVAL).await;
    pending(&mut second).await;
    assert_eq!(demand.ownership.workers.lock().unwrap().len(), 1);
    drop(server);
    tokio::time::advance(MONITOR_INTERVAL).await;
    drop(second.await);
    assert!(demand.ownership.workers.lock().unwrap().is_empty());
}
