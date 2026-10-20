//! Original per-socket workers join through the endpoint's same bounded owner.
use super::support::{address, ip, LOCAL, REMOTE};
use crate::runtime::{
    raw_ip::{EndpointStackOwner, EndpointStackRecipe},
    udp_flow::managed::{packet::PacketUdpFlow, ManagedTupleUdpFlowConnection},
};
use std::{future::Future, num::NonZeroUsize, sync::Arc, task::Poll, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};
use zero_platform_tokio::contract::scope::{HostedScope, TaskScope};

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}
fn scope(limit: usize) -> HostedScope {
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(limit));
    scope.enable_registration().unwrap();
    scope
}
async fn pending(future: impl Future) {
    tokio::pin!(future);
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}
async fn stop(
    graph: &EndpointStackOwner,
    scope: &mut HostedScope,
) -> Result<zero_platform_tokio::contract::lifecycle::CloseReport, zero_engine::EngineError> {
    let stopping = graph.stop(deadline());
    tokio::pin!(stopping);
    loop {
        tokio::select! {
            result = &mut stopping => return result,
            receipt = scope.next_receipt() => {
                let receipt = receipt.expect("same endpoint tasks await their original consumer");
                assert!(receipt.result().is_ok());
                scope.acknowledge(&receipt).unwrap();
            }
        }
    }
}
#[tokio::test]
async fn dropping_packet_udp_requests_exit_but_only_the_original_scope_joins_the_actual_worker() {
    let mut scope = scope(2); // actual endpoint driver + one UDP worker; no TCP demand
    let graph = EndpointStackOwner::suspended();
    graph.bind_scope(scope.registrar().downgrade()).unwrap();
    let (packets, mut submitted) = mpsc::channel(4);
    let (_publication, endpoint) = watch::channel(packets);
    let device = graph
        .start(
            EndpointStackRecipe::prepare(vec![ip(LOCAL)], 1420, 0, endpoint, Default::default())
                .unwrap(),
        )
        .unwrap();
    let users = device.flow_users().unwrap();
    let flow = PacketUdpFlow::open(
        device.clone(),
        address(ip(REMOTE)),
        443,
        "10.0.0.2:443".parse().unwrap(),
        ip(LOCAL),
        None,
    )
    .unwrap();
    let witnesses = graph.test_original_witnesses();
    assert_eq!(witnesses.len(), 2);
    let mut responses = flow.subscribe_responses();
    flow.send(&address(ip(REMOTE)), 443, b"original worker")
        .await
        .unwrap();
    let packet = tokio::time::timeout_at(deadline(), submitted.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        zero_stack::packet::parse_udp(&packet.packet)
            .unwrap()
            .payload,
        b"original worker"
    );
    assert!(witnesses.iter().all(|witness| !witness.is_joined()));
    pending(responses.recv()).await; // no incoming UDP packet; actual receive is pending
    drop(flow);
    // The response sender is destroyed by the original future, not a marker
    // task. That still is not a JoinSet-consumed exit or native-close proof.
    assert!(tokio::time::timeout_at(deadline(), responses.recv())
        .await
        .unwrap()
        .is_err());
    assert!(witnesses.iter().all(|witness| !witness.is_joined()));
    pending(graph.stop(deadline())).await;
    assert!(!graph.is_drained());
    let report = stop(&graph, &mut scope).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.exited_tasks.len(), 2);
    assert!(report.closed_io.is_empty());
    assert!(witnesses.iter().all(|witness| witness.is_joined()));
    // Naked logical response receivers still lack transferred-user provenance;
    // these worker joins must not make that other phase appear complete.
    assert!(!users.shutdown(deadline()).await.unwrap().is_complete());
    assert!(Arc::ptr_eq(&graph.device().unwrap(), &device));
}
#[tokio::test]
async fn actual_udp_worker_capacity_rejection_preserves_the_original_spawn_source_without_fallback()
{
    let mut scope = scope(1); // sole original endpoint driver; no UDP credit
    let graph = EndpointStackOwner::suspended();
    graph.bind_scope(scope.registrar().downgrade()).unwrap();
    let (packets, _submitted) = mpsc::channel(4);
    let (_publication, endpoint) = watch::channel(packets);
    let device = graph
        .start(
            EndpointStackRecipe::prepare(vec![ip(LOCAL)], 1420, 0, endpoint, Default::default())
                .unwrap(),
        )
        .unwrap();
    let error = match PacketUdpFlow::open(
        device,
        address(ip(REMOTE)),
        443,
        "10.0.0.2:443".parse().unwrap(),
        ip(LOCAL),
        None,
    ) {
        Ok(_) => panic!("UDP worker escaped actual endpoint task capacity"),
        Err(error) => error,
    };
    assert_eq!(graph.test_original_witnesses().len(), 1);
    let mut source: &(dyn std::error::Error + 'static) = &error;
    let mut original = None;
    while let Some(next) = source.source() {
        if let Some(spawn) = next.downcast_ref::<zero_platform_tokio::contract::scope::SpawnError>()
        {
            original = Some(*spawn);
        }
        source = next;
    }
    assert_eq!(
        original,
        Some(zero_platform_tokio::contract::scope::SpawnError::Capacity)
    );
    assert!(stop(&graph, &mut scope).await.is_err());
    assert!(graph.is_drained());
}
