use super::*;
use std::{future::Future, task::Poll, time::Duration};
use tokio::sync::{mpsc, watch};

fn prepared_recipe() -> (
    EndpointStackRecipe,
    mpsc::Receiver<crate::runtime::raw_ip::EndpointPacket>,
) {
    let (sender, packets) = mpsc::channel(4);
    let (_, endpoint) = watch::channel(sender);
    (
        EndpointStackRecipe::prepare(
            vec!["10.0.0.1".parse().unwrap()],
            1420,
            0,
            endpoint,
            Default::default(),
        )
        .unwrap(),
        packets,
    )
}
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(3)
}
#[test]
fn original_task_owner_and_recipe_are_pure_without_runtime() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let owner = EndpointStackOwner::new();
    let (recipe, _packets) = prepared_recipe();
    assert!(!owner.state.lock().unwrap().attempted);
    drop(recipe);
    assert!(owner.state.lock().unwrap().device.is_none());
}
#[tokio::test]
async fn candidate_retains_original_stack_driver_and_control_until_same_owner_join() {
    let owner = EndpointStackOwner::new();
    let (recipe, mut packets) = prepared_recipe();
    let device = owner.start(recipe).unwrap();
    assert!(Arc::ptr_eq(&device.task, &owner.workers));
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
    let syn = tokio::time::timeout_at(deadline(), packets.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(zero_stack::packet::parse_tcp(&syn.packet).unwrap().syn);
    let report = owner.stop(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert!(report.closed_io.is_empty()); // the endpoint's listener closes apart.
    assert!(connecting.await.is_err());
    assert!(device.is_closed());
    assert!(owner.stop(deadline()).await.unwrap().is_complete());
}
#[tokio::test]
async fn cancelled_original_join_waiter_retains_the_candidate_and_prevents_late_start() {
    let owner = EndpointStackOwner::new();
    let (recipe, _packets) = prepared_recipe();
    let device = owner.start(recipe).unwrap();
    // Current-thread runtime: neither newly spawned original task can run
    // between this call and the first poll of their actual abort/join waiter.
    assert!(!owner.is_drained());
    let mut stopping = Box::pin(owner.stop(deadline()));
    std::future::poll_fn(|cx| {
        assert!(stopping.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(stopping);
    assert!(!owner.is_drained());
    assert!(Arc::ptr_eq(
        owner.state.lock().unwrap().device.as_ref().unwrap(),
        &device
    ));
    let (new_recipe, _packets) = prepared_recipe();
    assert!(owner.start(new_recipe).is_err());
    let report = owner.stop(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert!(report.closed_io.is_empty());
}
#[tokio::test]
async fn actual_task_admission_failure_survives_candidate_cleanup_with_original_source() {
    let owner = EndpointStackOwner::new();
    // Revoke the actual shared worker registrar, not a fake worker/future.
    owner.workers.abort();
    let (recipe, _packets) = prepared_recipe();
    let failed = match owner.start(recipe) {
        Err(error) => error,
        Ok(_) => panic!("revoked original registrar accepted a stack"),
    };
    let EngineError::Io(failed) = failed else {
        panic!("expected admission source")
    };
    let original = failed
        .get_ref()
        .unwrap()
        .downcast_ref::<SharedFailure>()
        .unwrap()
        .0
        .clone();
    let cleaned = owner.stop(deadline()).await.unwrap_err();
    let EngineError::Io(cleaned) = cleaned else {
        panic!("expected retained source")
    };
    assert!(Arc::ptr_eq(
        &original,
        &cleaned
            .get_ref()
            .unwrap()
            .downcast_ref::<SharedFailure>()
            .unwrap()
            .0
    ));
    assert!(owner.state.lock().unwrap().device.is_none());
}

#[tokio::test]
async fn original_panic_is_joined_even_when_its_source_remains_a_cleanup_failure() {
    let owner = EndpointStackOwner::new();
    let (body, ready) = tokio::sync::oneshot::channel();
    owner
        .workers
        .spawn_driver(async move {
            body.send(tokio::task::id()).unwrap();
            panic!("original endpoint fixture panic");
        })
        .unwrap();
    let actual_id = ready.await.unwrap();
    assert!(!owner.is_drained()); // body exited, no actual join consumed yet.
    let error = owner.stop(deadline()).await.unwrap_err();
    assert!(owner.is_drained());
    let mut source: &(dyn std::error::Error + 'static) = &error;
    let mut original = None;
    while let Some(next) = source.source() {
        if let Some(join) = next.downcast_ref::<tokio::task::JoinError>() {
            original = Some(join);
        }
        source = next;
    }
    let original = original.expect("original typed panic JoinError");
    assert!(original.is_panic());
    assert_eq!(original.id(), actual_id);
}
