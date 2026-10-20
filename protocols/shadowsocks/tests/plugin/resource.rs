use super::*;
use crate::transport::{
    plugin::fixtures::{config, PendingProbe, StartupMarker},
    ShadowsocksTransportState,
};
use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll, Waker},
};

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

#[tokio::test]
async fn shared_plugin_resource_confirms_the_actual_child_exit_and_blocks_later_starts() {
    let state = ShadowsocksTransportState::default();
    let resource = state.plugin_resource("127.0.0.1", 9999, config());
    let same = state.plugin_resource("127.0.0.1", 9999, config());
    assert!(Arc::ptr_eq(&resource.plan.running, &same.plan.running));
    assert_eq!(resource.start().await.unwrap(), same.start().await.unwrap());
    let closing = resource.clone();
    let result = tokio::spawn(async move { closing.shutdown(deadline()).await })
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, PluginTermination::Exited(status) if !status.success()));
    assert_eq!(same.shutdown(deadline()).await.unwrap(), result);
    assert_eq!(
        same.start().await.unwrap_err().kind(),
        io::ErrorKind::ConnectionAborted
    );
    assert_eq!(
        resource
            .plan
            .running
            .lock()
            .await
            .as_ref()
            .unwrap()
            .child
            .lock()
            .unwrap()
            .id(),
        None
    );
}

#[tokio::test]
async fn a_cold_resource_can_close_without_starting_a_process() {
    let state = ShadowsocksTransportState::default();
    let resource = state.plugin_resource("127.0.0.1", 9999, config());
    assert_eq!(
        resource.shutdown(deadline()).await.unwrap(),
        PluginTermination::NotStarted
    );
    assert!(resource.start().await.is_err());
    assert!(resource.plan.running.lock().await.is_none());
}

#[tokio::test]
async fn shutdown_cancels_pending_readiness_and_waits_for_the_retained_child() {
    let marker = StartupMarker::new();
    let state = ShadowsocksTransportState::default();
    let resource = state.plugin_resource("127.0.0.1", 9999, marker.config("time.sleep(60)"));
    let pending = PendingProbe::default();
    let probe = pending.probe();
    let starting = resource.clone();
    let request = tokio::spawn(async move { starting.start_with_probe(probe).await });
    pending.entered().await;
    let pid = marker.pid().await;
    assert!(
        !request.is_finished(),
        "shutdown must cancel pending readiness"
    );
    assert!(matches!(
        resource.shutdown(deadline()).await.unwrap(),
        PluginTermination::Exited(_)
    ));
    assert_eq!(
        request.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::ConnectionAborted
    );
    let slot = resource.plan.running.lock().await;
    let child = &slot.as_ref().unwrap().child;
    assert_eq!(
        child.lock().unwrap().id(),
        None,
        "PID {pid} must actually be reaped"
    );
    assert_eq!(
        std::fs::read_to_string(&marker.0).unwrap().lines().count(),
        1
    );
}

#[tokio::test]
async fn interrupted_and_timed_out_cleanup_keep_the_child_for_confirmed_retry() {
    let state = ShadowsocksTransportState::default();
    let resource = state.plugin_resource("127.0.0.1", 9999, config());
    resource.start().await.unwrap();
    let slot = resource.plan.running.lock().await;
    let child = &slot.as_ref().unwrap().child;
    let pid = child.lock().unwrap().id().unwrap();
    let mut closing = Box::pin(resource.shutdown(deadline()));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(closing.as_mut().poll(&mut cx), Poll::Pending));
    drop(closing);
    assert_eq!(child.lock().unwrap().id(), Some(pid));
    assert!(child.lock().unwrap().try_wait().unwrap().is_none());
    let error = resource
        .shutdown(Instant::now() + Duration::from_millis(10))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(child.lock().unwrap().id(), Some(pid));
    assert!(child.lock().unwrap().try_wait().unwrap().is_none());
    drop(slot);
    assert!(matches!(
        resource.shutdown(deadline()).await.unwrap(),
        PluginTermination::Exited(_)
    ));
    assert!(resource.start().await.is_err());
}
