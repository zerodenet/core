use super::*;
use crate::transport::plugin::fixtures::{PendingProbe, StartupMarker};
use std::{io, time::Duration};
use tokio::time::{timeout, Instant};

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn server_config(marker: &StartupMarker, tail: &str) -> PluginConfig {
    let mut config = marker.config(tail);
    config.args[1] = config.args[1]
        .replace("SS_LOCAL_HOST", "SS_REMOTE_HOST")
        .replace("SS_LOCAL_PORT", "SS_REMOTE_PORT");
    config
}
async fn listener() -> TokioListener {
    TokioListener::bind("127.0.0.1:0").await.unwrap()
}

#[tokio::test]
async fn cancelled_actual_inbound_readiness_retains_the_same_child_for_owner_reaping() {
    let marker = StartupMarker::new();
    let plan = Box::new(ShadowsocksInboundPluginPlan::new(server_config(
        &marker,
        "time.sleep(60)",
    )));
    let shared = plan.shared.clone();
    let mut owner = plan.owner();
    owner.start(deadline()).await.unwrap();
    let pending = PendingProbe::default();
    let request = tokio::spawn(plan.activate_with_probe(listener().await, pending.probe()));
    pending.entered().await;
    let pid = marker.pid().await;
    assert!(
        !request.is_finished(),
        "the actual TCP probe must be pending before abort"
    );
    request.abort();
    assert!(request.await.err().unwrap().is_cancelled());
    let report = owner.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert!(
        report.closed_io.is_empty(),
        "only the direct Child is reported here"
    );
    assert!(report
        .released_resources
        .iter()
        .any(|item| item.contains(&format!("child {pid} reaped:"))));
    assert!(
        shared.status().unwrap().is_some(),
        "actual try_wait must observe and reap the retained Child"
    );
}

#[tokio::test]
async fn dropping_inbound_completion_does_not_replace_explicit_reaping_confirmation() {
    let marker = StartupMarker::new();
    let plan = Box::new(ShadowsocksInboundPluginPlan::new(server_config(
        &marker,
        "s.listen(); time.sleep(60)",
    )));
    let shared = plan.shared.clone();
    let mut owner = plan.owner();
    owner.start(deadline()).await.unwrap();
    let original = listener().await;
    let remote = original.local_addr().unwrap();
    let carrier = timeout(Duration::from_secs(3), plan.activate(original))
        .await
        .unwrap()
        .unwrap();
    let pid = marker.pid().await;
    assert_ne!(carrier.listener.local_addr().unwrap(), remote);
    assert_eq!(carrier.datagram.local_addr().unwrap(), remote);
    drop(carrier);
    assert!(
        shared.status().unwrap().is_none(),
        "completion is a monitor, not the unique Child owner"
    );
    let report = owner.shutdown(deadline()).await.unwrap();
    assert!(report.released_resources[0].contains(&format!("child {pid} reaped:")));
    assert!(shared.status().unwrap().is_some());
}

#[tokio::test]
async fn actual_inbound_spawn_failure_preserves_os_source_and_closed_owner_cannot_activate() {
    let marker = StartupMarker::new();
    let mut config = server_config(&marker, "time.sleep(60)");
    config.command = marker
        .0
        .with_extension("missing-program")
        .to_string_lossy()
        .into_owned();
    let plan = Box::new(ShadowsocksInboundPluginPlan::new(config));
    let mut owner = plan.owner();
    owner.start(deadline()).await.unwrap();
    let result = plan.activate(listener().await).await;
    match result {
        Err(RuntimeError::Io(source)) => assert_eq!(source.kind(), io::ErrorKind::NotFound),
        _ => panic!("expected the actual native spawn failure"),
    }
    assert!(owner
        .shutdown(deadline())
        .await
        .unwrap()
        .released_resources
        .is_empty());

    let plan = Box::new(ShadowsocksInboundPluginPlan::new(server_config(
        &marker,
        "time.sleep(60)",
    )));
    let shared = plan.shared.clone();
    let mut owner = plan.owner();
    owner.start(deadline()).await.unwrap();
    owner.shutdown(deadline()).await.unwrap();
    assert!(plan.activate(listener().await).await.is_err());
    assert!(shared.status().is_err());
}
