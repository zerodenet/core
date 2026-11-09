use super::*;
use std::{num::NonZeroUsize, time::Duration};
use zero_platform_tokio::contract::scope::{HostedScope, TaskScope};
mod failures;
#[cfg(unix)]
mod isolation;
mod pressure;
mod task_pressure;
fn config(path: &std::path::Path) -> LogConfig {
    LogConfig {
        files: vec![zero_config::LogFileConfig {
            path: path.to_str().unwrap().into(),
            level: None,
            max_bytes: 4,
            max_files: 2,
        }],
        ..Default::default()
    }
}
fn prepare(config: &LogConfig) -> (HostedScope, ScopedTracing) {
    let mut scope = TaskScope::suspended_hosted(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(config.files.len()),
    );
    let owner = ScopedTracing::prepare(config, &scope.registrar()).unwrap();
    scope.enable_registration().unwrap();
    (scope, owner)
}
async fn until(
    scope: &mut HostedScope,
    owner: &mut ScopedTracing,
    ready: impl Fn(&ScopedTracing) -> bool,
) {
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            owner.poll_workers(scope, cx).unwrap();
            if ready(owner) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await
    .unwrap();
}
async fn initialized(scope: &mut HostedScope, owner: &mut ScopedTracing) {
    until(scope, owner, |owner| owner.ready() && owner.drained()).await;
}
async fn retire(scope: &mut HostedScope, owner: &mut ScopedTracing) {
    owner.request_stop();
    until(scope, owner, |owner| {
        owner.drained() && owner.native_closed()
    })
    .await;
    scope.begin_shutdown();
    until(scope, owner, |_| true).await;
    assert!(scope.is_drained(), "only actual joins and original ACKs");
    assert!(
        owner.close_after_workers(),
        "matching native close independent from Scope"
    );
    assert_eq!(owner.usage().outputs, 0);
}
#[tokio::test]
async fn original_idle_job_must_join_and_ack_before_the_next_chunk_runs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("idle.log");
    let config = config(&path);
    let (mut scope, mut owner) = prepare(&config);
    owner.drive(&scope.registrar()).unwrap();
    let receipt = tokio::time::timeout(Duration::from_secs(5), scope.next_receipt())
        .await
        .unwrap()
        .unwrap();
    let witness = owner.witness(0).unwrap();
    assert!(witness.matches_joined(receipt.joined()));
    assert!(witness.is_joined());
    assert!(!witness
        .try_joined()
        .expect("actual original task has joined")
        .registration_released());
    let mut writer = owner.writer(0).unwrap();
    writer.write_all(b"old\n").unwrap();
    owner.drive(&scope.registrar()).unwrap();
    assert!(owner.witness(0).unwrap().same_registration(&witness));
    assert_eq!(fs::read(&path).unwrap(), b"");
    assert_eq!(owner.usage().pending_bytes, 4);
    scope.acknowledge(&receipt).unwrap();
    drop(receipt);
    assert!(witness
        .try_joined()
        .expect("actual original task has joined")
        .registration_released());
    drop(witness); // actual observed header, not just an ACK
    until(&mut scope, &mut owner, |owner| owner.drained()).await;
    assert_eq!(fs::read(&path).unwrap(), b"old\n");
    assert_eq!(owner.usage().pending_bytes, 0);
    // Idle completion is not a permanently parked blocking thread. A new
    // enqueue starts a fresh real job while using the SAME open file owner.
    writer.write_all(b"new\n").unwrap();
    until(&mut scope, &mut owner, |owner| owner.drained()).await;
    assert_eq!(fs::read(&path).unwrap(), b"new\n");
    assert_eq!(fs::read(path.with_extension("1")).unwrap(), b"old\n");
    retire(&mut scope, &mut owner).await;
}
#[test]
fn optional_logging_limits_preserve_default_wire_and_check_real_layouts() {
    let config = LogConfig::default();
    assert_eq!(
        serde_json::to_value(&config).unwrap(),
        serde_json::json!({
            "level":"info", "files":[], "rate_limit":null
        })
    );
    let mut invalid = config.clone();
    invalid.max_pending_chunks = Some(0);
    assert!(invalid.validate_owner_limits().is_err());
    invalid.max_pending_chunks = Some(usize::MAX);
    assert!(book::Limits::new(&invalid).is_err());
    assert_eq!(config.effective_max_pending_chunks(), 128_000);
    assert_eq!(config.effective_max_pending_bytes(), 16 * 1024 * 1024);
}
