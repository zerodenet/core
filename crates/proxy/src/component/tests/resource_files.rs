use super::*;
#[path = "resource_files/cleanup.rs"]
mod cleanup;
#[path = "resource_files/concurrency.rs"]
mod concurrency;
#[path = "resource_files/fixtures.rs"]
mod fixtures;
#[path = "resource_files/native.rs"]
mod native;
use fixtures::*;
use tokio::time::{Duration, Instant};
use zero_platform_tokio::contract::component::{ResourceAccess, ResourceDeclaration};
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn grant(path: PathBuf, max_bytes: usize) -> Arc<HostResources> {
    HostResources::new(
        ComponentId(201),
        &[ResourceDeclaration::File { path, max_bytes }],
        CancellationToken::new(),
    )
    .unwrap()
}
fn io_kind(error: &(dyn Error + 'static)) -> Option<io::ErrorKind> {
    if let Some(error) = error.downcast_ref::<io::Error>() {
        return Some(error.kind());
    }
    error.source().and_then(io_kind)
}
#[tokio::test]
async fn declared_file_is_one_bounded_snapshot_and_exact_path_authority_is_start_only() {
    let directory = Directory::new();
    let path = directory.0.join("certificate");
    std::fs::write(&path, b"original").unwrap();
    let grants = grant(path.clone(), 8);
    assert_eq!(
        grants.read_file(path.clone(), 8).await.unwrap_err().reason,
        Reason::InvalidConfig
    );
    let access = grants.activate(deadline()).unwrap();
    assert_eq!(
        grants
            .read_file(path.with_extension("other"), 8)
            .await
            .unwrap_err()
            .reason,
        Reason::InvalidConfig
    );
    assert_eq!(
        grants.read_file(path.clone(), 9).await.unwrap_err().reason,
        Reason::InvalidConfig
    );
    assert_eq!(
        grants.read_file(path.clone(), 8).await.unwrap(),
        b"original"
    );
    std::fs::write(&path, b"changed!").unwrap();
    assert_eq!(
        grants.read_file(path.clone(), 8).await.unwrap(),
        b"original"
    );
    assert_eq!(
        grants.read_file(path.clone(), 7).await.unwrap_err().reason,
        Reason::Capacity
    );
    assert_eq!(
        grants
            .state
            .lock()
            .unwrap()
            .files
            .receipts
            .released_resources
            .len(),
        1
    );
    drop(access);
    assert_eq!(
        grants.read_file(path.clone(), 8).await.unwrap_err().reason,
        Reason::InvalidConfig
    );
    let report = grants.shutdown_io(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.released_resources.len(), 1);
    assert!(report.closed_io.is_empty());
    assert!(matches!(grants.activate(deadline()),Err(error) if error.reason==Reason::Revoked));
}
#[tokio::test]
async fn actual_file_failures_keep_their_native_source_and_do_not_restart_jobs() {
    let directory = Directory::new();
    let path = directory.0.join("absent");
    let grants = grant(path.clone(), 4);
    let _access = grants.activate(deadline()).unwrap();
    let error = grants.read_file(path.clone(), 4).await.unwrap_err();
    assert_eq!(error.reason, Reason::Io);
    assert_eq!(io_kind(&error), Some(io::ErrorKind::NotFound));
    std::fs::write(&path, b"now!").unwrap();
    let repeated = grants.read_file(path, 4).await.unwrap_err();
    assert_eq!(io_kind(&repeated), Some(io::ErrorKind::NotFound));
    assert_eq!(
        grants
            .shutdown_io(deadline())
            .await
            .unwrap()
            .released_resources
            .len(),
        1
    );
    let actual = directory.0.join("large");
    std::fs::write(&actual, b"large").unwrap();
    let grants = grant(actual.clone(), 4);
    let _access = grants.activate(deadline()).unwrap();
    let error = grants.read_file(actual, 4).await.unwrap_err();
    assert_eq!(error.reason, Reason::Capacity);
    assert_eq!(io_kind(&error), Some(io::ErrorKind::InvalidData));
    assert!(grants.shutdown_io(deadline()).await.unwrap().is_complete());
}
#[tokio::test]
async fn cancelling_the_actual_blocking_file_waiter_retains_its_job_until_confirmed_join() {
    let directory = Directory::new();
    let path = directory.0.join("actual");
    std::fs::write(&path, b"bounded").unwrap();
    let grants = grant(path.clone(), 7);
    let gate = Gate::new();
    let _release = gate.release_guard();
    grants.state.lock().unwrap().files.before_read = Some(gate.hook());
    let _access = grants.activate(deadline()).unwrap();
    let reading = grants.clone();
    let request = tokio::spawn(async move { reading.read_file(path, 7).await });
    gate.entered().await;
    assert!(
        !request.is_finished(),
        "the actual owned blocking job is still suspended"
    );
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    let report = grants
        .shutdown_io(Instant::now() + Duration::from_millis(20))
        .await
        .unwrap();
    assert!(!report.is_complete());
    assert!(report.released_resources.is_empty());
    assert_eq!(grants.state.lock().unwrap().files.records.len(), 1);
    gate.release();
    let report = grants.shutdown_io(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.released_resources.len(), 1);
    let state = grants.state.lock().unwrap();
    assert!(state.files.records.is_empty());
    assert!(
        matches!(&state.files.grants.values().next().unwrap().cached,Cached::Done(Ok(bytes)) if bytes.as_ref()==b"bounded")
    );
}
