//! Real accepted-task exits retain every typed source while the parent drains.
use super::*;
#[tokio::test]
async fn every_actual_connection_error_and_panic_survives_the_original_join_drain() {
    let original = Arc::new(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "actual handler output failed",
    ));
    let source = original.clone();
    let mut tasks = JoinSet::new();
    tasks.spawn(async move { Err::<(), _>(io::Error::new(source.kind(), source)) });
    tasks.spawn(async move { std::panic::panic_any("actual accepted connection task panic") });
    let originals = UnixOriginals::default();
    let mut failures = Vec::new();
    while let Some(joined) = tasks.join_next_with_id().await {
        settle_connection(joined, &originals, &mut failures).await;
    }
    assert_eq!(failures.len(), 2);
    let returned = failures
        .iter()
        .find_map(|error| error.get_ref()?.downcast_ref::<Arc<io::Error>>())
        .unwrap();
    assert!(Arc::ptr_eq(returned, &original));
    let panic = failures
        .iter()
        .find_map(|error| error.get_ref()?.downcast_ref::<tokio::task::JoinError>())
        .unwrap();
    assert!(panic.is_panic());
    assert!(tasks.is_empty());
}
