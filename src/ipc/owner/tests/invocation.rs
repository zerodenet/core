//! Actual raw worker results and cleanup sources are independent of Apply joins.
use super::*;
#[tokio::test]
async fn raw_blocking_original_keeps_actual_id_and_join_error_after_request_record_is_dropped() {
    let observed = zero_runtime::ApplyObservation::new();
    let (id, weak) = observed
        .scope(async {
            let original =
                tokio::task::spawn_blocking(|| panic!("actual original blocking command"));
            let id = original.id();
            let source = Arc::new(original.await.unwrap_err());
            assert_eq!(source.id(), id);
            let weak = Arc::downgrade(&source);
            zero_runtime::ApplyObservation::record_blocking_outcome(id, Some(source)).unwrap();
            (id, weak)
        })
        .await;
    assert!(
        observed.wait_joined().await.is_none(),
        "there was no accepted Apply, not a claim about this raw job or IO"
    );
    assert_eq!(observed.blocking_outcome().unwrap().task_id(), id);
    let returned = super::wait_command_operation(&observed).await.unwrap_err();
    let actual = returned
        .get_ref()
        .unwrap()
        .downcast_ref::<Arc<tokio::task::JoinError>>()
        .unwrap();
    assert!(Arc::ptr_eq(actual, &weak.upgrade().unwrap()) && actual.is_panic());
    drop(observed);
    assert!(
        weak.upgrade().is_some(),
        "actual Work receiver error retains the same source"
    );
    drop(returned);
    assert!(weak.upgrade().is_none());
}
#[tokio::test]
async fn both_concrete_cleanup_phases_keep_exact_sources_without_fabricating_a_task_exit() {
    use zero_runtime::{CommandCleanupKind, CommandInvocationFault};
    let observed = zero_runtime::ApplyObservation::new();
    // Pure source-receiver data: these errors do not assert an IO acquisition,
    // native close, Scope exit or registration ACK.
    let probe = Arc::new(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "same probe cleanup",
    ));
    let dns = Arc::new(io::Error::new(
        io::ErrorKind::TimedOut,
        "same resolution cleanup",
    ));
    observed
        .scope(async {
            let accepted = zero_runtime::ApplyObservation::retain_cleanup_failure(
                CommandCleanupKind::Probe,
                probe.clone(),
            )
            .unwrap();
            assert!(Arc::ptr_eq(&accepted.retained_source().unwrap(), &probe));
            zero_runtime::ApplyObservation::retain_cleanup_failure(
                CommandCleanupKind::Resolution,
                dns.clone(),
            )
            .unwrap();
        })
        .await;
    assert!(observed.wait_joined().await.is_none());
    let returned = super::wait_command_operation(&observed).await.unwrap_err();
    let originals = returned
        .get_ref()
        .unwrap()
        .downcast_ref::<zero_runtime::CommandInvocationFaults>()
        .unwrap();
    assert_eq!(originals.faults().count(), 2);
    assert!(originals.faults().any(|fault|matches!(fault,CommandInvocationFault::Cleanup(CommandCleanupKind::Probe,source) if Arc::ptr_eq(source,&probe))));
    assert!(originals.faults().any(|fault|matches!(fault,CommandInvocationFault::Cleanup(CommandCleanupKind::Resolution,source) if Arc::ptr_eq(source,&dns))));
}
