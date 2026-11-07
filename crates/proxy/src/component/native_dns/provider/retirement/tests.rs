//! Recognize only the concrete caller's actual deadline, never error fields/chains.
use super::*;
#[derive(Debug)]
struct WorkAndObservation {
    work: Arc<std::io::Error>,
    observation: std::io::Error,
}
impl std::fmt::Display for WorkAndObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.work.fmt(f)
    }
}
impl std::error::Error for WorkAndObservation {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.observation)
    }
}
#[tokio::test]
async fn observer_receipt_is_top_level_only_and_real_timeout_classified_os_source_is_retained() {
    let directory = tempfile::tempdir().unwrap();
    let actual = Arc::new(std::fs::File::open(directory.path().join("missing")).unwrap_err());
    let error = ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component)
        .with_source(actual.clone());
    let received = std::io::Error::other(error);
    assert!(
        !pending_observation(&received),
        "classification fields cannot hide a real OS fault"
    );
    let elapsed =
        tokio::time::timeout_at(tokio::time::Instant::now(), std::future::pending::<()>())
            .await
            .unwrap_err();
    let observation = pending_deadline(elapsed);
    assert!(pending_observation(&observation));
    let both = std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        WorkAndObservation {
            work: actual.clone(),
            observation,
        },
    );
    assert!(
        !pending_observation(&both),
        "a nested observer cannot hide independent work failure"
    );
    let retained = both
        .get_ref()
        .unwrap()
        .downcast_ref::<WorkAndObservation>()
        .unwrap();
    assert!(Arc::ptr_eq(&retained.work, &actual));
}

#[test]
fn retirement_forwarding_retains_actual_os_source_receipt_and_typed_decision_fields() {
    let directory = tempfile::tempdir().unwrap();
    let actual = Arc::new(std::fs::File::open(directory.path().join("missing")).unwrap_err());
    let receipt = zero_platform_tokio::contract::CleanupFailureReceipt::new();
    assert!(receipt.bind_payload(actual.clone()));
    let mut source = ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component)
        .with_source(actual)
        .with_cleanup_failure_receipt(receipt.clone());
    source.delivery = zero_platform_tokio::contract::DeliveryState::Unknown;
    let source = Arc::new(source);
    for _ in 0..64 {
        let observed = super::super::error(std::io::Error::other(source.clone()));
        assert!(Arc::ptr_eq(observed.retained_source().unwrap(), &source));
        assert!(observed
            .cleanup_failure_receipt()
            .unwrap()
            .same_receipt(&receipt));
        assert_eq!(observed.stage, source.stage);
        assert_eq!(observed.reason, source.reason);
        assert_eq!(observed.delivery, source.delivery);
        assert!(
            !receipt.is_acknowledged(),
            "forwarding is not fault acceptance or native close"
        );
    }
    let boxed = super::super::error(std::io::Error::other(ChannelError::retained(
        source.clone(),
    )));
    assert!(Arc::ptr_eq(boxed.retained_source().unwrap(), &source));
    assert!(boxed
        .cleanup_failure_receipt()
        .unwrap()
        .same_receipt(&receipt));
}
