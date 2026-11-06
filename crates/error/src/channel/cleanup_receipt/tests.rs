use super::*;
use std::sync::OnceLock;

#[test]
fn receiver_retains_the_same_fault_record_including_late_secondary_before_acknowledgement() {
    #[derive(Debug)]
    struct Sources {
        first: Arc<ChannelError>,
        secondary: OnceLock<Arc<ChannelError>>,
    }
    impl fmt::Display for Sources {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            fmt::Display::fmt(self.first.as_ref(), f)
        }
    }
    impl Error for Sources {}
    let first = Arc::new(ChannelError::new(
        Stage::Close,
        Reason::Io,
        Impact::Component,
    ));
    let sources = Arc::new(Sources {
        first: first.clone(),
        secondary: OnceLock::new(),
    });
    let receipt = CleanupFailureReceipt::new();
    let foreign = CleanupFailureReceipt::new();
    assert!(receipt.bind_payload(sources.clone()));
    let returned = ChannelError::new(first.stage, first.reason, first.impact)
        .with_source(sources.clone())
        .with_cleanup_failure_receipt(receipt.clone());
    // This persistent row is the receiver, not merely the producer's future.
    let receiver = Arc::new(returned);
    assert!(!receipt.is_acknowledged());
    assert!(!foreign.acknowledge_retained(&receiver));
    assert!(!receipt.acknowledge_retained(&first));
    assert!(receipt.acknowledge_retained(&receiver));
    assert!(receipt.is_acknowledged() && !foreign.is_acknowledged());
    let second = Arc::new(ChannelError::new(
        Stage::Close,
        Reason::Protocol,
        Impact::Session,
    ));
    sources.secondary.set(second.clone()).unwrap();
    drop(sources);
    let same_payload = receiver
        .source()
        .unwrap()
        .downcast_ref::<Arc<Sources>>()
        .unwrap();
    assert!(Arc::ptr_eq(&same_payload.first, &first));
    assert!(Arc::ptr_eq(same_payload.secondary.get().unwrap(), &second));
    let forwarded = ChannelError::retained(receiver);
    assert!(forwarded
        .cleanup_failure_receipt()
        .unwrap()
        .same_receipt(&receipt));
    assert_eq!(forwarded.stage, first.stage);
    assert_eq!(forwarded.reason, first.reason);
}
