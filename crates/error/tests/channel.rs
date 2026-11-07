use std::{error::Error, io};
use zero_error::channel::*;

#[test]
fn structured_decisions_retain_source_without_printing_private_details() {
    let mut error = ChannelError::new(Stage::Handshake, Reason::Protocol, Impact::Session)
        .with_source(io::Error::other("private protocol diagnostic"));
    assert!(error
        .source()
        .unwrap()
        .downcast_ref::<io::Error>()
        .is_some());
    assert!(!error.to_string().contains("private protocol"));
    error.retry = RetryClass::BeforeSubmission;
    let failure = SendFailure::new(error, DeliveryState::Unknown);
    assert_eq!(failure.error.delivery, failure.delivery);
    assert!(!failure.error.retry_permitted());
    assert!(failure.source().unwrap().source().is_some());
}

#[test]
fn retained_observations_keep_the_exact_original_arc_and_all_decision_fields() {
    use std::sync::Arc;
    let missing = std::fs::File::open("/dev/null/zero-retained-original-file").unwrap_err();
    assert!(missing.raw_os_error().is_some());
    let mut original =
        ChannelError::new(Stage::Close, Reason::Io, Impact::Component).with_source(missing);
    original.component = Some(zero_core::component::ComponentId(817));
    original.delivery = DeliveryState::Unknown;
    original.retry = RetryClass::Never;
    let original = Arc::new(original);
    let weak = Arc::downgrade(&original);
    let forwarded = ChannelError::retained(original.clone());
    assert_eq!(forwarded.component, original.component);
    assert_eq!(forwarded.session, original.session);
    assert_eq!(forwarded.stage, original.stage);
    assert_eq!(forwarded.reason, original.reason);
    assert_eq!(forwarded.impact, original.impact);
    assert_eq!(forwarded.delivery, original.delivery);
    assert_eq!(forwarded.retry, original.retry);
    assert!(Arc::ptr_eq(
        forwarded
            .source()
            .unwrap()
            .downcast_ref::<Arc<ChannelError>>()
            .unwrap(),
        &original
    ));
    drop(original);
    assert!(weak.upgrade().is_some());
    assert!(forwarded
        .source()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref::<io::Error>()
        .unwrap()
        .raw_os_error()
        .is_some());
    drop(forwarded);
    assert!(weak.upgrade().is_none());
}
