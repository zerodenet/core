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
