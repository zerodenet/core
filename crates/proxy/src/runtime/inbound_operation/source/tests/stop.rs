//! Pure stop classification; these facts make no native-close claim.
use super::*;
#[test]
fn source_stop_only_classifies_revoked_admission_before_submission() {
    let cancel = CancellationToken::new();
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let mut error = ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component);
    error.delivery = DeliveryState::NotSubmitted;
    assert!(!stopped_admission(&error, &cancel, &shutdown));
    stop.send_replace(true);
    assert!(stopped_admission(&error, &cancel, &shutdown));
    stop.send_replace(false);
    cancel.cancel();
    assert!(stopped_admission(&error, &cancel, &shutdown));
    error.stage = Stage::Close;
    assert!(!stopped_admission(&error, &cancel, &shutdown));
    error.stage = Stage::Prepare;
    error.delivery = DeliveryState::Unknown;
    assert!(!stopped_admission(&error, &cancel, &shutdown));
    error.delivery = DeliveryState::NotSubmitted;
    error.reason = Reason::Io;
    assert!(!stopped_admission(&error, &cancel, &shutdown));
}

#[test]
fn source_receive_cancel_is_expected_only_while_the_same_source_is_stopping() {
    let cancel = CancellationToken::new();
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let mut error = ChannelError::new(Stage::Receive, Reason::Cancelled, Impact::Component);
    assert!(!stopped_receive(&error, &cancel, &shutdown));
    stop.send_replace(true);
    assert!(stopped_receive(&error, &cancel, &shutdown));
    assert!(!stopped_admission(&error, &cancel, &shutdown));
    error.reason = Reason::Io;
    assert!(!stopped_receive(&error, &cancel, &shutdown));
    error.reason = Reason::Cancelled;
    error.stage = Stage::Close;
    assert!(!stopped_receive(&error, &cancel, &shutdown));
    error.stage = Stage::Receive;
    error.impact = Impact::Session;
    assert!(!stopped_receive(&error, &cancel, &shutdown));
    error.impact = Impact::Component;
    error.session = Some(zero_core::component::SessionId(1));
    assert!(!stopped_receive(&error, &cancel, &shutdown));
    error.session = None;
    stop.send_replace(false);
    assert!(!stopped_receive(&error, &cancel, &shutdown));
    cancel.cancel();
    assert!(stopped_receive(&error, &cancel, &shutdown));
}
