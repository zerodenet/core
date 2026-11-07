use super::*;
use zero_platform_tokio::contract::{ChannelError, DeliveryState, Impact, Reason, Stage};
fn rejected() -> ChannelError {
    let mut error = ChannelError::new(Stage::Send, Reason::Unsupported, Impact::Datagram);
    error.delivery = DeliveryState::NotSubmitted;
    error
}
fn bridge(error: ChannelError) -> EngineError {
    std::io::Error::other(error).into()
}
#[test]
fn only_explicit_local_packet_rejection_keeps_an_established_flow() {
    assert!(packet_rejected_before_submission(&bridge(rejected())));
    let mut unknown = rejected();
    unknown.delivery = DeliveryState::Unknown;
    assert!(!packet_rejected_before_submission(&bridge(unknown)));
    let mut driver = rejected();
    driver.reason = Reason::Io;
    assert!(!packet_rejected_before_submission(&bridge(driver)));
    let mut socket = rejected();
    socket.impact = Impact::Component;
    assert!(!packet_rejected_before_submission(&bridge(socket)));
    let mut receive = rejected();
    receive.stage = Stage::Receive;
    assert!(!packet_rejected_before_submission(&bridge(receive)));
    let untyped = EngineError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "Trojan MUX does not support empty raw UDP packets",
    ));
    assert!(!packet_rejected_before_submission(&untyped));
    let nested = EngineError::Io(std::io::Error::other(std::io::Error::other(rejected())));
    assert!(
        !packet_rejected_before_submission(&nested),
        "no diagnostic chain authority"
    );
}
