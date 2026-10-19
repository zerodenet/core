use super::super::classify_channel_establishment_failure as classify;
use zero_engine::PassiveRelayOutcome::{Failure, Neutral};
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};

#[test]
fn native_prefix_health_keeps_local_admission_cancellation_and_dns_neutral() {
    for reason in [
        Reason::Cancelled,
        Reason::Revoked,
        Reason::StalePlan,
        Reason::InvalidConfig,
        Reason::Capacity,
        Reason::Internal,
    ] {
        assert_eq!(
            classify(&ChannelError::new(Stage::Dial, reason, Impact::Session)),
            Neutral
        );
    }
    assert_eq!(
        classify(&ChannelError::new(
            Stage::Prepare,
            Reason::Timeout,
            Impact::Component
        )),
        Neutral
    );
    let dns = ChannelError::new(Stage::Dial, Reason::Io, Impact::Session)
        .with_source(std::io::Error::other("failed to resolve proxy node"));
    assert_eq!(classify(&dns), Neutral);
    let local = ChannelError::new(Stage::Dial, Reason::Io, Impact::Session)
        .with_source(std::io::Error::from(std::io::ErrorKind::AddrNotAvailable));
    assert_eq!(classify(&local), Neutral);
}

#[test]
fn native_prefix_health_records_actual_remote_rejection_and_transport_failure() {
    assert_eq!(
        classify(&ChannelError::new(
            Stage::Handshake,
            Reason::Protocol,
            Impact::Session
        )),
        Failure
    );
    let remote = ChannelError::new(Stage::Dial, Reason::Io, Impact::Session)
        .with_source(std::io::Error::from(std::io::ErrorKind::ConnectionRefused));
    assert_eq!(classify(&remote), Failure);
}
