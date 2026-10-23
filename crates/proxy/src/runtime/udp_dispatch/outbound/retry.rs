//! Preserve the standard contract through the temporary legacy error boundary.
use std::{error::Error, sync::Arc};
use zero_engine::EngineError;
use zero_platform_tokio::contract::ChannelError;
pub(super) fn permits_fallback(error: &EngineError) -> bool {
    standard_retry(error, 0).unwrap_or(true)
}
fn standard_retry(error: &(dyn Error + 'static), depth: u8) -> Option<bool> {
    // Unknown/cyclic wrapper chains cannot prove safe replay.
    if depth >= 64 {
        return Some(false);
    }
    if let Some(channel) = error.downcast_ref::<ChannelError>() {
        return Some(channel.retry_permitted());
    }
    if let Some(channel) = error.downcast_ref::<Arc<ChannelError>>() {
        return Some(channel.retry_permitted());
    }
    if let Some(EngineError::Io(io)) = error.downcast_ref::<EngineError>() {
        if let Some(result) = standard_retry(io, depth + 1) {
            return Some(result);
        }
    }
    // io::Error and transparent errors may omit the wrapped value from source().
    // Inspect its actual retained object before continuing the source chain.
    if let Some(io) = error.downcast_ref::<std::io::Error>() {
        if let Some(inner) = io.get_ref() {
            if let Some(result) = standard_retry(inner, depth + 1) {
                return Some(result);
            }
        }
    }
    error
        .source()
        .and_then(|source| standard_retry(source, depth + 1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_platform_tokio::contract::{DeliveryState, Impact, Reason, RetryClass, Stage};
    #[derive(Debug)]
    struct Wrapper(Box<dyn Error + Send + Sync>);
    impl std::fmt::Display for Wrapper {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "retained wrapper")
        }
    }
    impl Error for Wrapper {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(self.0.as_ref())
        }
    }
    #[test]
    fn actual_channel_retry_and_delivery_must_both_allow_replay_across_wrappers() {
        for (delivery, retry, allowed) in [
            (DeliveryState::NotSubmitted, RetryClass::Never, false),
            (DeliveryState::Unknown, RetryClass::BeforeSubmission, false),
            (
                DeliveryState::NotSubmitted,
                RetryClass::BeforeSubmission,
                true,
            ),
            (
                DeliveryState::LocallyAccepted,
                RetryClass::BeforeSubmission,
                false,
            ),
        ] {
            let mut channel = ChannelError::new(Stage::Handshake, Reason::Io, Impact::Session)
                .with_source(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "actual handshake ended before complete reply",
                ));
            channel.delivery = delivery;
            channel.retry = retry;
            let retained = Arc::new(channel);
            let error = EngineError::Io(std::io::Error::other(Wrapper(Box::new(
                std::io::Error::other(retained.clone()),
            ))));
            assert_eq!(permits_fallback(&error), allowed);
            assert_eq!(retained.stage, Stage::Handshake);
            assert!(retained
                .source()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .is_some());
        }
    }
    #[test]
    fn a_legacy_failure_without_standard_retry_metadata_keeps_its_existing_rule() {
        assert!(permits_fallback(&EngineError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "legacy dial failed"
        ))));
    }
}
