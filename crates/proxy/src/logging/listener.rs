use std::io;

use tracing::{debug, warn};
use zero_engine::EngineError;

use super::rate_limit::{environmental_failure_log_decision, FailureLogDecision};

pub(crate) const INBOUND_ACCEPT_ROUTE_STAGE: &str = "inbound_accept_route";

pub(crate) fn log_listener_connection_error(
    stage: &'static str,
    protocol: &'static str,
    inbound_tag: &str,
    remote_addr: &impl std::fmt::Debug,
    error: &EngineError,
) {
    if is_transient_disconnect(error) {
        debug!(inbound_tag, ?remote_addr, protocol, stage, error = %error,
            "connection closed before request completed");
    } else {
        let suppressed_failures = match environmental_failure_log_decision(&error.to_string()) {
            Some(FailureLogDecision::Suppress) => {
                debug!(inbound_tag, ?remote_addr, protocol, stage, error = %error,
                    "repeated environmental connection failure warning suppressed");
                return;
            }
            Some(FailureLogDecision::Warn { suppressed }) => suppressed,
            None => 0,
        };
        warn!(inbound_tag, ?remote_addr, protocol, stage, error = %error,
            suppressed_failures, "connection failed");
    }
}

fn is_transient_disconnect(error: &EngineError) -> bool {
    match error {
        EngineError::Io(source) => matches!(
            source.kind(),
            io::ErrorKind::UnexpectedEof
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::BrokenPipe
                | io::ErrorKind::NotConnected
        ),
        EngineError::Core(zero_core::Error::Io(message)) => {
            message.contains("unexpected EOF") || message.contains("failed to read")
        }
        _ => false,
    }
}

#[cfg(test)]
#[path = "listener/tests.rs"]
mod tests;
