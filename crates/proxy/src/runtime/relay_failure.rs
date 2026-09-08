use crate::transport::{failure_origin, TransportFailureOrigin};
use zero_engine::EngineError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RelayFailureAttribution {
    pub close_reason: &'static str,
    pub stage: &'static str,
    pub upstream: bool,
}

pub(crate) fn classify_relay_failure(error: &EngineError) -> RelayFailureAttribution {
    let message = error.to_string().to_ascii_lowercase();
    if message.contains("connection reset by local client") {
        return RelayFailureAttribution {
            close_reason: "client_error",
            stage: "client_transport",
            upstream: false,
        };
    }
    if message.contains("local tun tcp acknowledgement timed out")
        || message.contains("local tun packet transport closed")
    {
        return RelayFailureAttribution {
            close_reason: "tun_error",
            stage: "tun_transport",
            upstream: false,
        };
    }
    match failure_origin(error) {
        Some(TransportFailureOrigin::Client) => {
            return RelayFailureAttribution {
                close_reason: "client_error",
                stage: "client_transport",
                upstream: false,
            }
        }
        Some(TransportFailureOrigin::LocalNetwork) => {
            return RelayFailureAttribution {
                close_reason: "network_error",
                stage: "local_network",
                upstream: false,
            }
        }
        Some(TransportFailureOrigin::NameResolution) => {
            return RelayFailureAttribution {
                close_reason: "dns_error",
                stage: "resolve_upstream",
                upstream: false,
            }
        }
        _ => {}
    }
    RelayFailureAttribution {
        close_reason: "upstream_error",
        stage: "relay",
        upstream: true,
    }
}

#[cfg(test)]
#[path = "relay_failure/tests.rs"]
mod tests;
