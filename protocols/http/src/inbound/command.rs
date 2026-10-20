use super::{HttpForwardRequest, HttpInboundMode, HttpInboundRequest};
use alloc::vec::Vec;
use zero_core::{Network, ProtocolType, Session};

/// Parsed CONNECT carries destination facts before any business ID exists.
#[derive(Debug)]
pub enum HttpInboundCommand {
    Connect(zero_core::request::Destination),
    Forward(HttpForwardRequest),
}
impl HttpInboundCommand {
    pub(super) fn into_legacy(self) -> HttpInboundRequest {
        match self {
            Self::Connect(destination) => HttpInboundRequest {
                session: Session::new(
                    0,
                    destination.address,
                    destination.port,
                    Network::Tcp,
                    ProtocolType::new("http"),
                ),
                mode: HttpInboundMode::Connect,
                replay: Vec::new(),
                forward: None,
            },
            Self::Forward(forward) => HttpInboundRequest {
                session: forward.session.clone(),
                mode: HttpInboundMode::Forward,
                replay: forward.head.clone(),
                forward: Some(forward),
            },
        }
    }
}
