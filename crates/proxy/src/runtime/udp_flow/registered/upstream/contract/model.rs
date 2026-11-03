use super::target::DatagramAssociationTarget;
use crate::protocol_registry::UdpRuntimeServices;
use zero_core::Session;

pub(crate) struct UpstreamAssociationSend<'a> {
    pub(crate) services: Option<UdpRuntimeServices>,
    pub(crate) session: &'a Session,
    pub(crate) server: &'a str,
    pub(crate) port: u16,
    pub(crate) target: DatagramAssociationTarget,
    pub(crate) payload: &'a [u8],
}

#[cfg(feature = "upstream-association-runtime")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpstreamAssociationCloseReason {
    Closed,
    IdleTimeout,
    Dropped,
}
