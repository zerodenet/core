use zero_core::Address;

use super::accounting::UdpInboundResponseAccounting;

#[cfg(feature = "upstream-association-runtime")]
pub(crate) struct UdpUpstreamResponseParts {
    pub(crate) target: Address,
    pub(crate) port: u16,
    pub(crate) payload: Vec<u8>,
    pub(crate) accounting: UdpInboundResponseAccounting,
}

pub(crate) struct UdpDirectResponseParts<'payload> {
    pub(crate) target: Address,
    pub(crate) port: u16,
    pub(crate) payload: &'payload [u8],
    pub(crate) accounting: UdpInboundResponseAccounting,
    pub(crate) guard: Option<crate::runtime::udp_socket::DirectUdpResponseGuard>,
}

pub(crate) struct UdpChainResponseParts {
    pub(crate) target: Address,
    pub(crate) port: u16,
    pub(crate) payload: Vec<u8>,
    pub(crate) accounting: UdpInboundResponseAccounting,
}

impl UdpDirectResponseParts<'_> {
    pub(crate) fn is_current(&self) -> bool {
        self.guard.as_ref().is_none_or(|guard| guard.is_current())
    }
}
