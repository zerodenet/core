#[cfg(feature = "managed-udp-runtime")]
use crate::protocol_registry::UdpRuntimeServices;
#[cfg(feature = "managed-udp-runtime")]
use crate::runtime::udp_flow::managed::{ManagedUdpFlowKind, ManagedUdpFlowResume};
use zero_core::Session;

#[cfg(feature = "managed-udp-runtime")]
pub(super) struct ManagedUdpSend<'a> {
    pub(super) services: Option<UdpRuntimeServices>,
    #[cfg(feature = "managed-datagram-runtime")]
    pub(super) tag: &'a str,
    pub(super) session: &'a Session,
    #[cfg(feature = "managed-stream-runtime")]
    pub(super) carrier: Option<crate::runtime::udp_flow::managed::ManagedRelayStreamCarrier<'a>>,
    #[cfg(feature = "managed-stream-runtime")]
    pub(super) tls_server_name: Option<&'a str>,
    pub(super) server: &'a str,
    pub(super) port: u16,
    pub(super) resume: ManagedUdpFlowResume,
    pub(super) payload: &'a [u8],
    pub(super) kind: ManagedUdpFlowKind,
}

#[cfg(feature = "managed-datagram-runtime")]
pub(crate) struct ManagedDatagramStart<'a, T> {
    pub(crate) services: Option<UdpRuntimeServices>,
    pub(crate) tag: &'a str,
    pub(crate) session: &'a Session,
    pub(crate) server: &'a str,
    pub(crate) port: u16,
    pub(crate) resume: T,
    pub(crate) payload: &'a [u8],
}

#[cfg(feature = "upstream-association-runtime")]
pub(crate) struct UpstreamTrackedStart<'a, T> {
    pub(crate) services: Option<crate::protocol_registry::UdpRuntimeServices>,
    pub(crate) tag: &'a str,
    pub(crate) session: &'a Session,
    pub(crate) server: &'a str,
    pub(crate) port: u16,
    pub(crate) resume: T,
    pub(crate) payload: &'a [u8],
}
