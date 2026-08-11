use crate::runtime::udp_flow::sessions::UdpSessionFlows;
use crate::runtime::udp_flow::state::UdpFlowState;
use crate::runtime::udp_ingress::UdpIngressRuntime;
use crate::runtime::udp_socket::DirectUdpSockets;

#[derive(Debug, Clone, Copy)]
pub(crate) struct UdpFlowCancellation {
    pub(super) session_id: u64,
    pub(super) close_association: bool,
}

impl UdpFlowCancellation {
    pub(crate) fn new(session_id: u64, close_association: bool) -> Self {
        Self {
            session_id,
            close_association,
        }
    }
}

/// Protocol-agnostic UDP dispatch state.
///
/// Owns per-session flow bookkeeping plus neutral registered-handler,
/// packet-path, and chain-task state.
/// Created per inbound UDP session/association.
pub(crate) struct UdpDispatch {
    pub(super) runtime: UdpIngressRuntime,
    pub(super) inbound_tag: String,
    pub(super) flows: UdpSessionFlows,
    /// Ephemeral UDP socket for direct outbound (sends to target, receives responses).
    pub(super) direct_socket: DirectUdpSockets,
    /// Managed protocol, packet-path, and chain response state for this UDP session.
    pub(super) flow_state: UdpFlowState,
    pub(super) cancel_tx: tokio::sync::mpsc::UnboundedSender<UdpFlowCancellation>,
    pub(super) cancel_rx: tokio::sync::mpsc::UnboundedReceiver<UdpFlowCancellation>,
}
