use crate::runtime::udp_flow::packet_path_chain::PacketPathManager;
use crate::runtime::udp_flow::registered::RegisteredUdpState;
use crate::runtime::udp_flow::response_join::{ResponseSetLease, ResponseSetOwner};

pub(crate) struct UdpFlowState {
    pub(in crate::runtime) registered: RegisteredUdpState,
    pub(super) packet_path: PacketPathManager,
    pub(in crate::runtime) chain_tasks: ResponseSetLease,
    pub(super) response_owner: std::sync::Arc<ResponseSetOwner>,
}
