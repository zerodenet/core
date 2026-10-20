use tokio::task::JoinSet;

use crate::runtime::udp_flow::packet_path::ChainTask;
use crate::runtime::udp_flow::packet_path_chain::PacketPathManager;
use crate::runtime::udp_flow::registered::RegisteredUdpState;

pub(crate) struct UdpFlowState {
    pub(super) registered: RegisteredUdpState,
    pub(super) packet_path: PacketPathManager,
    pub(super) chain_tasks: JoinSet<ChainTask>,
    pub(super) response_tasks: super::super::response_join::ResponseTasks,
}
