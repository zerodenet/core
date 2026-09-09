use super::sender::ManagedTupleUdpSender;
use crate::runtime::udp_flow::managed::connection::model::{
    ManagedUdpConnection, SharedManagedUdpConnection,
};
use crate::runtime::udp_flow::managed::connection::response::spawn_tuple_response_bridge;
use crate::runtime::udp_flow::packet_path::ChainTask;
use std::sync::Arc;
use tokio::task::JoinSet;
use zero_core::Address;
use zero_engine::EngineError;

struct ManagedTupleUdpConnection {
    sender: Arc<dyn ManagedTupleUdpSender>,
}

#[async_trait::async_trait]
impl ManagedUdpConnection for ManagedTupleUdpConnection {
    fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
    async fn send(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        self.sender.send(target, port, payload).await
    }

    fn spawn_response_bridge(&self, chain_tasks: &mut JoinSet<ChainTask>, session_id: u64) {
        spawn_tuple_response_bridge(
            chain_tasks,
            self.sender.subscribe_responses(),
            session_id,
            self.sender.closed_message(),
        );
    }
}

pub(super) fn managed_tuple_udp_connection(
    sender: Arc<dyn ManagedTupleUdpSender>,
) -> SharedManagedUdpConnection {
    Arc::new(ManagedTupleUdpConnection { sender })
}
