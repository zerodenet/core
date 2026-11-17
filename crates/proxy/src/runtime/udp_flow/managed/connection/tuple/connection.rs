use super::sender::ManagedTupleUdpSender;
use crate::runtime::udp_flow::managed::connection::model::{
    ManagedUdpConnection, SharedManagedUdpConnection,
};
use std::sync::Arc;
use zero_core::Address;
use zero_engine::EngineError;

struct ManagedTupleUdpConnection {
    sender: Arc<dyn ManagedTupleUdpSender>,
}

#[async_trait::async_trait]
impl ManagedUdpConnection for ManagedTupleUdpConnection {
    async fn send(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        self.sender.send(target, port, payload).await
    }
}

pub(super) fn managed_tuple_udp_connection(
    sender: Arc<dyn ManagedTupleUdpSender>,
) -> SharedManagedUdpConnection {
    Arc::new(ManagedTupleUdpConnection { sender })
}
