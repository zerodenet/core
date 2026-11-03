use std::sync::Arc;

use super::super::super::connection::{
    ManagedDatagramUdpConnection, SharedManagedDatagramUdpConnection,
};
use super::super::response::{
    spawn_datagram_response_bridge, spawn_upstream_response_pump, ManagedDatagramResponse,
    ManagedDatagramResponseWaiters,
};
use super::model::{ManagedDatagramConnection, ManagedDatagramSender};
use crate::runtime::udp_flow::packet_path::ChainTask;
use tokio::task::JoinSet;
use zero_core::Address;
use zero_engine::EngineError;

#[async_trait::async_trait]
impl ManagedDatagramUdpConnection for ManagedDatagramConnection {
    async fn send_datagram(
        &self,
        chain_tasks: &mut JoinSet<ChainTask>,
        _services: Option<crate::protocol_registry::UdpRuntimeServices>,
        session_id: u64,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        let response_rx = self.waiters.register(target, port);
        if let Err(error) = self.sender.send_datagram(target, port, payload).await {
            self.waiters.remove(target, port);
            return Err(error);
        }

        spawn_datagram_response_bridge(chain_tasks, response_rx, session_id, self.closed_message);
        Ok(payload.len())
    }
}

pub(crate) fn managed_datagram_connection(
    sender: Arc<dyn ManagedDatagramSender>,
    response_rx: tokio::sync::broadcast::Receiver<ManagedDatagramResponse>,
    closed_message: &'static str,
) -> SharedManagedDatagramUdpConnection {
    let waiters = ManagedDatagramResponseWaiters::new();
    spawn_upstream_response_pump(response_rx, waiters.clone_handle());
    Arc::new(ManagedDatagramConnection {
        sender,
        waiters,
        closed_message,
    })
}
