//! The original Source keeps each logical response in its actual JoinSet.
use super::super::super::connection::{
    ManagedDatagramUdpConnection, SharedManagedDatagramUdpConnection,
};
use super::model::ManagedDatagramFlowConnection;
use crate::runtime::udp_flow::packet_path::ChainTask;
use std::sync::Arc;
use tokio::task::JoinSet;
use zero_core::Address;
use zero_engine::EngineError;
struct Connection<T> {
    connection: T,
}
#[async_trait::async_trait]
impl<T: ManagedDatagramFlowConnection> ManagedDatagramUdpConnection for Connection<T> {
    async fn send_datagram(
        &self,
        chain_tasks: &mut JoinSet<ChainTask>,
        _services: Option<crate::protocol_registry::UdpRuntimeServices>,
        session_id: u64,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        // The same protocol receive driver owns the target FIFO. No second
        // waiter table, broadcast subscriber or response-pump task is created.
        let response = self
            .connection
            .request_datagram(target, port, payload)
            .await?;
        chain_tasks.spawn(async move {
            response
                .await
                .map(|(target, port, payload)| (target, port, payload, Some(session_id)))
        });
        Ok(payload.len())
    }
}
pub(crate) fn managed_datagram_connection_from_flow<T: ManagedDatagramFlowConnection>(
    connection: T,
) -> SharedManagedDatagramUdpConnection {
    Arc::new(Connection { connection })
}
