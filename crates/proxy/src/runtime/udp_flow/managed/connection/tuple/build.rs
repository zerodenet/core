use super::connection::managed_tuple_udp_connection;
use super::flow::ManagedTupleUdpFlowConnection;
use super::sender::ManagedTupleUdpSender;
use crate::runtime::udp_flow::managed::connection::model::SharedManagedUdpConnection;
use std::sync::Arc;
use zero_core::Address;
use zero_engine::EngineError;

struct ManagedTupleUdpFlowSender<T> {
    connection: T,
}

#[async_trait::async_trait]
impl<T> ManagedTupleUdpSender for ManagedTupleUdpFlowSender<T>
where
    T: ManagedTupleUdpFlowConnection,
{
    async fn send(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        self.connection.send(target, port, payload).await
    }
}

pub(crate) fn managed_tuple_udp_connection_from_flow<T>(connection: T) -> SharedManagedUdpConnection
where
    T: ManagedTupleUdpFlowConnection,
{
    managed_tuple_udp_connection(Arc::new(ManagedTupleUdpFlowSender { connection }))
}
