use std::sync::Arc;

use super::super::super::connection::SharedManagedDatagramUdpConnection;
use super::super::response::{ManagedDatagramResponse, ManagedDatagramResponseWaiters};
use super::model::{
    ManagedDatagramConnection, ManagedDatagramFlowConnection, ManagedDatagramSender,
};
use super::sender::managed_datagram_connection;
use zero_core::Address;
use zero_engine::EngineError;

struct ManagedDatagramFlowSender<T> {
    connection: T,
}

#[async_trait::async_trait]
impl<T> ManagedDatagramSender for ManagedDatagramFlowSender<T>
where
    T: ManagedDatagramFlowConnection,
{
    async fn send_datagram(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        self.connection.send_datagram(target, port, payload).await
    }
}

pub(crate) fn managed_datagram_connection_from_flow<T>(
    connection: T,
) -> Result<SharedManagedDatagramUdpConnection, EngineError>
where
    T: ManagedDatagramFlowConnection,
{
    let closed_message = connection.closed_message();
    let waiters = ManagedDatagramResponseWaiters::new();
    let destination = waiters.clone_handle();
    let sink: Arc<dyn Fn(ManagedDatagramResponse) + Send + Sync> =
        Arc::new(move |(target, port, payload)| {
            destination.deliver(target, port, payload);
        });
    if let Some(installed) = connection.install_response_sink(&sink) {
        installed?;
        return Ok(Arc::new(ManagedDatagramConnection {
            sender: Arc::new(ManagedDatagramFlowSender { connection }),
            waiters,
            _response_sink: Some(sink),
            closed_message,
        }));
    }
    let response_rx = connection.subscribe_responses();
    Ok(managed_datagram_connection(
        Arc::new(ManagedDatagramFlowSender { connection }),
        response_rx,
        closed_message,
    ))
}
