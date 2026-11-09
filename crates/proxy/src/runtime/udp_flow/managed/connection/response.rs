use crate::runtime::udp_flow::packet_path::ChainTask;
use tokio::task::JoinSet;
use zero_core::Address;
use zero_engine::EngineError;

pub(crate) fn spawn_response_bridge<T, F>(
    chain_tasks: &mut JoinSet<ChainTask>,
    mut response_rx: tokio::sync::broadcast::Receiver<T>,
    session_id: u64,
    closed_message: &'static str,
    mut into_packet: F,
) where
    T: Clone + Send + 'static,
    F: FnMut(T) -> (Address, u16, Vec<u8>) + Send + 'static,
{
    chain_tasks.spawn(async move {
        let response = response_rx
            .recv()
            .await
            .map_err(|_| EngineError::Io(std::io::Error::other(closed_message)))?;
        let (target, port, payload) = into_packet(response);
        Ok((target, port, payload.into(), Some(session_id)))
    });
}

#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
pub(crate) fn spawn_tuple_response_bridge(
    chain_tasks: &mut JoinSet<ChainTask>,
    response_rx: tokio::sync::broadcast::Receiver<(Address, u16, Vec<u8>)>,
    session_id: u64,
    closed_message: &'static str,
) {
    spawn_response_bridge(
        chain_tasks,
        response_rx,
        session_id,
        closed_message,
        |packet| packet,
    );
}
