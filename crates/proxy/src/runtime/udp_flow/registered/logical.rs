//! Typed packet connections with one bounded response read per session.
mod datagram;
use super::{state::RegisteredUdpConnection, RegisteredUdpState};
use crate::runtime::udp_flow::{
    managed::{
        managed_tuple_udp_connection_from_flow, ManagedTupleUdpFlowConnection,
        SharedManagedUdpConnection,
    },
    outbound::ManagedUdpFlowRef,
    packet_path::ChainTask,
    result::FlowFailure,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::{
    sync::{broadcast, Mutex as AsyncMutex},
    task::JoinSet,
};
use zero_core::{Address, Session};
use zero_engine::EngineError;

type Packet = (Address, u16, Vec<u8>);
#[derive(Clone)]
pub(crate) struct LogicalConnection(Arc<Connection>);
struct Connection {
    io: ConnectionIo,
    reader: Arc<super::super::response_join::ResponseTask>,
    closed: Arc<AtomicBool>,
}
#[derive(Clone)]
enum ConnectionIo {
    Tuple {
        sender: SharedManagedUdpConnection,
        receiver: Arc<AsyncMutex<broadcast::Receiver<Packet>>>,
    },
    Datagram(Arc<datagram::DatagramConnection>),
}
impl ConnectionIo {
    async fn receive(&self) -> Result<Packet, EngineError> {
        match self {
            Self::Tuple { receiver, .. } => receiver
                .lock()
                .await
                .recv()
                .await
                .map_err(|error| std::io::Error::other(error).into()),
            Self::Datagram(connection) => connection.receive().await,
        }
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.reader.cancel();
    }
}
impl std::fmt::Debug for LogicalConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LogicalConnection")
    }
}
impl LogicalConnection {
    pub(crate) fn observed_remote(&self) -> Option<std::net::SocketAddr> {
        match &self.0.io {
            ConnectionIo::Datagram(connection) => connection.remote,
            ConnectionIo::Tuple { .. } => None,
        }
    }
    pub(crate) fn from_flow<T: ManagedTupleUdpFlowConnection>(flow: T) -> Self {
        let receiver = flow.subscribe_responses();
        Self(Arc::new(Connection {
            io: ConnectionIo::Tuple {
                sender: managed_tuple_udp_connection_from_flow(flow),
                receiver: Arc::new(AsyncMutex::new(receiver)),
            },
            reader: super::super::response_join::ResponseTask::new(),
            closed: Arc::new(AtomicBool::new(false)),
        }))
    }
    pub(crate) fn open_datagram<'a>(
        execution: &'a crate::protocol_registry::TcpExecutionServices,
        target: &'a crate::inventory::DatagramAssociationTarget,
        session: &'a Session,
    ) -> zero_platform_tokio::contract::BoxFuture<'a, Result<Self, EngineError>> {
        // Erase this cold-open future at the runtime boundary, rather than
        // embedding host preparation in every enclosing protocol relay future.
        Box::pin(async move {
            let connection = datagram::DatagramConnection::open(execution, target, session).await?;
            Ok(Self(Arc::new(Connection {
                io: ConnectionIo::Datagram(connection),
                reader: super::super::response_join::ResponseTask::new(),
                closed: Arc::new(AtomicBool::new(false)),
            })))
        })
    }
    pub(crate) fn open_relay<'a>(
        execution: &'a crate::protocol_registry::TcpExecutionServices,
        prepared: crate::inventory::PreparedDatagramRelay,
        session: &'a Session,
    ) -> zero_platform_tokio::contract::BoxFuture<'a, Result<Self, EngineError>> {
        Box::pin(async move {
            let connection =
                datagram::DatagramConnection::open_relay(execution, prepared, session).await?;
            Ok(Self(Arc::new(Connection {
                io: ConnectionIo::Datagram(connection),
                reader: super::super::response_join::ResponseTask::new(),
                closed: Arc::new(AtomicBool::new(false)),
            })))
        })
    }
    pub(crate) async fn send(
        &self,
        session: &Session,
        payload: &[u8],
    ) -> Result<usize, FlowFailure> {
        let result = match &self.0.io {
            ConnectionIo::Tuple { sender, .. } => {
                sender.send(&session.target, session.port, payload).await
            }
            ConnectionIo::Datagram(connection) => {
                let result = connection.send(session, payload).await;
                if result.is_ok() {
                    self.0.closed.store(false, Ordering::Release);
                }
                result
            }
        };
        result.map_err(|error| FlowFailure {
            stage: "udp_logical_send",
            error,
            upstream: None,
        })
    }

    // A completed read is rearmed by runtime polling, independently of sends.
    // The receiver itself survives each task, including unsolicited responses.
    fn poll_response(
        &self,
        tasks: &mut JoinSet<ChainTask>,
        owner: &super::super::response_join::ResponseTasks,
        session_id: u64,
    ) {
        if self.0.closed.load(Ordering::Acquire) {
            return;
        }
        let io = self.0.io.clone();
        let closed = self.0.closed.clone();
        owner.spawn(tasks, &self.0.reader, async move {
            match io.receive().await {
                Ok((target, port, payload)) => Ok((target, port, payload, Some(session_id))),
                Err(error) => {
                    closed.store(true, Ordering::Release);
                    Err(error)
                }
            }
        });
    }
}
impl RegisteredUdpState {
    pub(crate) fn register_logical(
        &mut self,
        session_id: u64,
        connection: LogicalConnection,
    ) -> ManagedUdpFlowRef {
        let id = self.register_connection(
            Some(session_id),
            RegisteredUdpConnection::Logical(connection),
        );
        self.logical_sessions.insert(session_id, id);
        id
    }
    pub(crate) fn poll_logical_responses(
        &self,
        tasks: &mut JoinSet<ChainTask>,
        owner: &super::super::response_join::ResponseTasks,
    ) {
        for (&session_id, id) in &self.logical_sessions {
            if let Some(RegisteredUdpConnection::Logical(connection)) = self.connections.get(id) {
                connection.poll_response(tasks, owner, session_id);
            }
        }
    }
}

#[cfg(test)]
mod tests;
