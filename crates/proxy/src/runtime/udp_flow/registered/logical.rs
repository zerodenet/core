//! Typed packet connections with one bounded response read per session.
mod datagram;
#[cfg(test)]
#[path = "logical/tests/legacy.rs"]
mod legacy;
pub(in crate::runtime::udp_flow) mod receiver;
use super::{state::RegisteredUdpConnection, RegisteredUdpState};
#[cfg(test)]
use crate::runtime::udp_flow::{
    managed::{
        managed_tuple_udp_connection_from_flow, ManagedTupleUdpFlowConnection,
        SharedManagedUdpConnection,
    },
    packet_path::ChainTask,
};
use crate::runtime::udp_flow::{outbound::ManagedUdpFlowRef, result::FlowFailure};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
#[cfg(test)]
use tokio::{
    sync::{broadcast, Mutex as AsyncMutex},
    task::JoinSet,
};
use zero_core::{Address, Session};
use zero_engine::EngineError;

#[cfg(test)]
type Packet = (Address, u16, Vec<u8>);
#[derive(Clone)]
pub(crate) struct LogicalConnection(Arc<Connection>);
struct Connection {
    io: ConnectionIo,
    #[cfg(test)]
    reader: Arc<super::super::response_join::ResponseTask>,
    native: Option<zero_traits::packet_metadata::PacketMetadataArc<receiver::NativeReceiver>>,
    closed: Arc<AtomicBool>,
}
#[derive(Clone)]
enum ConnectionIo {
    #[cfg(test)]
    Tuple {
        sender: SharedManagedUdpConnection,
        receiver: Arc<AsyncMutex<broadcast::Receiver<Packet>>>,
    },
    Datagram(Arc<datagram::DatagramConnection>),
}
impl ConnectionIo {
    #[cfg(test)]
    async fn receive(&self) -> Result<Packet, EngineError> {
        match self {
            Self::Tuple { receiver, .. } => receiver
                .lock()
                .await
                .recv()
                .await
                .map_err(|error| std::io::Error::other(error).into()),
            Self::Datagram(connection) => {
                let (target, port, payload) = connection.receive_packet().await?;
                Ok((target, port, payload.to_vec()))
            }
        }
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        #[cfg(test)]
        self.reader.cancel();
        if let Some(receiver) = &self.native {
            receiver.stop();
        }
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
            #[cfg(test)]
            ConnectionIo::Tuple { .. } => None,
        }
    }
    #[cfg(test)]
    pub(crate) fn from_flow<T: ManagedTupleUdpFlowConnection>(flow: T) -> Self {
        let receiver = flow.subscribe_responses();
        Self(Arc::new(Connection {
            io: ConnectionIo::Tuple {
                sender: managed_tuple_udp_connection_from_flow(flow),
                receiver: Arc::new(AsyncMutex::new(receiver)),
            },
            reader: super::super::response_join::ResponseTask::new(),
            native: None,
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
            Self::native_connection(execution, connection, session)
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
            Self::native_connection(execution, connection, session)
        })
    }
    fn native_connection(
        execution: &crate::protocol_registry::TcpExecutionServices,
        io: Arc<datagram::DatagramConnection>,
        session: &Session,
    ) -> Result<Self, EngineError> {
        let closed = Arc::new(AtomicBool::new(false));
        #[cfg(test)]
        if let Some(fixture) = legacy::unscoped_native(execution, &io, &closed) {
            return Ok(fixture);
        }
        let native =
            receiver::NativeReceiver::prepare(execution, io.clone(), closed.clone(), session.id)?;
        Ok(Self(Arc::new(Connection {
            io: ConnectionIo::Datagram(io),
            #[cfg(test)]
            reader: super::super::response_join::ResponseTask::new(),
            native: Some(native),
            closed,
        })))
    }
    pub(in crate::runtime::udp_flow) fn install_response(
        &self,
        responses: &mut super::super::response_join::ResponseSetLease,
    ) -> Result<(), EngineError> {
        match &self.0.native {
            Some(receiver) => responses.install_logical_receiver(receiver),
            #[cfg(test)]
            None => Ok(()),
            #[cfg(not(test))]
            None => {
                Err(std::io::Error::other("standard Datagram lacks its actual RX provider").into())
            }
        }
    }
    pub(in crate::runtime::udp_flow) fn return_failed_open(
        &self,
        responses: &mut super::super::response_join::ResponseSetLease,
    ) {
        if let Some(receiver) = &self.0.native {
            responses.return_failed_logical_open(receiver);
        }
    }
    pub(crate) async fn send(
        &self,
        session: &Session,
        payload: &[u8],
    ) -> Result<usize, FlowFailure> {
        let result = match &self.0.io {
            #[cfg(test)]
            ConnectionIo::Tuple { sender, .. } => {
                sender.send(&session.target, session.port, payload).await
            }
            ConnectionIo::Datagram(connection) => {
                let result = connection.send(session, payload).await;
                if result.is_ok() {
                    self.0.closed.store(false, Ordering::Release);
                    if let Some(receiver) = &self.0.native {
                        receiver.changed();
                    }
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
    #[cfg(test)]
    fn poll_response(
        &self,
        tasks: &mut JoinSet<ChainTask>,
        owner: &super::super::response_join::ResponseTasks,
        session_id: u64,
    ) {
        if self.0.native.is_some() || self.0.closed.load(Ordering::Acquire) {
            return;
        }
        let io = self.0.io.clone();
        let closed = self.0.closed.clone();
        owner.spawn(tasks, &self.0.reader, async move {
            match io.receive().await {
                Ok((target, port, payload)) => Ok((target, port, payload.into(), Some(session_id))),
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
    #[cfg(test)]
    pub(crate) fn has_standalone_fixture_for_test(&self) -> bool {
        self.logical_sessions.values().any(|id|matches!(self.connections.get(id),Some(RegisteredUdpConnection::Logical(connection)) if connection.0.native.is_none()))
    }
    #[cfg(test)]
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
