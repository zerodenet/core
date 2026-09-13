//! Endpoint-free packet connections with one bounded receive queue per session.
use super::RegisteredUdpState;
use crate::runtime::udp_flow::{
    managed::{
        managed_tuple_udp_connection_from_flow, ManagedTupleUdpFlowConnection,
        ManagedUdpFlowResume, SharedManagedUdpConnection,
    },
    outbound::ManagedUdpFlowRef,
    packet_path::ChainTask,
    result::FlowFailure,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::{
    sync::{broadcast, Mutex as AsyncMutex},
    task::{AbortHandle, JoinSet},
};
use zero_core::{Address, Session};
use zero_engine::EngineError;

type Packet = (Address, u16, Vec<u8>);
#[derive(Clone)]
pub(crate) struct LogicalConnection(Arc<Connection>);
struct Connection {
    sender: SharedManagedUdpConnection,
    receiver: Arc<AsyncMutex<broadcast::Receiver<Packet>>>,
    reader: Mutex<Option<AbortHandle>>,
    closed: Arc<AtomicBool>,
}
impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(reader) = self.reader.get_mut().unwrap().take() {
            reader.abort();
        }
    }
}
impl std::fmt::Debug for LogicalConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LogicalConnection")
    }
}
impl LogicalConnection {
    pub(crate) fn from_flow<T: ManagedTupleUdpFlowConnection>(flow: T) -> Self {
        let receiver = flow.subscribe_responses();
        Self(Arc::new(Connection {
            sender: managed_tuple_udp_connection_from_flow(flow),
            receiver: Arc::new(AsyncMutex::new(receiver)),
            reader: Mutex::new(None),
            closed: Arc::new(AtomicBool::new(false)),
        }))
    }
    pub(crate) async fn send(
        &self,
        session: &Session,
        payload: &[u8],
    ) -> Result<usize, FlowFailure> {
        self.0
            .sender
            .send(&session.target, session.port, payload)
            .await
            .map_err(|error| FlowFailure {
                stage: "udp_logical_send",
                error,
                upstream: None,
            })
    }
    // A completed read is rearmed by runtime polling, independently of sends.
    // The receiver itself survives each task, including unsolicited responses.
    fn poll_response(&self, tasks: &mut JoinSet<ChainTask>, session_id: u64) {
        let mut reader = self.0.reader.lock().unwrap();
        if self.0.closed.load(Ordering::Acquire)
            || reader.as_ref().is_some_and(|task| !task.is_finished())
        {
            return;
        }
        let receiver = self.0.receiver.clone();
        let closed = self.0.closed.clone();
        *reader = Some(tasks.spawn(async move {
            match receiver.lock().await.recv().await {
                Ok((target, port, payload)) => Ok((target, port, payload, Some(session_id))),
                Err(error) => {
                    closed.store(true, Ordering::Release);
                    Err(EngineError::Io(std::io::Error::other(error)))
                }
            }
        }));
    }
}
impl RegisteredUdpState {
    pub(crate) fn register_logical(
        &mut self,
        session_id: u64,
        connection: LogicalConnection,
    ) -> ManagedUdpFlowRef {
        self.release_logical_session(session_id);
        let id = self.register_managed_flow(ManagedUdpFlowResume::new(connection));
        self.logical_sessions.insert(session_id, id);
        id
    }
    pub(crate) fn release_logical_session(&mut self, session_id: u64) {
        if let Some(id) = self.logical_sessions.remove(&session_id) {
            self.managed_resumes.remove(&id);
        }
    }
    pub(crate) fn poll_logical_responses(&self, tasks: &mut JoinSet<ChainTask>) {
        for (&session_id, id) in &self.logical_sessions {
            if let Some(connection) = self
                .managed_resumes
                .get(id)
                .and_then(|resume| resume.as_ref::<LogicalConnection>())
            {
                connection.poll_response(tasks, session_id);
            }
        }
    }
}
