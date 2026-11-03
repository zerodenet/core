//! One real registered datagram reader, bounded sessions and one receiver each.
use super::*;
use std::{collections::BTreeMap, sync::Mutex};
use tokio::sync::mpsc;
use zero_core::component::TaskId;
use zero_platform_tokio::contract::{
    scope::{TaskClass, TaskJoinWitness, TaskJoined},
    CancellationToken,
};
#[derive(Default)]
struct State {
    next: u64,
    stopped: bool,
    sessions: BTreeMap<u32, mpsc::Sender<crate::udp::Hysteria2UdpPacket>>,
}
pub struct OwnedUdpDispatcher {
    state: Arc<Mutex<State>>,
    cancel: CancellationToken,
    task: TaskId,
    witness: TaskJoinWitness,
}
impl OwnedUdpDispatcher {
    pub(super) fn new(
        connection: OwnedConnection,
        tasks: &zero_platform_tokio::contract::scope::WeakTaskRegistrar,
    ) -> Result<Self, ChannelError> {
        let state = Arc::new(Mutex::new(State {
            next: 1,
            ..Default::default()
        }));
        let cancel = tasks
            .cancellation()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?
            .child_token();
        let shared = state.clone();
        let stop = cancel.clone();
        // Only the actual registered Scope owns/joins this worker. No raw
        // tokio task or second registry is created for a pooled connection.
        let (task,witness)=tasks.spawn_tracked(TaskClass::Auxiliary,async move {
            loop {
                let data=tokio::select! {biased;
                    _=stop.cancelled()=>break,
                    result=connection.read_datagram()=>match result {Ok(data)=>data,Err(source)=>{
                        let mut shared=shared.lock().unwrap();shared.stopped=true;shared.sessions.clear();
                        return if stop.is_cancelled(){Ok(())}else{Err(source)};
                    }},
                };
                let Ok(packet)=crate::udp::parse_udp_datagram(&data) else {continue};
                if let Some(sender)=shared.lock().unwrap().sessions.get(&packet.session_id()) {
                    // Preserve the original per-session loss policy: slow
                    // consumers cannot block siblings or fabricate delivery.
                    let _=sender.try_send(packet);
                }
            }
            let mut shared=shared.lock().unwrap();
            shared.stopped=true;
            shared.sessions.clear();
            Ok(())
        }).map_err(|source|error(Stage::Prepare,Reason::Capacity).with_source(source))?;
        Ok(Self {
            state,
            cancel,
            task,
            witness,
        })
    }
    pub(super) fn witness(&self) -> TaskJoinWitness {
        self.witness.clone()
    }
    pub fn task_id(&self) -> TaskId {
        self.task
    }
    pub fn stop(&self) {
        self.cancel.cancel();
    }
    pub async fn joined(&self, end: tokio::time::Instant) -> Result<TaskJoined, ChannelError> {
        self.stop();
        let joined = tokio::time::timeout_at(end, self.witness.wait())
            .await
            .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
        if joined.result().is_err() {
            self.witness
                .acknowledge_hosted_failure(&joined)
                .map_err(|source| error(Stage::Close, Reason::Internal).with_source(source))?;
        }
        Ok(joined)
    }
    pub(super) fn open(
        &self,
        connection: Arc<OwnedAuthentication>,
    ) -> Result<OwnedUdpChannel, ChannelError> {
        let mut state = self.state.lock().unwrap();
        if state.stopped || self.cancel.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        if state.sessions.len() >= 4096 || state.next > u32::MAX as u64 {
            return Err(error(Stage::Prepare, Reason::Capacity));
        }
        let id = state.next as u32;
        state.next += 1;
        let (tx, rx) = mpsc::channel(64);
        state.sessions.insert(id, tx);
        Ok(OwnedUdpChannel {
            connection,
            id,
            state: self.state.clone(),
            receive: tokio::sync::Mutex::new(Receive {
                rx,
                reassembly: Default::default(),
            }),
        })
    }
}
impl Drop for OwnedUdpDispatcher {
    fn drop(&mut self) {
        self.stop();
    }
}
struct Receive {
    rx: mpsc::Receiver<crate::udp::Hysteria2UdpPacket>,
    reassembly: crate::udp::Hysteria2UdpReassembler,
}
pub struct OwnedUdpChannel {
    connection: Arc<OwnedAuthentication>,
    id: u32,
    state: Arc<Mutex<State>>,
    receive: tokio::sync::Mutex<Receive>,
}
impl OwnedUdpChannel {
    pub fn send(
        &self,
        target: &zero_core::Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, zero_platform_tokio::contract::SendFailure> {
        use zero_platform_tokio::contract::{DeliveryState, SendFailure};
        let not_submitted = |error| SendFailure::new(error, DeliveryState::NotSubmitted);
        let connection = &self.connection.connection;
        let max = connection
            .max_datagram_size()
            .ok_or_else(|| not_submitted(error(Stage::Send, Reason::Unsupported)))?;
        let fragments = crate::udp::build_udp_fragments(self.id, target, port, payload, max)
            .map_err(|source| {
                not_submitted(error(Stage::Send, Reason::Protocol).with_source(source))
            })?;
        let mut encoded = 0;
        for fragment in fragments {
            let size = fragment.len();
            connection
                .send_datagram(fragment.into())
                .map_err(|mut error| {
                    let delivery = if encoded == 0 {
                        DeliveryState::NotSubmitted
                    } else {
                        DeliveryState::Unknown
                    };
                    error.delivery = delivery;
                    SendFailure::new(error, delivery)
                })?;
            encoded += size;
        }
        Ok(encoded)
    }
    pub async fn receive(&self) -> Result<(zero_core::Address, u16, Vec<u8>), ChannelError> {
        let mut state = self.receive.lock().await;
        loop {
            let packet = state
                .rx
                .recv()
                .await
                .ok_or_else(|| error(Stage::Receive, Reason::Revoked))?;
            if let Ok(Some(packet)) = state.reassembly.push(packet) {
                return Ok(packet.into_datagram_parts());
            }
        }
    }
}
impl Drop for OwnedUdpChannel {
    fn drop(&mut self) {
        self.state.lock().unwrap().sessions.remove(&self.id);
    }
}
