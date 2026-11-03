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
    pending: Option<(
        OwnedConnection,
        zero_platform_tokio::contract::scope::WeakTaskRegistrar,
    )>,
    attempted: bool,
    rejected: Option<Arc<ChannelError>>,
    task: Option<(TaskId, TaskJoinWitness)>,
}
impl OwnedUdpDispatcher {
    pub(super) fn prepare(
        connection: OwnedConnection,
        tasks: &zero_platform_tokio::contract::scope::WeakTaskRegistrar,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                next: 1,
                ..Default::default()
            })),
            cancel,
            pending: Some((connection, tasks.clone())),
            attempted: false,
            rejected: None,
            task: None,
        }
    }
    pub(super) fn start(&mut self) -> Result<(), ChannelError> {
        if self.task.is_some() {
            return Ok(());
        }
        if self.attempted {
            return Err(self.registration_error());
        }
        self.attempted = true;
        let (connection, tasks) = self
            .pending
            .take()
            .ok_or_else(|| self.registration_error())?;
        let shared = self.state.clone();
        let stop = self.cancel.clone();
        // Only the actual registered Scope owns/joins this worker. No raw
        // tokio task or second registry is created for a pooled connection.
        // The original prepared dispatcher is installed before this call can
        // invoke a registrar waker. An interrupted registration stays Unknown.
        let registration=tasks.spawn_tracked(TaskClass::Auxiliary,async move {
            loop {
                let data=tokio::select! {biased;
                    _=stop.cancelled()=>break,
                    result=connection.read_datagram()=>match result {Ok(data)=>data,Err(source)=>{
                        let retired={let mut shared=shared.lock().unwrap();shared.stopped=true;std::mem::take(&mut shared.sessions)};drop(retired);
                        return if stop.is_cancelled(){Ok(())}else{Err(source)};
                    }},
                };
                let Ok(packet)=crate::udp::parse_udp_datagram(&data) else {continue};
                let sender=shared.lock().unwrap().sessions.get(&packet.session_id()).cloned();
                if let Some(sender)=sender {
                    // Preserve the original per-session loss policy: slow
                    // consumers cannot block siblings or fabricate delivery.
                    let _=sender.try_send(packet);
                }
            }
            let retired={let mut shared=shared.lock().unwrap();shared.stopped=true;std::mem::take(&mut shared.sessions)};drop(retired);
            Ok(())
        });
        match registration {
            Ok(task) => {
                self.task = Some(task);
                Ok(())
            }
            Err(source) => {
                self.rejected = Some(Arc::new(
                    error(Stage::Prepare, Reason::Capacity).with_source(source),
                ));
                Err(self.registration_error())
            }
        }
    }
    fn registration_error(&self) -> ChannelError {
        match &self.rejected {
            Some(source) => error(Stage::Prepare, Reason::Capacity)
                .with_source(OriginalRegistration(source.clone())),
            None => error(Stage::Close, Reason::Internal).with_source(std::io::Error::other(
                "original UDP dispatcher registration is unconfirmed",
            )),
        }
    }
    pub(super) fn witness(&self) -> Result<Option<TaskJoinWitness>, ChannelError> {
        if let Some((_, witness)) = &self.task {
            return Ok(Some(witness.clone()));
        }
        if self.attempted && self.rejected.is_none() {
            return Err(self.registration_error());
        }
        Ok(None)
    }
    pub fn stop(&self) {
        self.cancel.cancel();
    }
    pub async fn joined(&self, end: tokio::time::Instant) -> Result<TaskJoined, ChannelError> {
        self.stop();
        let witness = self
            .witness()?
            .ok_or_else(|| error(Stage::Close, Reason::Revoked))?;
        let joined = tokio::time::timeout_at(end, witness.wait())
            .await
            .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
        if joined.result().is_err() {
            witness
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
        if self.task.is_none() || state.stopped || self.cancel.is_cancelled() {
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
    /// Explicitly retire this original logical registration after its unique
    /// receiver and all transferred channel guards have returned. Native QUIC
    /// and UDP closure remain the pool owner's separate obligations.
    pub fn retire(mut self) -> zero_platform_tokio::contract::lifecycle::CloseReport {
        let retired = self.state.lock().unwrap().sessions.remove(&self.id);
        drop(retired);
        let receive = self.receive.get_mut();
        receive.rx.close();
        while receive.rx.try_recv().is_ok() {}
        zero_platform_tokio::contract::lifecycle::CloseReport {
            released_resources: vec![format!(
                "Hysteria2 original UDP logical registration {}",
                self.id
            )],
            ..Default::default()
        }
    }
    pub fn is_closed(&self) -> bool {
        !self.connection.is_ready()
    }
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
        let retired = self.state.lock().unwrap().sessions.remove(&self.id);
        drop(retired);
    }
}
#[derive(Debug)]
struct OriginalRegistration(Arc<ChannelError>);
impl std::fmt::Display for OriginalRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for OriginalRegistration {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
