//! A previous native wire is a real user of its original graph. Its view and
//! permit travel together through the next driver and return before retirement.
use super::*;
use zero_platform_tokio::controlled_datagram::metadata::users::{
    drivers::{Admission, DriverOwner},
    LogicalUserPermit,
};
pub(super) type ReceivePermit = LogicalUserPermit<Admission<OwnedDriverOrigin, DriverBody>>;

pub struct PacketHopMemory(datagram_queue::memory::Receive);
impl PacketHopMemory {
    pub fn prepare(
        book: zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
    ) -> io::Result<Self> {
        datagram_queue::memory::Receive::prepare_unmasked(book).map(Self)
    }
}
pub struct PacketReceiveLoan {
    source: super::super::HostedMetadataSocket,
    peer: SocketAddr,
    scope: zero_platform_tokio::contract::scope::TaskScopeIdentity,
    identity: Arc<continuation::PacketIdentity>,
    memory: PacketHopMemory,
    _user: ReceivePermit,
    receiver: Arc<Mutex<Option<DriverOwner<OwnedDriverOrigin, DriverBody>>>>,
}
#[derive(Default)]
pub(super) struct Wires {
    pub source: Option<super::super::HostedMetadataSocket>,
    pub previous: Option<PacketReceiveLoan>,
}
pub(super) struct Return {
    pub channels: Option<Channels>,
    pub target: Arc<Mutex<Option<Channels>>>,
    pub wires: Wires,
    pub returned_wires: Arc<Mutex<Wires>>,
    pub final_retirement: Arc<AtomicBool>,
}
impl Drop for Return {
    fn drop(&mut self) {
        // This is custody return, never a native close or successful task exit.
        *self.returned_wires.lock().unwrap() = std::mem::take(&mut self.wires);
        let mut target = self.target.lock().unwrap();
        if self.final_retirement.load(Ordering::Acquire) {
            if let Some(channels) = self.channels.as_mut() {
                channels.outgoing.close();
            }
        }
        *target = self.channels.take();
    }
}
pub(super) async fn receive_previous(
    previous: &mut Option<PacketReceiveLoan>,
) -> io::Result<Vec<datagram_queue::Packet>> {
    let Some(previous) = previous.as_mut() else {
        return std::future::pending().await;
    };
    let socket = previous.source.socket();
    // Native callbacks must enter their own original broker, even while the
    // current wire's driver is polling this old receive view.
    let packets = callbacks::Boundary::new(
        Box::pin(datagram_queue::receive_budgeted(
            socket.as_ref(),
            &mut previous.memory.0,
            true,
        )),
        previous.source.callbacks.clone(),
    )
    .await?;
    Ok(packets
        .into_iter()
        .filter(|packet| packet.peer == previous.peer)
        .collect())
}
impl PacketSegment {
    /// Reserve the old receive user before sealing its registration. The
    /// original driver, pending send, source and queues stay installed on cancel.
    pub async fn pause_for_hop_original(&mut self) -> Result<(), ChannelError> {
        if self.hop_user.is_none() {
            self.hop_user = Some(self.graph.users().acquire()?);
        }
        self.stop();
        if let Some(id) = self.driver {
            if self.received.is_none() {
                self.received = Some(self.receiver.receive_original(id).await?);
            }
            self.receiver
                .release_body(self.received.as_ref().unwrap())?;
            if self.received.as_ref().unwrap().joined.result().is_err() {
                return Err(error(Reason::Io));
            }
        }
        Ok(())
    }
    /// Called only after pause joined the current driver. This releases the
    /// obsolete receive view, allowing its own graph/native owner to close.
    pub fn return_previous_wire(&mut self) -> Option<PacketHopMemory> {
        let PacketReceiveLoan {
            source,
            memory,
            _user,
            ..
        } = self.wires.lock().unwrap().previous.take()?;
        drop(source);
        drop(_user);
        Some(memory)
    }
    /// No await separates removing these unique originals from installing them
    /// in the protocol's next retained owner.
    pub fn handoff_for_hop(
        &mut self,
        memory: PacketHopMemory,
    ) -> Result<(PacketQueues, PacketReceiveLoan), PacketHopMemory> {
        if self.received.is_none() || self.hop_user.is_none() {
            return Err(memory);
        }
        let mut wires = self.wires.lock().unwrap();
        let mut queues = self.queues.lock().unwrap();
        if wires.previous.is_some() || wires.source.is_none() || queues.is_none() {
            return Err(memory);
        }
        let loan = PacketReceiveLoan {
            source: wires.source.take().unwrap(),
            peer: self.peer,
            scope: self.tasks.scope_identity(),
            identity: self.identity.clone(),
            memory,
            _user: self.hop_user.take().unwrap(),
            receiver: self.hop_receiver.clone(),
        };
        Ok((PacketQueues(queues.take(), self.identity.clone()), loan))
    }
    pub fn attach_previous_wire(
        &mut self,
        previous: PacketReceiveLoan,
    ) -> Result<(), PacketReceiveLoan> {
        let mut wires = self.wires.lock().unwrap();
        if self.driver.is_some()
            || wires.previous.is_some()
            || !Arc::ptr_eq(&self.identity, &previous.identity)
            || !previous.scope.same_scope(&self.tasks.scope_identity())
        {
            return Err(previous);
        }
        if previous.receiver.lock().unwrap().is_some() {
            return Err(previous);
        }
        *previous.receiver.lock().unwrap() = Some(self.graph.drivers().clone());
        wires.previous = Some(previous);
        Ok(())
    }
    /// Only after the transferred old view returned from the next driver can
    /// the original graph mint its real receipt. This does not close the FD.
    pub fn returned_wire_receipt(&self) -> Option<MetadataUsersClosedReceipt> {
        let receipt = self.graph.completion().try_owned_closed()?;
        let receiver = self.hop_receiver.lock().unwrap();
        match receiver.as_ref() {
            Some(receiver) => receipt
                .with_related_driver_receipt(receiver.owned_registration_completion()?)
                .ok(),
            None => Some(receipt),
        }
    }
    /// If rotation was interrupted after the actual driver returned, revoke
    /// further logical sends so the endpoint can finish its own shutdown.
    pub fn seal_paused_queues(&mut self) {
        if self.stop.is_cancelled() {
            // If the real native send is still pending, Return seals these
            // same queues when that driver actually gives them back.
            let mut queues = self.queues.lock().unwrap();
            self.final_retirement.store(true, Ordering::Release);
            if let Some(channels) = queues.as_mut() {
                channels.outgoing.close();
            }
        }
    }
    pub(super) fn release_wire_views(&mut self) {
        drop(self.source.take());
        *self.wires.lock().unwrap() = Wires::default();
        drop(self.hop_user.take());
    }
}

impl PacketQueues {
    pub fn seal_unserved(&mut self) {
        if let Some(channels) = self.0.as_mut() {
            channels.outgoing.close();
        }
    }
}
