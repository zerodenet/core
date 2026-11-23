//! Same authenticated endpoint, bounded current/previous native originals.
use super::*;
use crate::component::quic::entry::{Segment, State};
use zero_platform_tokio::{
    contract::scope::{TaskClass, TaskJoinWitness, TaskJoined},
    controlled_carrier::CarrierCandidates,
    controlled_datagram::{
        metadata::users::MetadataUsersClosedReceipt,
        metadata_pool::{MetadataPoolOrigin, MetadataPoolPermit},
    },
};
use zero_transport::quic::owned::packet::{PacketHopMemory, PacketReceiveLoan, PacketSegment};

pub(in crate::component::quic) struct Hopping {
    pub actor: Option<TaskJoinWitness>,
    pub joined: Option<TaskJoined>,
    pub initializer: Option<TaskJoinWitness>,
    profile: zero_transport::datagram_hop::Profile,
    candidates: PacketMetadataArc<CarrierCandidates>,
    index: usize,
    logical: std::net::SocketAddr,
    memory: Option<PacketHopMemory>,
    previous: Option<Wire>,
    pending: Option<PacketReceiveLoan>,
    permit: Option<MetadataPoolPermit>,
}
struct Wire {
    segment: PacketSegment,
    origin: MetadataPoolOrigin,
    permit: Option<MetadataPoolPermit>,
    receipt: Option<MetadataUsersClosedReceipt>,
}
impl Hopping {
    pub fn prepare(
        profile: zero_transport::datagram_hop::Profile,
        candidates: PacketMetadataArc<CarrierCandidates>,
        index: usize,
        logical: std::net::SocketAddr,
        generation: &Generation,
    ) -> Result<Self, ChannelError> {
        Ok(Self {
            actor: None,
            joined: None,
            initializer: None,
            profile,
            candidates,
            index,
            logical,
            memory: Some(
                PacketHopMemory::prepare(generation.buffers.clone()).map_err(|source| {
                    error(Stage::Prepare, Reason::Capacity).with_source(source)
                })?,
            ),
            previous: None,
            pending: None,
            permit: None,
        })
    }
    /// The previous loan must first return from the actual current driver.
    /// A successful user receipt alone never releases an original reservation.
    pub async fn close_previous(&mut self, tasks: &TaskRegistrar) -> Result<(), ChannelError> {
        let Some(wire) = self.previous.as_mut() else {
            return Ok(());
        };
        if wire.receipt.is_none() {
            wire.receipt = Some(
                wire.segment
                    .returned_wire_receipt()
                    .ok_or_else(|| error(Stage::Close, Reason::Internal))?,
            );
        }
        let receipt = wire.receipt.as_ref().unwrap();
        if wire
            .origin
            .close_native_wire_original(receipt, tasks)
            .await?
            .is_none()
        {
            return Err(error(Stage::Close, Reason::Timeout));
        }
        for registration in receipt
            .registrations()
            .chain(receipt.related_registrations())
        {
            registration.wait_registration_released().await;
        }
        if let Some(initializer) = wire.origin.initializer_registration() {
            initializer.wait_registration_released().await;
        }
        if let Some(permit) = wire.permit.as_ref() {
            permit.close_original(Some(receipt), tasks).await?;
            wire.origin.registration_observation().wait_original().await;
        }
        // The first original still anchors the complete protocol phase. Its
        // native wire has closed, but its Root charge is deliberately retained.
        self.previous = None;
        Ok(())
    }
    pub fn return_pending(&mut self) {
        drop(self.pending.take());
    }
}
pub(super) async fn spawn(
    generation: PacketMetadataArc<Generation>,
    entry: PacketMetadataArc<Entry>,
) -> Result<(), ChannelError> {
    let mut state = entry.owners.state.lock().await;
    let Some(hop) = state.hopping.as_mut() else {
        return Ok(());
    };
    if hop.actor.is_some() || entry.owners.stop.is_cancelled() {
        return Err(error(Stage::Prepare, Reason::Revoked));
    }
    let (begin, ready) = oneshot::channel();
    let owner = entry.clone();
    let tasks = generation.tasks.clone();
    let (_, witness) = tasks
        .spawn_tracked(TaskClass::Auxiliary, async move {
            if ready.await.is_err() {
                return Ok(());
            }
            let result = run(&generation, &owner).await;
            if let Err(source) = result {
                let source = owner.owners.receive_setup(source);
                owner.retirement.request_retirement();
                return Err(ChannelError::retained(source));
            }
            Ok(())
        })
        .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
    hop.actor = Some(witness);
    let _ = begin.send(());
    Ok(())
}
async fn run(generation: &Generation, entry: &Entry) -> Result<(), ChannelError> {
    loop {
        let interval = entry
            .owners
            .state
            .lock()
            .await
            .hopping
            .as_ref()
            .unwrap()
            .profile
            .next_interval();
        tokio::select! { biased;
            _=entry.owners.stop.cancelled()=>return Ok(()),
            _=generation.cancel.cancelled()=>return Ok(()),
            _=tokio::time::sleep(interval)=>{},
        }
        tokio::select! { biased;
            _=entry.owners.stop.cancelled()=>return Ok(()),
            _=generation.cancel.cancelled()=>return Ok(()),
            result=rotate(generation,entry)=>result?,
        }
    }
}
async fn rotate(generation: &Generation, entry: &Entry) -> Result<(), ChannelError> {
    {
        let mut state = entry.owners.state.lock().await;
        let State {
            segment, hopping, ..
        } = &mut *state;
        let Some(Segment::Native(current)) = segment.as_mut() else {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        };
        let hop = hopping.as_mut().unwrap();
        current.pause_for_hop_original().await?;
        if let Some(memory) = current.return_previous_wire() {
            hop.memory = Some(memory);
        }
        // No third live wire. The obsolete previous original and its actual
        // Host registrations finish before another grant may open native IO.
        hop.close_previous(&generation.tasks).await?;
        let memory = hop
            .memory
            .take()
            .ok_or_else(|| error(Stage::Prepare, Reason::Internal))?;
        let (queues, loan) = match current.handoff_for_hop(memory) {
            Ok(actual) => actual,
            Err(memory) => {
                hop.memory = Some(memory);
                return Err(error(Stage::Prepare, Reason::Internal));
            }
        };
        hop.pending = Some(loan);
        let Some(Segment::Native(segment)) = state.segment.take() else {
            unreachable!()
        };
        let origin = state.native_origin.take().unwrap();
        let hop = state.hopping.as_mut().unwrap();
        hop.previous = Some(Wire {
            segment,
            origin,
            permit: hop.permit.take(),
            receipt: None,
        });
        state.queues = Some(queues);
    }
    let preparation = generation
        .metadata
        .grants()
        .prepare_original(&generation.tasks)?;
    let (begin, ready) = oneshot::channel::<(MetadataPoolPermit, TaskJoinWitness)>();
    // This short initializer, rather than the long-lived rotation actor, is the
    // actual per-wire caller. Its ACK can settle while later hops are active.
    let owners = entry.owners.clone();
    let (candidates, index, logical, port) = {
        let state = owners.state.lock().await;
        let hop = state.hopping.as_ref().unwrap();
        (
            hop.candidates.clone(),
            hop.index,
            hop.logical,
            hop.profile.next_port(),
        )
    };
    let tasks = generation.tasks.clone();
    let initializer_tasks = tasks.clone();
    let mut retained = entry.owners.state.lock().await;
    let (permit, witness) = preparation.spawn_auxiliary(&tasks, async move {
        let (permit, caller) = ready
            .await
            .map_err(|source| error(Stage::Prepare, Reason::Revoked).with_source(source))?;
        let mut state = owners.state.lock().await;
        // Install the exact original before an open may fail or unwind.
        state.native_origin = Some(permit.origin());
        state.hopping.as_mut().unwrap().permit = Some(permit);
        if owners.stop.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        let permit = state.hopping.as_ref().unwrap().permit.as_ref().unwrap();
        let (wire, grant) =
            candidates.open_metadata_with_deadline(index, Some(port), permit, &caller, None)?;
        state.unbound = Some(grant);
        let grant = state.unbound.take().unwrap();
        let queues = state.queues.take().unwrap();
        let mut segment =
            match PacketSegment::prepare(grant, queues, initializer_tasks, wire, logical) {
                Ok(segment) => segment,
                Err(rejected) => {
                    let rejected = *rejected;
                    state.unbound = Some(rejected.grant);
                    state.queues = Some(rejected.queues);
                    return Err(error(Stage::Prepare, Reason::Io).with_source(rejected.error));
                }
            };
        let loan = state.hopping.as_mut().unwrap().pending.take().unwrap();
        if let Err(loan) = segment.attach_previous_wire(loan) {
            state.hopping.as_mut().unwrap().pending = Some(loan);
            state.segment = Some(Segment::Native(segment));
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        state.segment = Some(Segment::Native(segment));
        let Some(Segment::Native(segment)) = state.segment.as_mut() else {
            unreachable!()
        };
        segment.start()
    })?;
    retained.hopping.as_mut().unwrap().initializer = Some(witness.clone());
    if begin.send((permit, witness.clone())).is_err() {
        return Err(error(Stage::Prepare, Reason::Revoked));
    }
    drop(retained);
    let joined = witness.wait().await;
    if let Err(source) = joined.result() {
        return Err(ChannelError::retained(source));
    }
    witness.wait_registration_released().await;
    Ok(())
}
