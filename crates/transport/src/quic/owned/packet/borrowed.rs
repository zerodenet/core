//! One original loan, the existing packet queues, and its actual Scope driver.
use super::*;
use crate::quic::owned::DriverExitReceiver;
use zero_platform_tokio::{
    contract::scope::{TaskClass, TaskJoinWitness, TaskJoined},
    controlled_carrier::{loan::BorrowedDatagram, CarrierAdoption, CarrierDriver, CarrierUserView},
};

mod observation;
mod retire;
type Input = CarrierDriver<(BorrowedDatagram, CarrierUserView)>;
pub struct BorrowedPacketSegment {
    adoption: CarrierAdoption,
    identity: Arc<continuation::PacketIdentity>,
    input: Option<Input>,
    endpoint_claim: Option<zero_platform_tokio::controlled_carrier::loan::CarrierEndpointClaim>,
    queues: Arc<Mutex<Option<Channels>>>,
    driver: Option<TaskJoinWitness>,
    joined: Option<TaskJoined>,
    final_retirement: Arc<AtomicBool>,
    attempted: bool,
    stop: CancellationToken,
    logical: SocketAddr,
    tasks: TaskRegistrar,
    receive_observation: observation::Probe,
}
/// Returned unique original queues and the same received driver outcome.
/// Native/control/user closure and Scope Host ACK were independently verified
/// by this actual segment. Its report includes the original native/loan report
/// and this segment's same received driver outcome. Error presence does not
/// undo positive closure facts.
pub struct BorrowedPacketReturned {
    queues: PacketQueues,
    joined: Option<TaskJoined>,
    report: zero_platform_tokio::contract::lifecycle::CloseReport,
}
pub struct RejectedBorrowedPacketSegment {
    pub error: io::Error,
    pub io: BorrowedDatagram,
    pub view: CarrierUserView,
    pub queues: PacketQueues,
}
struct ReturnQueues {
    channels: Option<Channels>,
    target: Arc<Mutex<Option<Channels>>>,
}
impl Drop for ReturnQueues {
    fn drop(&mut self) {
        *self.target.lock().unwrap() = self.channels.take();
    }
}
impl BorrowedPacketSegment {
    /// The caller stores this segment in its existing entry before starting it.
    /// The original LoanOwner has already been installed in the receiver slot.
    pub fn prepare(
        io: BorrowedDatagram,
        view: CarrierUserView,
        queues: PacketQueues,
        tasks: TaskRegistrar,
        logical: SocketAddr,
    ) -> Result<Self, Box<RejectedBorrowedPacketSegment>> {
        let adoption = io.adoption();
        if !view.matches(&adoption) || queues.0.is_none() {
            return Err(Box::new(RejectedBorrowedPacketSegment {
                error: io::ErrorKind::InvalidInput.into(),
                io,
                view,
                queues,
            }));
        }
        // A loan exposes protocol datagrams, not the donor's native GSO/ECN
        // capabilities. Reusing a queue must not retain the preceding hop's.
        if let Some(native) = queues.0.as_ref().and_then(|queue| queue.native.as_ref()) {
            let mut caps = native.lock().unwrap();
            caps.current = None;
            caps.maximum_transmit_segments = 1;
        }
        let input = io.into_driver().split(|io| ((), (io, view))).1;
        Ok(Self {
            identity: queues.1.clone(),
            adoption,
            input: Some(input),
            endpoint_claim: None,
            queues: Arc::new(Mutex::new(queues.0)),
            driver: None,
            joined: None,
            final_retirement: Arc::new(AtomicBool::new(false)),
            attempted: false,
            stop: CancellationToken::new(),
            logical,
            tasks,
            receive_observation: observation::new(),
        })
    }
    /// Kernel protocol path: the actual unique loan/view must issue its one
    /// endpoint claim before any worker starts. Never falls back to library.
    pub fn prepare_protocol(
        io: BorrowedDatagram,
        view: CarrierUserView,
        queues: PacketQueues,
        tasks: TaskRegistrar,
        logical: SocketAddr,
    ) -> Result<Self, Box<RejectedBorrowedPacketSegment>> {
        if queues.0.is_none() || !view.matches(&io.adoption()) {
            return Err(Box::new(RejectedBorrowedPacketSegment {
                error: io::ErrorKind::InvalidInput.into(),
                io,
                view,
                queues,
            }));
        }
        let claim = match io.claim_endpoint_users(&view, &tasks) {
            Ok(claim) => claim,
            Err(source) => {
                return Err(Box::new(RejectedBorrowedPacketSegment {
                    error: io::Error::other(source),
                    io,
                    view,
                    queues,
                }))
            }
        };
        let mut segment = Self::prepare(io, view, queues, tasks, logical)?;
        segment.endpoint_claim = Some(claim);
        Ok(segment)
    }
    pub(in crate::quic::owned) fn bind_endpoint_users(
        &mut self,
        socket: &PacketSocket,
        tasks: TaskRegistrar,
        limits: super::super::OwnedEndpointLimits,
    ) -> Result<
        (
            zero_platform_tokio::controlled_carrier::loan::CarrierEndpointUserGraph<
                OwnedDriverOrigin,
                DriverBody,
            >,
            DriverExitReceiver,
        ),
        ChannelError,
    > {
        if !Arc::ptr_eq(&self.identity, &socket.identity)
            || !self
                .tasks
                .scope_identity()
                .same_scope(&tasks.scope_identity())
            || self.attempted
        {
            return Err(error(Reason::StalePlan));
        }
        let claim = self
            .endpoint_claim
            .take()
            .ok_or_else(|| error(Reason::StalePlan))?;
        match claim.bind_users(tasks, limits.drivers, limits.users) {
            Ok(graph) => Ok(graph),
            Err(claim) => {
                self.endpoint_claim = Some(claim);
                Err(error(Reason::StalePlan))
            }
        }
    }
    /// Actual unique IO input must be returned first. The claim was never bound
    /// to hooks and proves no native close or Scope task outcome.
    pub fn return_unstarted_endpoint(&mut self) -> Result<(), ChannelError> {
        if self.endpoint_claim.is_some() && (self.driver.is_some() || self.attempted) {
            return Err(error(Reason::StalePlan));
        }
        drop(self.input.take());
        if let Some(claim) = self.endpoint_claim.take() {
            match claim.return_unstarted() {
                Ok(receipt) => self.adoption.retain_endpoint_users_closed(receipt)?,
                Err(claim) => {
                    self.endpoint_claim = Some(claim);
                    return Err(error(Reason::StalePlan));
                }
            }
        }
        Ok(())
    }
    /// Registers exactly one bidirectional Aux in the owning factory's Scope.
    /// Failed registration returns the queues and leaves original control in
    /// the same receiver slot for the owner's ordinary retirement.
    pub fn start(&mut self) -> Result<(), ChannelError> {
        if self.attempted || self.stop.is_cancelled() || self.tasks.cancellation().is_cancelled() {
            return Err(error(Reason::Revoked));
        }
        let input = self.input.take().ok_or_else(|| error(Reason::Revoked))?;
        if let Some(source) = self
            .joined
            .as_ref()
            .and_then(|joined| joined.result().err())
        {
            return Err(ChannelError::retained(source));
        }
        let channels = self
            .queues
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| error(Reason::Revoked))?;
        self.attempted = true;
        let mut queue = ReturnQueues {
            channels: Some(channels),
            target: self.queues.clone(),
        };
        let stop = self.stop.clone();
        let receive_observation = observation::clone_probe(&self.receive_observation);
        let logical = self.logical;
        let final_retirement = self.final_retirement.clone();
        let (_, witness) = self.adoption.spawn_draining_datagram_driver(
            &self.tasks,
            TaskClass::Auxiliary,
            input,
            move |(io, view)| async move {
                // One original driver's workspace, retained by its configured
                // entry until the same driver actually joins.
                let mut bytes = if queue.channels.as_ref().unwrap().memory.is_none() {
                    vec![0; 65535]
                } else {
                    Vec::new()
                };
                let result = async {
                    let channels = queue.channels.as_mut().unwrap();
                    loop {
                        tokio::select! { biased;
                            _ = stop.cancelled() => {
                                if final_retirement.load(Ordering::Acquire) { channels.outgoing.close(); }
                                // A hop keeps its same queue; snapshot the finite
                                // already accepted prefix so later producers cannot
                                // extend cleanup. Final retirement seals it first.
                                let accepted = channels.outgoing.len();
                                for _ in 0..accepted {
                                    let packet = channels.outgoing.try_recv().map_err(io::Error::other)?;
                                    send_packet(&io, &packet, logical).await?;
                                }
                                return Ok::<_, io::Error>(());
                            },
                            packet = channels.outgoing.recv() => {
                                let Some(packet) = packet else { return Ok(()) };
                                // Keep this actual accepted send future through
                                // Pending; stop cannot discard an unknown submit.
                                send_packet(&io, &packet, logical).await?;
                            },
                            received = async {
                                let scratch = match channels.memory.as_mut() {
                                    Some(memory) => &mut memory.receive.scratch.bytes,
                                    None => &mut bytes,
                                };
                                observation::receive(&io, scratch, &receive_observation).await
                            } => {
                                let size = received?;
                                // Keep the existing bounded UDP queue's full
                                // policy, with no second FIFO.
                                let packet = match channels.memory.as_ref() {
                                    Some(memory) => match memory.receive.copy(&memory.receive.scratch.bytes[..size], logical, None) {
                                        Ok(packet) => Some(packet),
                                        Err(source) if source.kind() == io::ErrorKind::WouldBlock => None,
                                        Err(source) => return Err(source),
                                    },
                                    None => Some(datagram_queue::Packet { bytes: bytes[..size].to_vec(), peer: logical, metadata: None, _credit: None }),
                                };
                                if let Some(packet) = packet { let _ = channels.incoming.try_send(packet); }
                            },
                        }
                    }
                }
                .await;
                drop(io);
                drop(view);
                result.map_err(|source| error(Reason::Io).with_source(source))
            },
        )?;
        self.driver = Some(witness);
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn actual_receive_pending(&self) -> bool {
        self.receive_observation.load(Ordering::Acquire)
    }
    pub fn driver(&self) -> Option<&TaskJoinWitness> {
        self.driver.as_ref()
    }
    pub fn adoption(&self) -> &CarrierAdoption {
        &self.adoption
    }
    pub fn stop(&self) {
        self.stop.cancel();
    }
}

impl Drop for BorrowedPacketSegment {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn send_packet(
    io: &BorrowedDatagram,
    packet: &datagram_queue::Packet,
    logical: SocketAddr,
) -> io::Result<()> {
    if packet.peer != logical {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    // A returned native GSO batch may meet a protocol datagram hop. Keep the
    // same full backing/credit and submit each original datagram in order.
    match packet.metadata.and_then(|metadata| metadata.segment_size) {
        Some(0) => Err(io::ErrorKind::InvalidInput.into()),
        Some(segment) if !packet.bytes.is_empty() => {
            for bytes in packet.bytes.chunks(segment) {
                io.send(bytes).await?;
            }
            Ok(())
        }
        _ => io.send(&packet.bytes).await,
    }
}
