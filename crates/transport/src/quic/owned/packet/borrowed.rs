//! One original loan, the existing packet queues, and its actual Scope driver.
use super::*;
use zero_platform_tokio::{
    contract::scope::{TaskClass, TaskJoinWitness, TaskJoined},
    controlled_carrier::{loan::BorrowedDatagram, CarrierAdoption, CarrierDriver, CarrierUserView},
};

type Input = CarrierDriver<(BorrowedDatagram, CarrierUserView)>;
pub struct BorrowedPacketSegment {
    adoption: CarrierAdoption,
    identity: Arc<continuation::PacketIdentity>,
    input: Option<Input>,
    queues: Arc<Mutex<Option<Channels>>>,
    driver: Option<TaskJoinWitness>,
    joined: Option<TaskJoined>,
    final_retirement: Arc<AtomicBool>,
    attempted: bool,
    stop: CancellationToken,
    logical: SocketAddr,
    tasks: TaskRegistrar,
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
            queues: Arc::new(Mutex::new(queues.0)),
            driver: None,
            joined: None,
            final_retirement: Arc::new(AtomicBool::new(false)),
            attempted: false,
            stop: CancellationToken::new(),
            logical,
            tasks,
        })
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
        let logical = self.logical;
        let final_retirement = self.final_retirement.clone();
        let (_, witness) = self.adoption.spawn_draining_datagram_driver(
            &self.tasks,
            TaskClass::Auxiliary,
            input,
            move |(io, view)| async move {
                // One original driver's workspace, retained by its configured
                // entry until the same driver actually joins.
                let mut bytes = vec![0; 65535];
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
                                    if packet.peer != logical { return Err(io::ErrorKind::InvalidInput.into()); }
                                    io.send(&packet.bytes).await?;
                                }
                                return Ok::<_, io::Error>(());
                            },
                            packet = channels.outgoing.recv() => {
                                let Some(packet) = packet else { return Ok(()) };
                                if packet.peer != logical {
                                    return Err(io::ErrorKind::InvalidInput.into());
                                }
                                // Keep this actual accepted send future through
                                // Pending; stop cannot discard an unknown submit.
                                io.send(&packet.bytes).await?;
                            },
                            received = io.receive(&mut bytes) => {
                                let size = received?;
                                // Keep the existing bounded UDP queue's full
                                // policy, with no second FIFO.
                                let _ = channels.incoming.try_send(datagram_queue::Packet {
                                    bytes: bytes[..size].to_vec(), peer: logical, metadata: None,
                                });
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
    pub fn driver(&self) -> Option<&TaskJoinWitness> {
        self.driver.as_ref()
    }
    pub fn adoption(&self) -> &CarrierAdoption {
        &self.adoption
    }
    pub fn stop(&self) {
        self.stop.cancel();
    }
    /// Request final sealing only after the same endpoint's users/drivers have
    /// returned. It creates no native/Scope receipt and does not retire the loan.
    pub fn stop_final(&self) {
        self.final_retirement.store(true, Ordering::Release);
        self.stop();
    }
    pub async fn retire_final(
        &mut self,
        end: tokio::time::Instant,
    ) -> Result<PacketQueues, ChannelError> {
        self.stop_final();
        self.retire(end).await
    }
    /// The host alone consumes and ACKs the actual Scope exit. Until then this
    /// owner and its original queues stay retained; body return is no proof.
    pub async fn retire(
        &mut self,
        end: tokio::time::Instant,
    ) -> Result<PacketQueues, ChannelError> {
        self.stop();
        drop(self.input.take());
        if let Some(driver) = &self.driver {
            if self.joined.is_none() {
                self.joined = Some(
                    tokio::time::timeout_at(end, driver.wait())
                        .await
                        .map_err(|_| error(Reason::Timeout))?,
                );
            }
            let joined = self.joined.as_ref().unwrap();
            if joined.result().is_err() {
                driver
                    .acknowledge_hosted_failure(joined)
                    .map_err(|source| error(Reason::StalePlan).with_source(source))?;
            }
            tokio::time::timeout_at(end, driver.wait_registration_released())
                .await
                .map_err(|_| error(Reason::Timeout))?;
        }
        let report = self.adoption.retire(end).await?;
        if !report.is_complete() {
            return Err(error(Reason::Timeout));
        }
        if let Some(source) = self
            .joined
            .as_ref()
            .and_then(|joined| joined.result().err())
        {
            return Err(ChannelError::retained(source));
        }
        let mut channels = self
            .queues
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| error(Reason::Revoked))?;
        if self.final_retirement.load(Ordering::Acquire) {
            channels.outgoing.close();
        }
        Ok(PacketQueues(Some(channels), self.identity.clone()))
    }
}
impl Drop for BorrowedPacketSegment {
    fn drop(&mut self) {
        self.stop();
    }
}
