//! One original loan, the existing packet queues, and its actual Scope driver.
use super::*;
use zero_platform_tokio::{
    contract::scope::{TaskClass, TaskJoinWitness},
    controlled_carrier::{loan::BorrowedDatagram, CarrierAdoption, CarrierDriver, CarrierUserView},
};

type Input = CarrierDriver<(BorrowedDatagram, CarrierUserView)>;
pub struct BorrowedPacketSegment {
    adoption: CarrierAdoption,
    input: Option<Input>,
    queues: Arc<Mutex<Option<Channels>>>,
    driver: Option<TaskJoinWitness>,
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
            adoption,
            input: Some(input),
            queues: Arc::new(Mutex::new(queues.0)),
            driver: None,
            attempted: false,
            stop: tasks.cancellation().child_token(),
            logical,
            tasks,
        })
    }
    /// Registers exactly one bidirectional Aux in the owning factory's Scope.
    /// Failed registration returns the queues and leaves original control in
    /// the same receiver slot for the owner's ordinary retirement.
    pub fn start(&mut self) -> Result<(), ChannelError> {
        if self.attempted || self.stop.is_cancelled() {
            return Err(error(Reason::Revoked));
        }
        let input = self.input.take().ok_or_else(|| error(Reason::Revoked))?;
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
        let (_, witness) = self.adoption.spawn_driver(
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
                            _ = stop.cancelled() => return Ok::<_, io::Error>(()),
                            packet = channels.outgoing.recv() => {
                                let Some(packet) = packet else { return Ok(()) };
                                if packet.peer != logical {
                                    return Err(io::ErrorKind::InvalidInput.into());
                                }
                                tokio::select! { biased;
                                    _ = stop.cancelled() => return Ok(()),
                                    sent = io.send(&packet.bytes) => sent?,
                                }
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
        self.adoption.request_retirement();
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
            tokio::time::timeout_at(end, driver.wait_registration_released())
                .await
                .map_err(|_| error(Reason::Timeout))?;
        }
        let report = self.adoption.retire(end).await?;
        if !report.is_complete() {
            return Err(error(Reason::Timeout));
        }
        let channels = self
            .queues
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| error(Reason::Revoked))?;
        Ok(PacketQueues(Some(channels)))
    }
}
impl Drop for BorrowedPacketSegment {
    fn drop(&mut self) {
        self.stop();
    }
}
