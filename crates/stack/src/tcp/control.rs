use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::packet_output::{ObservedPacket, PacketSender};
use tokio::sync::{mpsc, watch};
use tracing::warn;

const TCP_CONTROL_QUEUE_CAPACITY: usize = 256;

#[derive(Clone)]
pub(super) struct TcpControlPackets {
    sender: mpsc::Sender<ObservedPacket>,
    outbound: PacketSender,
    pending: Arc<AtomicUsize>,
    dropped: Arc<AtomicU64>,
    closed: Arc<AtomicBool>,
    send_gate: Arc<Mutex<()>>,
    failure: Arc<Mutex<Option<Arc<zero_traits::PacketDeriveError>>>>,
    _lifetime: Arc<ControlLifetime>,
    worker: Arc<Mutex<Option<ControlWorker>>>,
    task_spawner: crate::task_owner::TcpTaskSpawner,
}

struct ControlRejected {
    packet: zero_traits::PacketBuffer,
    reason: zero_traits::PacketDropReason,
    closed: Option<&'static str>,
    first_saturation: bool,
}
impl ControlRejected {
    fn closed(packet: zero_traits::PacketBuffer, closed: Option<&'static str>) -> Self {
        Self {
            packet,
            reason: zero_traits::PacketDropReason::QueueClosed,
            closed,
            first_saturation: false,
        }
    }
}

struct ControlLifetime {
    shutdown: watch::Sender<bool>,
}

impl Drop for ControlLifetime {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

struct ControlWorker {
    receiver: mpsc::Receiver<ObservedPacket>,
    outbound: PacketSender,
    pending: Arc<AtomicUsize>,
    dropped: Arc<AtomicU64>,
    closed: Arc<AtomicBool>,
    shutdown: watch::Receiver<bool>,
}

impl TcpControlPackets {
    #[cfg(feature = "packet-memory")]
    pub(super) fn defer_memory_notifications(
        &self,
    ) -> Option<crate::packet_output::memory::PacketSendNotifications> {
        self.outbound.defer_memory_notifications()
    }
    pub(super) fn pending_packets(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }

    pub(super) fn new_with_tasks(
        outbound: PacketSender,
        task_spawner: crate::task_owner::TcpTaskSpawner,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(TCP_CONTROL_QUEUE_CAPACITY);
        let pending = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let closed = Arc::new(AtomicBool::new(false));
        let (shutdown, shutdown_rx) = watch::channel(false);
        Self {
            sender,
            task_spawner,
            outbound: outbound.clone(),
            pending: Arc::clone(&pending),
            dropped: Arc::clone(&dropped),
            closed: Arc::clone(&closed),
            send_gate: Arc::new(Mutex::new(())),
            failure: Arc::new(Mutex::new(None)),
            _lifetime: Arc::new(ControlLifetime { shutdown }),
            worker: Arc::new(Mutex::new(Some(ControlWorker {
                receiver,
                outbound,
                pending,
                dropped,
                closed,
                shutdown: shutdown_rx,
            }))),
        }
    }

    pub(super) fn with_observer(
        mut self,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Self {
        self.outbound = self.outbound.with_observer(observer);
        self
    }

    pub(super) fn ensure_worker(&self) -> bool {
        let worker = self
            .worker
            .lock()
            .expect("TCP control worker lock poisoned")
            .take();
        if let Some(worker) = worker {
            if !self.task_spawner.spawn(run_control_worker(worker)) {
                // The host did not accept this original worker; its receiver
                // is gone, so reject control output instead of spawning around it.
                self.closed.store(true, Ordering::Release);
            }
        }
        !self.closed.load(Ordering::Acquire)
    }

    pub(super) fn try_send_spec(&self, spec: crate::packet::TcpPacketSpec<'_>) -> bool {
        match super::output::build_packet(spec, &self.outbound) {
            Ok(packet) => self.try_send(packet),
            Err(source) => self.reject_storage(source),
        }
    }
    pub(super) fn try_send_cached(&self, packet: &super::output::CachedPacket) -> bool {
        match packet.outgoing() {
            Ok(packet) => self.try_send(packet),
            Err(source) => self.reject_storage(source),
        }
    }
    pub(super) fn failure(&self) -> Option<Arc<zero_traits::PacketDeriveError>> {
        self.failure
            .lock()
            .expect("TCP control storage failure lock")
            .clone()
    }
    pub(super) fn reject_storage(&self, source: zero_traits::PacketDeriveError) -> bool {
        use zero_traits::PacketDeriveError;
        match source {
            PacketDeriveError::Busy | PacketDeriveError::Capacity => {
                self.discard(zero_traits::PacketDropReason::QueueFull);
            }
            source => {
                let mut failure = self
                    .failure
                    .lock()
                    .expect("TCP control storage failure lock");
                failure.get_or_insert_with(|| Arc::new(source));
                drop(failure);
                self.report_closed(
                    "TCP control storage did not preserve its actual transmit owner",
                );
                let _ = self._lifetime.shutdown.send(true);
            }
        }
        false
    }
    pub(super) fn try_send(&self, packet: impl Into<zero_traits::PacketBuffer>) -> bool {
        let packet = packet.into();
        let result = {
            let _gate = self
                .send_gate
                .lock()
                .expect("TCP control send gate poisoned");
            if self.closed.load(Ordering::Acquire) {
                Err(ControlRejected::closed(packet, None))
            } else if self.pending.load(Ordering::Acquire) == 0 {
                match self.outbound.try_send_buffer(packet) {
                    Ok(()) => Ok(()),
                    Err(mpsc::error::TrySendError::Full(packet)) => self.enqueue(packet),
                    Err(mpsc::error::TrySendError::Closed(packet)) => Err(ControlRejected::closed(
                        packet,
                        Some("TCP control packet transport is closed"),
                    )),
                }
            } else {
                self.enqueue(packet)
            }
        };
        match result {
            Ok(()) => true,
            Err(rejected) => {
                // The actual buffer credit may wake another producer. Both it
                // and observer callbacks run after the original ordering lock.
                drop(rejected.packet);
                self.discard(rejected.reason);
                if rejected.first_saturation {
                    warn!(
                        capacity = TCP_CONTROL_QUEUE_CAPACITY,
                        "TCP control packet retry queue is full; coalescing further drop warnings"
                    );
                }
                if let Some(message) = rejected.closed {
                    self.report_closed(message);
                }
                false
            }
        }
    }

    fn enqueue(&self, packet: zero_traits::PacketBuffer) -> Result<(), ControlRejected> {
        self.pending.fetch_add(1, Ordering::AcqRel);
        match self.sender.try_send(ObservedPacket {
            packet,
            observer: self.outbound.observer(),
        }) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(packet)) => {
                self.pending.fetch_sub(1, Ordering::AcqRel);
                Err(ControlRejected {
                    packet: packet.packet,
                    reason: zero_traits::PacketDropReason::QueueFull,
                    closed: None,
                    first_saturation: self.dropped.fetch_add(1, Ordering::AcqRel) == 0,
                })
            }
            Err(mpsc::error::TrySendError::Closed(packet)) => {
                self.pending.fetch_sub(1, Ordering::AcqRel);
                Err(ControlRejected::closed(
                    packet.packet,
                    Some("TCP control packet retry queue is closed"),
                ))
            }
        }
    }

    fn discard(&self, reason: zero_traits::PacketDropReason) {
        if let Some(observer) = self.outbound.observer() {
            observer.dropped_reason(reason);
        }
    }
    fn report_closed(&self, message: &'static str) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            warn!(dropped = self.dropped.load(Ordering::Acquire), "{message}");
        }
    }
}

async fn run_control_worker(mut worker: ControlWorker) {
    loop {
        let packet = tokio::select! {
            biased;
            _ = worker.shutdown.changed() => return,
            packet = worker.receiver.recv() => match packet {
                Some(packet) => packet,
                None => return,
            },
        };
        let mut discard = PendingDiscard(packet.observer.clone());
        let send_result = tokio::select! {
            biased;
            _ = worker.shutdown.changed() => return,
            result = worker.outbound.send_packet(packet) => result,
        };
        if send_result.is_err() {
            worker.receiver.close();
            let first_close = !worker.closed.swap(true, Ordering::AcqRel);
            worker.pending.store(0, Ordering::Release);
            if first_close {
                warn!(
                    dropped = worker.dropped.load(Ordering::Acquire),
                    "TCP control packet transport closed with queued responses"
                );
            }
            return;
        }
        discard.0 = None;
        if worker.pending.fetch_sub(1, Ordering::AcqRel) == 1 {
            let dropped = worker.dropped.swap(0, Ordering::AcqRel);
            if dropped > 0 {
                warn!(
                    dropped,
                    "TCP control packet retry queue recovered after saturation"
                );
            }
        }
    }
}

/// Only final discard counts. Initial queue saturation can be retried successfully.
struct PendingDiscard(Option<Arc<dyn zero_traits::IoObserver>>);
impl Drop for PendingDiscard {
    fn drop(&mut self) {
        if let Some(observer) = &self.0 {
            observer.dropped_reason(zero_traits::PacketDropReason::QueueClosed);
        }
    }
}
impl Drop for ControlWorker {
    fn drop(&mut self) {
        self.receiver.close();
        while let Ok(packet) = self.receiver.try_recv() {
            if let Some(observer) = packet.observer {
                observer.dropped_reason(zero_traits::PacketDropReason::QueueClosed);
            }
        }
    }
}
#[cfg(test)]
#[path = "../../tests/observation/tcp_control.rs"]
mod tests;
