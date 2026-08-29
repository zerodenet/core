use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, watch};
use tracing::warn;

const TCP_CONTROL_QUEUE_CAPACITY: usize = 256;

#[derive(Clone)]
pub(super) struct TcpControlPackets {
    sender: mpsc::Sender<Vec<u8>>,
    outbound: mpsc::Sender<Vec<u8>>,
    pending: Arc<AtomicUsize>,
    dropped: Arc<AtomicU64>,
    closed: Arc<AtomicBool>,
    send_gate: Arc<Mutex<()>>,
    _lifetime: Arc<ControlLifetime>,
    worker: Arc<Mutex<Option<ControlWorker>>>,
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
    receiver: mpsc::Receiver<Vec<u8>>,
    outbound: mpsc::Sender<Vec<u8>>,
    pending: Arc<AtomicUsize>,
    dropped: Arc<AtomicU64>,
    closed: Arc<AtomicBool>,
    shutdown: watch::Receiver<bool>,
}

impl TcpControlPackets {
    pub(super) fn new(outbound: mpsc::Sender<Vec<u8>>) -> Self {
        let (sender, receiver) = mpsc::channel(TCP_CONTROL_QUEUE_CAPACITY);
        let pending = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let closed = Arc::new(AtomicBool::new(false));
        let (shutdown, shutdown_rx) = watch::channel(false);
        Self {
            sender,
            outbound: outbound.clone(),
            pending: Arc::clone(&pending),
            dropped: Arc::clone(&dropped),
            closed: Arc::clone(&closed),
            send_gate: Arc::new(Mutex::new(())),
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

    pub(super) fn ensure_worker(&self) {
        let worker = self
            .worker
            .lock()
            .expect("TCP control worker lock poisoned")
            .take();
        if let Some(worker) = worker {
            tokio::spawn(run_control_worker(worker));
        }
    }

    pub(super) fn try_send(&self, packet: Vec<u8>) -> bool {
        let _gate = self
            .send_gate
            .lock()
            .expect("TCP control send gate poisoned");
        if self.closed.load(Ordering::Acquire) {
            return false;
        }
        if self.pending.load(Ordering::Acquire) == 0 {
            match self.outbound.try_send(packet) {
                Ok(()) => return true,
                Err(mpsc::error::TrySendError::Full(packet)) => {
                    return self.enqueue(packet);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.report_closed("TCP control packet transport is closed");
                    return false;
                }
            }
        }
        self.enqueue(packet)
    }

    fn enqueue(&self, packet: Vec<u8>) -> bool {
        self.pending.fetch_add(1, Ordering::AcqRel);
        match self.sender.try_send(packet) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.pending.fetch_sub(1, Ordering::AcqRel);
                if self.dropped.fetch_add(1, Ordering::AcqRel) == 0 {
                    warn!(
                        capacity = TCP_CONTROL_QUEUE_CAPACITY,
                        "TCP control packet retry queue is full; coalescing further drop warnings"
                    );
                }
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.pending.fetch_sub(1, Ordering::AcqRel);
                self.report_closed("TCP control packet retry queue is closed");
                false
            }
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
        let send_result = tokio::select! {
            biased;
            _ = worker.shutdown.changed() => return,
            result = worker.outbound.send(packet) => result,
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
