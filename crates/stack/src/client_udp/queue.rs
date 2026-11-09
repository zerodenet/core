//! Bounded admission. Charges live only while an event is QUEUED, never for
//! application-owned payloads, business usage or protocol state.
use super::{ClientUdpDatagram, ClientUdpEvent, ClientUdpStackError};
use crate::packet::Endpoint;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientUdpReceiveQueueConfig {
    pub capacity: usize,
    pub max_payload_bytes: Option<usize>,
    pub max_stack_payload_bytes: Option<usize>,
}
impl Default for ClientUdpReceiveQueueConfig {
    fn default() -> Self {
        Self {
            capacity: 64,
            max_payload_bytes: None,
            max_stack_payload_bytes: None,
        }
    }
}
impl ClientUdpReceiveQueueConfig {
    pub(super) fn validate(self) -> Result<(), ClientUdpStackError> {
        if !(1..=65_536).contains(&self.capacity)
            || self.max_payload_bytes.is_some_and(|n| n < 65_527)
            || self.max_stack_payload_bytes.is_some_and(|n| n < 65_527)
        {
            return Err(ClientUdpStackError::InvalidReceiveQueue);
        }
        Ok(())
    }
}

pub(super) struct ByteBudget {
    used: AtomicUsize,
    maximum: usize,
}
impl ByteBudget {
    pub(super) fn new(maximum: usize) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicUsize::new(0),
            maximum,
        })
    }
    #[cfg(all(test, feature = "packet-memory"))]
    pub(super) fn test_used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }
    fn acquire(&self, bytes: usize) -> Result<(), ()> {
        self.used
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes).filter(|sum| *sum <= self.maximum)
            })
            .map(|_| ())
            .map_err(|_| ())
    }
    fn release(&self, bytes: usize) {
        self.used.fetch_sub(bytes, Ordering::Relaxed);
    }
}
struct BudgetState {
    socket: Option<ByteBudget>,
    stack: Option<Arc<ByteBudget>>,
}
#[derive(Clone)]
pub(super) struct QueueBudget {
    state: Option<Arc<BudgetState>>,
}
impl QueueBudget {
    pub(super) fn new(socket: Option<usize>, stack: Option<Arc<ByteBudget>>) -> Self {
        Self {
            state: (socket.is_some() || stack.is_some()).then(|| {
                Arc::new(BudgetState {
                    socket: socket.map(|maximum| ByteBudget {
                        used: AtomicUsize::new(0),
                        maximum,
                    }),
                    stack,
                })
            }),
        }
    }
    fn acquire(&self, bytes: usize) -> Result<Charges, ()> {
        let Some(state) = &self.state else {
            return Ok(Charges {
                state: None,
                bytes: 0,
            });
        };
        if let Some(socket) = &state.socket {
            socket.acquire(bytes)?;
        }
        if let Some(stack) = &state.stack {
            if stack.acquire(bytes).is_err() {
                if let Some(socket) = &state.socket {
                    socket.release(bytes);
                }
                return Err(());
            }
        }
        Ok(Charges {
            state: Some(state.clone()),
            bytes,
        })
    }
}
struct Charges {
    state: Option<Arc<BudgetState>>,
    bytes: usize,
}
impl Drop for Charges {
    fn drop(&mut self) {
        if let Some(state) = &self.state {
            if let Some(socket) = &state.socket {
                socket.release(self.bytes);
            }
            if let Some(stack) = &state.stack {
                stack.release(self.bytes);
            }
        }
    }
}
pub(super) struct QueuedEvent {
    event: Event,
    _charges: Charges,
}
enum Event {
    Library(ClientUdpEvent),
    #[cfg(feature = "packet-memory")]
    Packet(super::ClientUdpPacketEvent),
}
impl QueuedEvent {
    pub(super) fn control(event: ClientUdpEvent) -> Self {
        Self {
            event: Event::Library(event),
            _charges: Charges {
                state: None,
                bytes: 0,
            },
        }
    }
    pub(super) fn into_packet_event(self) -> super::ClientUdpPacketEvent {
        match self.event {
            Event::Library(event) => event.into(),
            #[cfg(feature = "packet-memory")]
            Event::Packet(event) => event,
        }
    }
    #[cfg(test)]
    pub(super) fn into_event(self) -> ClientUdpEvent {
        self.into_packet_event().into_library_event()
    }
}

/// Fast path reserves before the single payload copy. On temporary slot
/// pressure, materialize once and retry, keeping the pre-optimization copy/send
/// opportunity. Never wait/spin in the shared device receive loop.
pub(super) fn enqueue_datagram(
    sender: &mpsc::Sender<QueuedEvent>,
    budget: &QueueBudget,
    source: Endpoint,
    bytes: usize,
    copy: impl FnOnce() -> Vec<u8>,
) -> Result<(), mpsc::error::TrySendError<()>> {
    use mpsc::error::TrySendError::{Closed, Full};
    match sender.try_reserve() {
        Ok(permit) => {
            let charges = budget.acquire(bytes).map_err(|_| Full(()))?;
            permit.send(QueuedEvent {
                event: Event::Library(ClientUdpEvent::Datagram(ClientUdpDatagram {
                    source,
                    payload: copy(),
                })),
                _charges: charges,
            });
            Ok(())
        }
        Err(Full(())) => {
            let payload = copy();
            let permit = sender.try_reserve()?;
            let charges = budget.acquire(bytes).map_err(|_| Full(()))?;
            permit.send(QueuedEvent {
                event: Event::Library(ClientUdpEvent::Datagram(ClientUdpDatagram {
                    source,
                    payload,
                })),
                _charges: charges,
            });
            Ok(())
        }
        Err(Closed(())) => Err(Closed(())),
    }
}

#[cfg(test)]
#[path = "../../tests/client_udp/queue.rs"]
mod tests;

#[cfg(feature = "packet-memory")]
pub(super) fn enqueue_packet(
    sender: &mpsc::Sender<QueuedEvent>,
    budget: &QueueBudget,
    source: Endpoint,
    bytes: &[u8],
    memory: &crate::packet_output::PacketSender,
) -> Result<(), mpsc::error::TrySendError<()>> {
    use mpsc::error::TrySendError::{Closed, Full};
    let (permit, payload) = match sender.try_reserve() {
        Ok(permit) => (Some(permit), None),
        Err(Closed(())) => return Err(Closed(())),
        // Preserve the original retry opportunity on transient queue pressure,
        // but this materialization now pays its actual Root before allocation.
        Err(Full(())) => (
            None,
            Some(memory.copy_received_payload(bytes).map_err(|_| Full(()))?),
        ),
    };
    let permit = match permit {
        Some(permit) => permit,
        None => sender.try_reserve()?,
    };
    let charges = budget.acquire(bytes.len()).map_err(|_| Full(()))?;
    let payload = match payload {
        Some(payload) => payload,
        None => memory.copy_received_payload(bytes).map_err(|_| Full(()))?,
    };
    permit.send(QueuedEvent {
        event: Event::Packet(super::ClientUdpPacketEvent::Datagram(
            super::ClientUdpPacketDatagram { source, payload },
        )),
        _charges: charges,
    });
    Ok(())
}

#[cfg(feature = "packet-memory")]
pub(super) fn enqueue_original_packet(
    sender: &mpsc::Sender<QueuedEvent>,
    budget: &QueueBudget,
    source: Endpoint,
    original: zero_traits::PacketBuffer,
    range: std::ops::Range<usize>,
    memory: &crate::packet_output::PacketSender,
) -> Result<(), mpsc::error::TrySendError<()>> {
    use mpsc::error::TrySendError::Full;
    let permit = sender.try_reserve()?;
    let charges = budget.acquire(range.len()).map_err(|_| Full(()))?;
    let payload = memory
        .retain_received_payload(original, range)
        .map_err(|_| Full(()))?;
    permit.send(QueuedEvent {
        event: Event::Packet(super::ClientUdpPacketEvent::Datagram(
            super::ClientUdpPacketDatagram { source, payload },
        )),
        _charges: charges,
    });
    Ok(())
}
