//! The root adapter prepays the original queued event and encoded byte storage.
//! Production handoff transfers the same lease into the actual encoder/wire
//! owner; poll_next is never a physical write or native-close receipt.
use super::*;
use prost::Message;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use zero_runtime::configuration::{
    ControlBudget, ControlBudgetLimits, ControlBudgetPermit, ControlCharge,
};

pub(super) fn defaults() -> Arc<ControlBudget> {
    ControlBudget::new(ControlBudgetLimits {
        connections: 256,
        subscriptions: 256,
        queued_event_bytes: 1024 * 1024,
        pipe_workspace_bytes: 2 * 1024 * 1024,
        messages: Default::default(),
    })
}
pub(super) struct QueuedEvent {
    pub(super) event: Result<PbEvent, Status>,
    pub(super) _charge: Option<ControlBudgetPermit>,
    pub(super) encoded: Option<(
        Arc<crate::messages::Record>,
        usize,
        Arc<crate::messages::output::Lease>,
    )>,
}
pub(super) struct Events(pub(super) mpsc::Receiver<QueuedEvent>);
impl futures_core::Stream for Events {
    type Item = Result<PbEvent, Status>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.0.poll_recv(cx) {
            Poll::Ready(Some(event)) => {
                if let Some((original, frame, lease)) = event.encoded {
                    original.enqueue(frame, lease);
                }
                Poll::Ready(Some(event.event))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
fn payload_len(value: &serde_json::Value) -> io::Result<usize> {
    struct Counter(usize);
    impl io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| io::Error::other("gRPC event length overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value).map_err(io::Error::other)?;
    Ok(counter.0)
}
fn varint_len(mut value: usize) -> usize {
    let mut bytes = 1;
    while value >= 128 {
        bytes += 1;
        value >>= 7;
    }
    bytes
}
pub(super) fn frame_len(event: &PbEvent, payload: &serde_json::Value) -> io::Result<usize> {
    let bytes = payload_len(payload)?;
    // `event.payload` is still empty. Field 4 has a one-byte key; include the
    // protobuf length prefix and gRPC's five-byte message header before allocate.
    event
        .encoded_len()
        .checked_add(1 + varint_len(bytes))
        .and_then(|base| base.checked_add(bytes))
        .and_then(|base| base.checked_add(5))
        .ok_or_else(|| io::Error::other("gRPC event frame length overflow"))
}
pub(super) fn reserve(
    owner: &Subscriptions,
    bytes: usize,
    closed: impl Fn() -> bool,
) -> io::Result<Option<ControlBudgetPermit>> {
    loop {
        if owner.stop.load(Ordering::Acquire) || closed() {
            return Ok(None);
        }
        // Keep this original event unencoded while capacity is unavailable,
        // including when one frame exceeds the current cap. A later committed
        // budget may admit it; stop/disconnect still release the real worker.
        match owner.budget.reserve(ControlCharge::QueuedEventBytes(bytes)) {
            Ok(permit) => return Ok(Some(permit)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(25))
            }
            Err(error) => return Err(error),
        }
    }
}

pub(super) fn reserve_message(
    owner: &Subscriptions,
    original: Option<&Arc<crate::messages::Record>>,
    bytes: usize,
    closed: impl Fn() -> bool,
) -> io::Result<
    Option<(
        Option<ControlBudgetPermit>,
        Option<Arc<crate::messages::output::Lease>>,
    )>,
> {
    let Some(original) = original else {
        return reserve(owner, bytes, closed)
            .map(|charge| charge.map(|charge| (Some(charge), None)));
    };
    loop {
        if owner.stop.load(Ordering::Acquire) || closed() {
            return Ok(None);
        }
        // Both actual owners must be prepaid before JSON or Prost allocate.
        // Composite admission is atomic: failure holds no partial allowance
        // and an ordered event never waits for its own duplicate raw charge.
        let attempt = original.encoded(bytes, ControlCharge::QueuedEventBytes(bytes));
        match attempt {
            Ok(encoded) => return Ok(Some((None, Some(encoded)))),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(25))
            }
            Err(error) => return Err(error),
        }
    }
}
