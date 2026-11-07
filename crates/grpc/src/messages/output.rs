//! Credit follows the original encoded Bytes through Hyper/H2 clones and slices.
use super::{registry::Lifetime, Record};
use hyper::body::{Body, Bytes, Frame, SizeHint};
use std::{
    collections::VecDeque,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use zero_runtime::configuration::{ControlBudgetPermit, ControlCharge};

pub(crate) enum Payload<'a> {
    Query(&'a zero_api::QueryResponse),
    Command(&'a zero_api::CommandResponse),
}
impl Payload<'_> {
    fn write(&self, writer: impl io::Write) -> Result<(), serde_json::Error> {
        match self {
            Self::Query(value) => serde_json::to_writer(writer, value),
            Self::Command(value) => serde_json::to_writer(writer, value),
        }
    }
}
pub(crate) struct Lease {
    raw: Mutex<Option<ControlBudgetPermit>>,
    _encoded: ControlBudgetPermit,
    _lifetime: Arc<Lifetime>,
}
pub(super) struct Message {
    remaining: usize,
    lease: Arc<Lease>,
}
#[derive(Default)]
pub(super) struct Outbox(Mutex<VecDeque<Message>>);
struct Counter(usize);
impl io::Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("gRPC response length overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn status(error: impl std::fmt::Display) -> tonic::Status {
    tonic::Status::resource_exhausted(error.to_string())
}
impl Record {
    pub(crate) async fn json(&self, value: Payload<'_>) -> Result<Vec<u8>, tonic::Status> {
        let mut count = Counter(0);
        value
            .write(&mut count)
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        if count.0 > self.policy.response_bytes {
            return Err(status("gRPC response exceeds its admitted message limit"));
        }
        let mut length = count.0;
        let mut prefix = 1usize;
        while length >= 128 {
            length >>= 7;
            prefix += 1;
        }
        let frame = count
            .0
            .checked_add(prefix + 6)
            .ok_or_else(|| status("gRPC response length overflow"))?;
        let lease = loop {
            let changed = self.budget.capacity_changed();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let attempt = self.encoded(frame, ControlCharge::ResponseBytes(count.0));
            match attempt {
                Ok(lease) => break lease,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => changed.await,
                Err(error) => return Err(status(error)),
            }
        };
        let mut json = Vec::new();
        json.try_reserve_exact(count.0).map_err(status)?;
        value
            .write(&mut json)
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        if json.len() != count.0 {
            return Err(tonic::Status::internal(
                "gRPC immutable response changed during serialization",
            ));
        }
        self.enqueue(frame, lease);
        Ok(json)
    }
    pub(crate) fn encoded(&self, frame: usize, raw: ControlCharge) -> io::Result<Arc<Lease>> {
        // BytesMut may retain a large empty tail after emitting a batch. Keep
        // its high-water storage bound with this same original encoder until
        // actual task retirement, independently of emitted frame lifetimes.
        let persistent = frame
            .checked_add(super::CODEC_YIELD_THRESHOLD)
            .and_then(|size| size.checked_mul(2))
            .and_then(|size| size.checked_add(super::CODEC_BUFFER_SIZE))
            .ok_or_else(|| io::Error::other("gRPC encoder workspace overflow"))?;
        // Bound the pinned encoder's fixed workspace and amortized BytesMut
        // growth before Prost can allocate. This is a prepaid storage bound,
        // not an RSS measurement or a change to native H2 buffering.
        let bound = frame
            .checked_mul(2)
            .and_then(|size| size.checked_add(super::CODEC_BUFFER_SIZE))
            .ok_or_else(|| io::Error::other("gRPC encoded storage bound overflow"))?;
        let (raw, encoded) = {
            let mut encoder = self
                .lifetime
                .encoder
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let additional = persistent.saturating_sub(encoder.1);
            let permits = encoder.0.reserve_encoded_response(additional, raw, bound)?;
            encoder.1 = encoder.1.max(persistent);
            permits
        };
        Ok(Arc::new(Lease {
            raw: Mutex::new(Some(raw)),
            _encoded: encoded,
            _lifetime: self.lifetime.clone(),
        }))
    }
    pub(crate) fn enqueue(&self, frame: usize, lease: Arc<Lease>) {
        self.output
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_back(Message {
                remaining: frame,
                lease,
            });
    }
}
struct WireBytes {
    bytes: Bytes,
    _holds: Vec<Arc<Lease>>,
    _lifetime: Arc<Lifetime>,
}
impl AsRef<[u8]> for WireBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
pub(crate) struct Output<B> {
    inner: B,
    original: Arc<Record>,
}
impl<B> Output<B> {
    pub(crate) fn new(inner: B, original: Arc<Record>) -> Self {
        Self { inner, original }
    }
}
impl<B: Body<Data = Bytes, Error = tonic::Status> + Unpin> Body for Output<B> {
    type Data = Bytes;
    type Error = tonic::Status;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        let frame = match std::task::ready!(Pin::new(&mut this.inner).poll_frame(cx)) {
            Some(Ok(frame)) => frame,
            other => return Poll::Ready(other),
        };
        let bytes = match frame.into_data() {
            Ok(bytes) => bytes,
            Err(frame) => return Poll::Ready(Some(Ok(frame))),
        };
        if bytes.is_empty() {
            return Poll::Ready(Some(Ok(Frame::data(bytes))));
        }
        let mut holds = Vec::new();
        let mut consumed = Vec::new();
        let mut remaining = bytes.len();
        {
            let mut queue = this
                .original
                .output
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while remaining != 0 {
                let Some(message) = queue.front_mut() else {
                    return Poll::Ready(Some(Err(tonic::Status::internal(
                        "encoded gRPC bytes have no original message owner",
                    ))));
                };
                let count = message.remaining.min(remaining);
                message.remaining -= count;
                remaining -= count;
                holds.push(message.lease.clone());
                if message.remaining == 0 {
                    consumed.push(queue.pop_front().unwrap());
                }
            }
        }
        // Encoding consumed the original protobuf values. Return their raw
        // storage credit outside the queue lock; encoded Bytes retain theirs.
        for lease in &holds {
            let raw = lease
                .raw
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            drop(raw);
        }
        drop(consumed);
        Poll::Ready(Some(Ok(Frame::data(Bytes::from_owner(WireBytes {
            bytes,
            _holds: holds,
            _lifetime: this.original.lifetime.clone(),
        })))))
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
