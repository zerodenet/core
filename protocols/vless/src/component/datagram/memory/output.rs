//! The original RX scratch, output copy and storage Box have distinct tickets.
use super::*;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use zero_traits::{PacketBuffer, PacketStorage};
type Acquired = (CarrierBufferFrameCredit, PacketMetadataCredit);
type Waiting = Pin<Box<dyn Future<Output = Result<Acquired, ChannelError>> + Send>>;
pub(in crate::component) struct Output {
    pending: Option<Waiting>,
    first: Option<PacketMetadataCredit>,
    lane: CarrierBufferWorkingLane,
    metadata: PacketDriverMetadataView,
    // This private one-at-a-time Box is actually destroyed before its exact
    // prepaid header can be reused or refunded. No raw future/weak escapes.
    _waiting: PacketMetadataCredit,
}
struct Storage {
    bytes: Vec<u8>,
    _credit: CarrierBufferFrameCredit,
}
impl AsRef<[u8]> for Storage {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsMut<[u8]> for Storage {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
impl PacketStorage for Storage {
    fn allocation_bytes(&self) -> usize {
        self.bytes.capacity()
    }
}
impl Output {
    pub(super) fn prepare(
        lane: CarrierBufferWorkingLane,
        metadata: &PacketDriverMetadataView,
    ) -> Result<Self, ChannelError> {
        let sample = acquire(lane.clone(), metadata.clone(), None, 0);
        let future = boxed_layout(&sample);
        drop(sample);
        let header = PacketMetadataLayout::boxed::<Storage>();
        let mut paid = metadata
            .reserve(future.checked_add(header).map_err(capacity)?)
            .map_err(capacity)?;
        let first = paid.split(header).map_err(capacity)?;
        Ok(Self {
            pending: None,
            first: Some(first),
            lane,
            metadata: metadata.clone(),
            _waiting: paid,
        })
    }
    pub fn poll_copy(
        &mut self,
        cx: &mut Context<'_>,
        payload: &[u8],
        target: &Destination,
    ) -> Poll<Result<(PacketBuffer, Destination), ChannelError>> {
        let domain = match &target.address {
            zero_core::Address::Domain(value) => value.len(),
            _ => 0,
        };
        if self.pending.is_none() {
            self.pending = Some(Box::pin(acquire(
                self.lane.clone(),
                self.metadata.clone(),
                self.first.take(),
                payload.len() + domain,
            )));
        }
        let acquired = std::task::ready!(self.pending.as_mut().unwrap().as_mut().poll(cx));
        // The actual previous waiting Box is deallocated before any next one.
        self.pending.take();
        let (credit, header) = acquired?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(payload.len()).map_err(capacity)?;
        bytes.extend_from_slice(payload);
        let address = target.clone();
        // Installed Kernel consumers carry this entire Datagram until address
        // projection/drop; the payload retains its same backing liability.
        let packet = PacketBuffer::from_prepaid_owner(
            Storage {
                bytes,
                _credit: credit,
            },
            header,
        )
        .map_err(|(source, _, _)| capacity(source))?;
        Poll::Ready(Ok((packet, address)))
    }
}
async fn acquire(
    lane: CarrierBufferWorkingLane,
    metadata: PacketDriverMetadataView,
    first: Option<PacketMetadataCredit>,
    bytes: usize,
) -> Result<Acquired, ChannelError> {
    let header = match first {
        Some(header) => header,
        None => metadata
            .reserve_owned(PacketMetadataLayout::boxed::<Storage>())
            .await
            .map_err(capacity)?,
    };
    let credit = lane.reserve(bytes).await.map_err(capacity)?;
    Ok((credit, header))
}
