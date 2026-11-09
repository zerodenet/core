use super::*;
use std::{future::Future, sync::Mutex};
pub(super) const READ: usize = 32768;
pub(super) const WRITE: usize = 65536;
pub(super) struct Scratch {
    pub(super) bytes: Vec<u8>,
    _credit: CarrierBufferCredit,
}
pub(super) struct Memory {
    pub(super) transmit: CarrierBufferWorkingLane,
    pub(super) receive: CarrierBufferWorkingLane,
    pub(super) queue_limit: usize,
    error: Mutex<Option<PacketMetadataCredit>>,
    // There is one inline driver and at most one pending TX admission future.
    // Their actual boxes are freed before the enclosing Stream drops Shared.
    _future_headers: PacketMetadataCredit,
}
pub(super) struct Prepared {
    pub(super) memory: Memory,
    pub(super) shared_header: PacketMetadataCredit,
    pub(super) stream_header: PacketMetadataCredit,
    pub(super) wake_header: PacketMetadataCredit,
    pub(super) scratch: Scratch,
    pub(super) codec: CarrierBufferCredit,
}
fn driver_layout<F>(_: fn(driver::Input) -> F) -> PacketMetadataLayout
where
    F: Future,
{
    PacketMetadataLayout::boxed::<F>()
}
fn wait_layout<F>(_: fn(&CarrierBufferWorkingLane, usize) -> F) -> PacketMetadataLayout
where
    F: Future,
{
    PacketMetadataLayout::boxed::<F>()
}
impl Prepared {
    pub(super) fn new(
        book: &CarrierBufferBudget,
        metadata: &PacketDriverMetadataView,
        codec_peak: usize,
    ) -> io::Result<Self> {
        let shared = PacketMetadataArc::<queue::Shared>::layout().map_err(io::Error::other)?;
        let stream = PacketMetadataLayout::boxed::<stream::Stream>();
        let error = PacketMetadataArc::<io::Error>::layout().map_err(io::Error::other)?;
        let future = driver_layout(driver::run)
            .checked_add(wait_layout(CarrierBufferWorkingLane::reserve))
            .map_err(io::Error::other)?;
        let layout = shared
            .checked_add(stream)
            .and_then(|v| v.checked_add(error))
            .and_then(|v| v.checked_add(wake::layout()))
            .and_then(|v| v.checked_add(future))
            .map_err(io::Error::other)?;
        let mut credit = metadata.reserve(layout).map_err(io::Error::other)?;
        let shared_header = credit.split(shared).map_err(io::Error::other)?;
        let stream_header = credit.split(stream).map_err(io::Error::other)?;
        let error = credit.split(error).map_err(io::Error::other)?;
        let wake_header = credit.split(wake::layout()).map_err(io::Error::other)?;
        let mut work = book.try_working_set(CarrierBufferWorkingShape {
            receive_scratch_bytes: READ,
            transmit_working_bytes: WRITE,
            receive_working_bytes: READ + 4,
        })?;
        // This bound includes old/new Vec coexistence in the EXISTING encoder
        // and fragment-piece builder, before their first random/state change.
        let codec = book.try_reserve(codec_peak)?;
        let scratch_credit = work.take_receive_scratch().expect("same prepared scratch");
        let scratch = Scratch {
            bytes: vec![0; READ],
            _credit: scratch_credit,
        };
        Ok(Self {
            memory: Memory {
                transmit: work.transmit(),
                receive: work.receive(),
                queue_limit: 16.min(book.limits().max_queue_packets),
                error: Mutex::new(Some(error)),
                _future_headers: credit,
            },
            shared_header,
            stream_header,
            wake_header,
            scratch,
            codec,
        })
    }
}
impl Memory {
    pub(super) fn failure(&self, error: io::Error) -> PacketMetadataArc<io::Error> {
        let credit = self
            .error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("one original inline driver outcome");
        PacketMetadataArc::from_prepaid(error, credit)
            .unwrap_or_else(|_| unreachable!("the exact original failure header was prepaid"))
    }
}
