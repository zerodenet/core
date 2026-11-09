//! The actual carrier Root pays codec/copy work and original queued backings.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{io, sync::Mutex};
use zero_platform_tokio::{
    contract::component::PacketDriverMetadataView,
    controlled_carrier::{
        CarrierBufferBudget, CarrierBufferCredit, CarrierBufferFrameCredit,
        CarrierBufferWorkingLane, CarrierBufferWorkingShape,
    },
};
use zero_traits::packet_metadata::{
    PacketMemoryError, PacketMetadataArc, PacketMetadataCredit, PacketMetadataLayout,
};
mod wait;
pub(super) use wait::Wait;
#[derive(Clone, Default)]
pub(crate) struct Memory(Option<PacketMetadataArc<Shared>>);
struct Shared {
    book: CarrierBufferBudget,
    metadata: PacketDriverMetadataView,
    transmit: CarrierBufferWorkingLane,
    receive: CarrierBufferWorkingLane,
    control: CarrierBufferWorkingLane,
    scratch: Mutex<Option<CarrierBufferCredit>>,
    needed_tx: AtomicUsize,
    ready_tx: Mutex<Option<CarrierBufferFrameCredit>>,
    needed_metadata: Mutex<Option<PacketMetadataLayout>>,
    ready_metadata: Mutex<Option<PacketMetadataCredit>>,
}
pub(crate) struct Scratch {
    pub bytes: Vec<u8>,
    _credit: Option<CarrierBufferCredit>,
}
impl Memory {
    pub(crate) fn prepare(
        book: CarrierBufferBudget,
        metadata: PacketDriverMetadataView,
        tx: usize,
        rx: usize,
    ) -> io::Result<Self> {
        let header = metadata
            .reserve(PacketMetadataArc::<Shared>::layout().map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
        let mut work = book.try_working_set(CarrierBufferWorkingShape {
            receive_scratch_bytes: 65536,
            transmit_working_bytes: tx.max(1),
            receive_working_bytes: rx.max(1),
        })?;
        let control = book.try_working_lane(4 * 65536 + 64)?;
        let shared = Shared {
            book,
            metadata,
            transmit: work.transmit(),
            receive: work.receive(),
            control,
            scratch: Mutex::new(work.take_receive_scratch()),
            needed_tx: AtomicUsize::new(0),
            ready_tx: Mutex::new(None),
            needed_metadata: Mutex::new(None),
            ready_metadata: Mutex::new(None),
        };
        PacketMetadataArc::from_prepaid(shared, header)
            .map(|value| Self(Some(value)))
            .map_err(|(source, _, _)| io::Error::other(source))
    }
    pub(crate) fn metadata(&self) -> Option<&PacketDriverMetadataView> {
        self.0.as_ref().map(|value| &value.metadata)
    }
    pub(crate) fn book(&self) -> Option<&CarrierBufferBudget> {
        self.0.as_ref().map(|value| &value.book)
    }
    pub(crate) fn scratch(&self) -> io::Result<Scratch> {
        let credit = match &self.0 {
            Some(value) => Some(
                value
                    .scratch
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                    .ok_or_else(|| io::Error::other("original mask scratch already transferred"))?,
            ),
            None => None,
        };
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(65536).map_err(io::Error::other)?;
        bytes.resize(65536, 0);
        Ok(Scratch {
            bytes,
            _credit: credit,
        })
    }
    pub(crate) fn extra_scratch(&self) -> io::Result<Scratch> {
        let credit = self
            .book()
            .map(|book| book.try_reserve(65536))
            .transpose()?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(65536).map_err(io::Error::other)?;
        bytes.resize(65536, 0);
        Ok(Scratch {
            bytes,
            _credit: credit,
        })
    }
    pub(crate) fn transmit(&self, bytes: usize) -> io::Result<Option<CarrierBufferFrameCredit>> {
        let Some(actual) = &self.0 else {
            return Ok(None);
        };
        let ready = actual
            .ready_tx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(ready) = ready {
            if ready.bytes() >= bytes {
                actual.needed_tx.store(0, Ordering::Release);
                return Ok(Some(ready));
            }
            drop(ready);
        }
        match actual.transmit.try_reserve(bytes) {
            Ok(credit) => {
                actual.needed_tx.store(0, Ordering::Release);
                Ok(Some(credit))
            }
            Err(source) => {
                if source.kind() == io::ErrorKind::WouldBlock {
                    actual.needed_tx.store(bytes, Ordering::Release);
                }
                Err(source)
            }
        }
    }
    pub(crate) fn reserve_table<T>(
        &self,
        rows: &mut zero_platform_tokio::contract::component::PacketMetadataVec<T>,
        max: usize,
    ) -> io::Result<()> {
        let Some(actual) = &self.0 else {
            return Err(io::ErrorKind::PermissionDenied.into());
        };
        let Some(layout) = rows.growth_layout(max).map_err(metadata_error)? else {
            return Ok(());
        };
        let old = PacketMetadataLayout::array::<T>(rows.capacity()).map_err(metadata_error)?;
        let peak = old
            .checked_add(layout)
            .and_then(|v| {
                v.checked_add(zero_traits::packet_metadata::PacketMetadataCounter::owned_layout())
            })
            .map_err(metadata_error)?;
        let limit = actual.metadata.snapshot().limit;
        if peak.allocations() > limit.allocations() || peak.bytes() > limit.bytes() {
            return Err(io::ErrorKind::OutOfMemory.into());
        }
        let mut offered = actual
            .ready_metadata
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let credit = match offered.as_mut() {
            Some(ready)
                if ready.layout().allocations() >= layout.allocations()
                    && ready.layout().bytes() >= layout.bytes() =>
            {
                Some(ready.split(layout).map_err(metadata_error)?)
            }
            _ => None,
        };
        drop(offered);
        let credit = match credit {
            Some(credit) => credit,
            None => match actual.metadata.reserve(layout) {
                Ok(credit) => credit,
                Err(source) => {
                    if matches!(source, PacketMemoryError::Busy | PacketMemoryError::Paused) {
                        *actual
                            .needed_metadata
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(layout);
                    }
                    return Err(metadata_error(source));
                }
            },
        };
        let retired = rows
            .grow_prepaid(credit, max)
            .map_err(|(source, _)| metadata_error(source))?;
        drop(retired);
        *actual
            .needed_metadata
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        Ok(())
    }
    pub(crate) fn receive(&self, bytes: usize) -> io::Result<Option<CarrierBufferFrameCredit>> {
        self.0
            .as_ref()
            .map(|value| value.receive.try_reserve(bytes))
            .transpose()
    }
    pub(crate) fn control(&self, bytes: usize) -> io::Result<Option<CarrierBufferFrameCredit>> {
        self.0
            .as_ref()
            .map(|value| value.control.try_reserve(bytes))
            .transpose()
    }
    pub(crate) fn copy(
        &self,
        bytes: &[u8],
        peer: std::net::SocketAddr,
    ) -> io::Result<crate::datagram_queue::Packet> {
        let credit = self.receive(bytes.len().max(1))?;
        Ok(crate::datagram_queue::Packet {
            bytes: bytes.to_vec(),
            peer,
            metadata: None,
            _credit: credit.map(Into::into),
        })
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.bytes));
    }
}

fn metadata_error(source: PacketMemoryError) -> io::Error {
    let kind = match source {
        PacketMemoryError::Busy | PacketMemoryError::Paused => io::ErrorKind::WouldBlock,
        PacketMemoryError::Capacity | PacketMemoryError::Overflow => io::ErrorKind::OutOfMemory,
        PacketMemoryError::Stopped => io::ErrorKind::BrokenPipe,
    };
    io::Error::new(kind, source)
}
