//! The actual carrier Root pays codec/copy work and original queued backings.
use std::{io, sync::Mutex};
use zero_platform_tokio::{
    contract::component::PacketDriverMetadataView,
    controlled_carrier::{
        CarrierBufferBudget, CarrierBufferCredit, CarrierBufferFrameCredit,
        CarrierBufferWorkingLane, CarrierBufferWorkingShape,
    },
};
use zero_traits::packet_metadata::PacketMetadataArc;
#[derive(Clone, Default)]
pub(crate) struct Memory(Option<PacketMetadataArc<Shared>>);
struct Shared {
    book: CarrierBufferBudget,
    metadata: PacketDriverMetadataView,
    transmit: CarrierBufferWorkingLane,
    receive: CarrierBufferWorkingLane,
    control: CarrierBufferWorkingLane,
    scratch: Mutex<Option<CarrierBufferCredit>>,
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
        let control = book.try_working_lane(2 * 65536 + 16)?;
        let shared = Shared {
            book,
            metadata,
            transmit: work.transmit(),
            receive: work.receive(),
            control,
            scratch: Mutex::new(work.take_receive_scratch()),
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
    pub(crate) fn transmit(&self, bytes: usize) -> io::Result<Option<CarrierBufferFrameCredit>> {
        self.0
            .as_ref()
            .map(|value| value.transmit.try_reserve(bytes))
            .transpose()
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
}
impl Drop for Scratch {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.bytes));
    }
}
