//! Concrete browser backing over the existing Host carrier byte book.
use std::{future::Future, io, pin::Pin};
use zero_platform_tokio::{
    contract::component::PacketDriverMetadataView,
    controlled_carrier::{
        CarrierBufferBudget, CarrierBufferCredit, CarrierBufferWorkingLane,
        CarrierBufferWorkingShape,
    },
};
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataCredit, PacketMetadataLayout};

#[derive(Clone)]
pub(super) struct Factory {
    pub(super) book: CarrierBufferBudget,
    pub(super) metadata: PacketDriverMetadataView,
}
pub(super) struct Buffers {
    pub(super) receive: CarrierBufferWorkingLane,
    pub(super) transmit: CarrierBufferWorkingLane,
    pub(super) factory: Factory,
    growth: tokio::sync::Mutex<()>,
    _scratch: Option<CarrierBufferCredit>,
    cancel_header: std::sync::Mutex<Option<PacketMetadataCredit>>,
}
#[derive(Clone, Default)]
pub(super) enum Memory {
    #[default]
    Library,
    Hosted(PacketMetadataArc<Buffers>),
}
impl Factory {
    pub(super) async fn connection(
        &self,
        header: PacketMetadataCredit,
        cancel_header: PacketMetadataCredit,
    ) -> io::Result<Memory> {
        // Pinned codec4096-byte chunks plus unchanged16KiB HTTP prefix. The
        // complete concrete set is admitted before any slot allocation/accept.
        let mut work = self
            .book
            .working_set(CarrierBufferWorkingShape {
                receive_scratch_bytes: 16 * 1024,
                transmit_working_bytes: 16 * 1024,
                receive_working_bytes: 64 * 1024,
            })
            .await?;
        let value = Buffers {
            receive: work.receive(),
            transmit: work.transmit(),
            factory: self.clone(),
            growth: tokio::sync::Mutex::new(()),
            _scratch: work.take_receive_scratch(),
            cancel_header: std::sync::Mutex::new(Some(cancel_header)),
        };
        let owned = PacketMetadataArc::from_prepaid(value, header)
            .map_err(|(source, _, _)| io::Error::other(source))?;
        Ok(Memory::Hosted(owned))
    }
    pub(super) async fn prepare(&self, bytes: usize) -> io::Result<CarrierBufferCredit> {
        self.book.reserve(bytes).await
    }
}

/// The sole inline pending admission of one real socket poller. Its exact
/// concrete future header is prepaid BEFORE boxing; the future is freed first.
pub(super) struct Wait {
    pub(super) future: Pin<Box<dyn Future<Output = io::Result<()>> + Send>>,
    _header: PacketMetadataCredit,
}
impl Wait {
    pub(super) fn new<F>(future: F, metadata: &PacketDriverMetadataView) -> io::Result<Self>
    where
        F: Future<Output = io::Result<()>> + Send + 'static,
    {
        let header = metadata
            .reserve(PacketMetadataLayout::boxed::<F>())
            .map_err(io::Error::other)?;
        Ok(Self {
            future: Box::pin(future),
            _header: header,
        })
    }
}
impl Memory {
    pub(super) fn cancellation(
        &self,
        cancel: tokio_util::sync::CancellationToken,
    ) -> io::Result<(
        Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>,
        Option<PacketMetadataCredit>,
    )> {
        let header = match self {
            Self::Library => None,
            Self::Hosted(actual) => {
                let header = actual
                    .cancel_header
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                Some(header.ok_or(io::ErrorKind::InvalidInput)?)
            }
        };
        Ok((Box::pin(cancel.cancelled_owned()), header))
    }
    fn growth(&self, bound: usize, receiving: bool) -> io::Result<Option<Wait>> {
        let Self::Hosted(actual) = self else {
            return Ok(None);
        };
        let lane = if receiving {
            &actual.receive
        } else {
            &actual.transmit
        };
        if bound <= lane.bound() {
            return Ok(None);
        }
        let retained = actual.clone();
        let future = async move {
            // A single concrete peer can grow its two backing lanes. Serialize
            // only those pure admissions so neither waits forever on its own
            // concurrently enlarged other lane. No shared resource scheduler.
            let _serial = retained.growth.lock().await;
            let (lane, other) = if receiving {
                (&retained.receive, &retained.transmit)
            } else {
                (&retained.transmit, &retained.receive)
            };
            if bound <= lane.bound() {
                return Ok(());
            }
            if bound
                .checked_add(other.bound())
                .is_none_or(|peak| peak > retained.factory.book.limits().total_max_bytes)
            {
                return Err(io::ErrorKind::InvalidInput.into());
            }
            lane.grow_bound(bound).await
        };
        Wait::new(future, &actual.factory.metadata).map(Some)
    }
    pub(super) fn receive_growth(&self, bound: usize) -> io::Result<Option<Wait>> {
        self.growth(bound, true)
    }
    pub(super) fn transmit_growth(&self, bound: usize) -> io::Result<Option<Wait>> {
        self.growth(bound, false)
    }
    pub(super) async fn transmit(&self, bound: usize) -> io::Result<()> {
        if let Some(wait) = self.transmit_growth(bound)? {
            wait.future.await?;
        }
        Ok(())
    }
}
