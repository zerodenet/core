//! Concrete u16 framing allocations from this actual owning Factory's Host.
mod output;
use super::*;
pub(in crate::component) use output::Output;
use std::ops::{Deref, DerefMut};
use zero_platform_tokio::{
    contract::component::{PacketDriverMetadataView, ResourceAccess},
    controlled_carrier::{
        CarrierBufferBudget, CarrierBufferFrameCredit, CarrierBufferWorkingLane,
        CarrierBufferWorkingShape,
    },
};
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataCredit, PacketMetadataLayout};
#[derive(Clone)]
pub(in crate::component) struct Budget {
    book: CarrierBufferBudget,
    metadata: PacketDriverMetadataView,
}
impl Budget {
    pub fn from_resources(resources: &dyn ResourceAccess) -> Result<Self, ChannelError> {
        Ok(Self {
            book: resources
                .carrier_buffer_budget()
                .ok_or_else(|| failure(Reason::Unsupported))?,
            metadata: resources.packet_driver_metadata()?,
        })
    }
    pub fn new(book: CarrierBufferBudget, metadata: PacketDriverMetadataView) -> Self {
        Self { book, metadata }
    }
}
pub(in crate::component) enum Prepared {
    Library,
    Hosted(Hosted),
}
pub(in crate::component) struct Hosted {
    pub shared: PacketMetadataCredit,
    pub tx_cancel: PacketMetadataCredit,
    pub rx_cancel: PacketMetadataCredit,
    pub tx: Buffer,
    pub rx: Buffer,
    pub output: Output,
    pub targets: Option<zero_platform_tokio::controlled_carrier::CarrierBufferCredit>,
}
impl Prepared {
    /// Reuse the actual prepaid TX backing for the original request. No
    /// temporary handshake Vec is allocated beside the retained framing Vec.
    pub fn request<'a>(
        &'a mut self,
        target: &Destination,
        uuid: &[u8; 16],
    ) -> Result<Request<'a>, zero_core::Error> {
        match self {
            Self::Library => crate::shared::build_request_to(
                &target.address,
                target.port,
                uuid,
                crate::shared::CMD_UDP,
            )
            .map(Request::Library),
            Self::Hosted(hosted) => {
                let bytes = &mut hosted.tx;
                bytes.clear();
                bytes.push(crate::shared::VLESS_VERSION);
                bytes.extend_from_slice(uuid);
                bytes.push(0);
                bytes.push(crate::shared::CMD_UDP);
                bytes.extend_from_slice(&target.port.to_be_bytes());
                crate::shared::write_address(bytes, &target.address)?;
                Ok(Request::Hosted(bytes))
            }
        }
    }
    pub fn new(
        budget: Option<&Budget>,
        cancel: &CancellationToken,
        admission: &Arc<SessionAdmission>,
        target: &Destination,
    ) -> Result<Self, ChannelError> {
        let Some(budget) = budget else {
            return Ok(Self::Library);
        };
        let domain = match &target.address {
            zero_core::Address::Domain(value) => value.len(),
            _ => 0,
        };
        let mut working = budget
            .book
            .try_working_set(CarrierBufferWorkingShape {
                receive_scratch_bytes: 65537,
                transmit_working_bytes: 65537,
                receive_working_bytes: 65535 + domain,
            })
            .map_err(capacity)?;
        // Two new per-session targets: Shared and the Tx limits. Returned
        // limits move the request's original, which remains its caller's duty.
        let targets = if domain == 0 {
            None
        } else {
            Some(
                budget
                    .book
                    .try_reserve(
                        domain
                            .checked_mul(2)
                            .ok_or_else(|| failure(Reason::Capacity))?,
                    )
                    .map_err(capacity)?,
            )
        };
        let output = Output::prepare(working.receive(), &budget.metadata)?;
        let header = PacketMetadataArc::<Shared>::layout().map_err(capacity)?;
        let sample = super::cancelled(cancel.clone(), admission.clone());
        let cancel_layout = boxed_layout(&sample);
        drop(sample);
        let layout = header
            .checked_add(cancel_layout)
            .and_then(|layout| layout.checked_add(cancel_layout))
            .map_err(capacity)?;
        let mut paid = budget.metadata.reserve(layout).map_err(capacity)?;
        let shared = paid.split(header).map_err(capacity)?;
        let tx_cancel = paid.split(cancel_layout).map_err(capacity)?;
        let tx = Buffer::new(
            working.transmit().try_reserve(65537).map_err(capacity)?,
            65537,
        )?;
        let rx = Buffer::scratch(
            working.take_receive_scratch().expect("one actual scratch"),
            65537,
        )?;
        Ok(Self::Hosted(Hosted {
            shared,
            tx_cancel,
            rx_cancel: paid,
            tx,
            rx,
            output,
            targets,
        }))
    }
}
pub(in crate::component) enum Request<'a> {
    Library(Vec<u8>),
    Hosted(&'a Buffer),
}
impl Deref for Request<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::Library(bytes) => bytes,
            Self::Hosted(bytes) => bytes,
        }
    }
}
pub(in crate::component) enum Buffer {
    Library(Vec<u8>),
    Frame {
        bytes: Vec<u8>,
        _credit: CarrierBufferFrameCredit,
    },
    Scratch {
        bytes: Vec<u8>,
        _credit: zero_platform_tokio::controlled_carrier::CarrierBufferCredit,
    },
}
impl Buffer {
    fn allocation(capacity: usize) -> Result<Vec<u8>, ChannelError> {
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(capacity).map_err(capacity_error)?;
        Ok(bytes)
    }
    fn new(credit: CarrierBufferFrameCredit, bound: usize) -> Result<Self, ChannelError> {
        Ok(Self::Frame {
            bytes: Self::allocation(bound)?,
            _credit: credit,
        })
    }
    fn scratch(
        credit: zero_platform_tokio::controlled_carrier::CarrierBufferCredit,
        bound: usize,
    ) -> Result<Self, ChannelError> {
        Ok(Self::Scratch {
            bytes: Self::allocation(bound)?,
            _credit: credit,
        })
    }
}
impl Deref for Buffer {
    type Target = Vec<u8>;
    fn deref(&self) -> &Vec<u8> {
        match self {
            Self::Library(bytes) | Self::Frame { bytes, .. } | Self::Scratch { bytes, .. } => bytes,
        }
    }
}
impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut Vec<u8> {
        match self {
            Self::Library(bytes) | Self::Frame { bytes, .. } | Self::Scratch { bytes, .. } => bytes,
        }
    }
}
pub(super) fn boxed_layout<F>(_: &F) -> PacketMetadataLayout {
    PacketMetadataLayout::boxed::<F>()
}
fn capacity_error(source: std::collections::TryReserveError) -> ChannelError {
    capacity(source)
}
fn capacity(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    failure(Reason::Capacity).with_source(source)
}
fn failure(reason: Reason) -> ChannelError {
    ChannelError::new(
        Stage::Prepare,
        reason,
        zero_platform_tokio::contract::Impact::Component,
    )
}

#[cfg(test)]
#[path = "../../../tests/component/datagram/memory.rs"]
mod tests;
