//! Exact same-Host allocation promises; no IO, Slot, Scope or receiver backedge.
use super::*;
use crate::contract::component::PacketDriverMetadataView;
use std::ops::Deref;
use zero_traits::packet_metadata::{
    PacketMemoryError, PacketMetadataArc, PacketMetadataCredit, PacketMetadataLayout,
};

pub(in crate::controlled_datagram::metadata_pool) enum Allocation<T> {
    Library(Arc<T>),
    Hosted(PacketMetadataArc<T>),
}
impl<T> Clone for Allocation<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Library(x) => Self::Library(x.clone()),
            Self::Hosted(x) => Self::Hosted(x.clone()),
        }
    }
}
impl<T> Deref for Allocation<T> {
    type Target = T;
    fn deref(&self) -> &T {
        match self {
            Self::Library(x) => x,
            Self::Hosted(x) => x,
        }
    }
}
impl<T> Allocation<T> {
    pub(in crate::controlled_datagram::metadata_pool) fn same(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Library(a), Self::Library(b)) => Arc::ptr_eq(a, b),
            (Self::Hosted(a), Self::Hosted(b)) => PacketMetadataArc::ptr_eq(a, b),
            _ => false,
        }
    }
}
impl<T: Error + Send + Sync + 'static> Allocation<T> {
    pub(super) fn attach(&self, error: ChannelError) -> ChannelError {
        match self {
            Self::Library(x) => error.with_source(x.clone()),
            Self::Hosted(x) => error.with_metadata_source(x.clone().into_error()),
        }
    }
    pub(super) fn bind(&self, receipt: &crate::contract::CleanupFailureReceipt) -> bool {
        match self {
            Self::Library(x) => receipt.bind_payload(x.clone()),
            Self::Hosted(x) => receipt.bind_metadata_payload(x.clone().into_error()),
        }
    }
}
pub(in crate::controlled_datagram::metadata_pool) struct Prepaid {
    credit: Option<PacketMetadataCredit>,
}
fn layout(
    limits: super::super::super::metadata::MetadataLimits,
) -> Result<PacketMetadataLayout, PacketMemoryError> {
    PacketMetadataArc::<crate::EgressSelection>::layout()?
        .checked_add(PacketMetadataArc::<super::super::completion::State>::layout()?)?
        .checked_add(PacketMetadataArc::<Originals>::layout()?)?
        .checked_add(PacketMetadataLayout::array::<Original>(
            limits.main_sockets.get(),
        )?)?
        .checked_add(PacketMetadataLayout::array::<Original>(
            limits.probe_attempts_per_initialization.get(),
        )?)?
        .checked_add(PacketMetadataArc::<Canonical>::layout()?)?
        .checked_add(PacketMetadataArc::<NativePayload>::layout()?)?
        .checked_add(PacketMetadataArc::<row::Payload>::layout()?)?
        .checked_add(crate::contract::CleanupFailureReceipt::metadata_layout()?)?
        .checked_add(crate::contract::CleanupFailureReceipt::metadata_layout()?)?
        .checked_add(zero_error::channel::RetainedError::metadata_layout()?)?
        .checked_add(zero_error::channel::RetainedError::metadata_layout()?)
}
impl Prepaid {
    pub(in crate::controlled_datagram::metadata_pool) fn egress_credit(
        &mut self,
    ) -> Option<PacketMetadataCredit> {
        self.take(
            PacketMetadataArc::<crate::EgressSelection>::layout()
                .expect("actual original egress allocation"),
        )
    }
    pub(in crate::controlled_datagram::metadata_pool) fn completion(
        &mut self,
    ) -> super::super::completion::Completion {
        super::super::completion::Completion::new(
            self.allocate(super::super::completion::State::default()),
        )
    }
    pub(in crate::controlled_datagram::metadata_pool) fn reserve(
        limits: super::super::super::metadata::MetadataLimits,
        memory: Option<&PacketDriverMetadataView>,
    ) -> Result<Self, ChannelError> {
        let credit = memory
            .map(|memory| layout(limits).and_then(|layout| memory.reserve(layout)))
            .transpose()
            .map_err(|source| {
                error(
                    Reason::Capacity,
                    "metadata original source allocation refused",
                )
                .with_source(source)
            })?;
        Ok(Self { credit })
    }
    fn take(&mut self, layout: PacketMetadataLayout) -> Option<PacketMetadataCredit> {
        self.credit.as_mut().map(|credit| {
            credit
                .split(layout)
                .expect("complete fixed source layout prepaid")
        })
    }
    pub(super) fn allocate<T>(&mut self, value: T) -> Allocation<T> {
        match self.take(PacketMetadataArc::<T>::layout().expect("fixed source allocation")) {
            Some(credit) => Allocation::Hosted(
                PacketMetadataArc::from_prepaid(value, credit)
                    .unwrap_or_else(|_| panic!("exact source layout prepaid")),
            ),
            None => Allocation::Library(Arc::new(value)),
        }
    }
    pub(super) fn rows(&mut self, count: usize) -> (Box<[Original]>, Option<PacketMetadataCredit>) {
        let credit = self.take(
            PacketMetadataLayout::array::<Original>(count).expect("validated bounded sources"),
        );
        let mut rows = Vec::with_capacity(count);
        rows.resize_with(count, Original::default);
        (rows.into_boxed_slice(), credit)
    }
    pub(super) fn canonical(&mut self) -> Canonical {
        Canonical {
            value: OnceLock::new(),
            credit: Mutex::new(
                self.take(
                    zero_error::channel::RetainedError::metadata_layout()
                        .expect("fixed original diagnostic"),
                ),
            ),
        }
    }
    pub(super) fn receipt(&mut self) -> crate::contract::CleanupFailureReceipt {
        match self
            .take(crate::contract::CleanupFailureReceipt::metadata_layout().expect("fixed receipt"))
        {
            Some(credit) => crate::contract::CleanupFailureReceipt::from_prepaid(credit)
                .unwrap_or_else(|_| panic!("exact receipt layout prepaid")),
            None => crate::contract::CleanupFailureReceipt::new(),
        }
    }
}
/// One actual diagnostic per origin, paid before native acquisition. No raw
/// Hosted Arc or Weak escapes; the sealed handle holds its header through Weak.
pub(super) struct Canonical {
    value: OnceLock<zero_error::channel::RetainedError>,
    credit: Mutex<Option<PacketMetadataCredit>>,
}
impl Canonical {
    pub(super) fn get_or_init(
        &self,
        build: impl FnOnce() -> ChannelError,
    ) -> zero_error::channel::RetainedError {
        self.value
            .get_or_init(|| {
                let error = build();
                let credit = {
                    self.credit
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .take()
                };
                match credit {
                    Some(credit) => zero_error::channel::RetainedError::from_prepaid(error, credit)
                        .unwrap_or_else(|_| panic!("diagnostic prepaid before native")),
                    None => Arc::new(error).into(),
                }
            })
            .clone()
    }
}
#[cfg(test)]
pub(super) enum WeakAllocation<T> {
    Library(Weak<T>),
    Hosted(zero_traits::packet_metadata::PacketMetadataWeak<T>),
}
#[cfg(test)]
impl<T> Allocation<T> {
    pub(super) fn downgrade(&self) -> WeakAllocation<T> {
        match self {
            Self::Library(x) => WeakAllocation::Library(Arc::downgrade(x)),
            Self::Hosted(x) => WeakAllocation::Hosted(PacketMetadataArc::downgrade(x)),
        }
    }
}
#[cfg(test)]
impl<T> WeakAllocation<T> {
    pub(super) fn upgrade(&self) -> Option<Allocation<T>> {
        match self {
            Self::Library(x) => x.upgrade().map(Allocation::Library),
            Self::Hosted(x) => x.upgrade().map(Allocation::Hosted),
        }
    }
}
