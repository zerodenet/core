//! Same existing callback, with closed custody of its concrete Arc header.
//! This owns allocation only; no IO/task/Scope/return capability is granted.
mod call;
use super::*;
pub use call::OwnedXhttpFactoryCall;
use zero_traits::packet_metadata::PacketMemoryError;
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataCredit, PacketMetadataLayout};
struct Holder {
    // The sole raw Arc has no public strong/weak/getter escape. Its actual
    // concrete header deallocates before this paid Holder's header/custody.
    actual: Arc<dyn Fn() -> OwnedXhttpFactoryCall + Send + Sync>,
}
#[derive(Clone)]
pub enum OwnedXhttpCarrierFactory {
    /// Explicit original independent-library callback custody.
    Shared(XhttpCarrierFactory),
    #[doc(hidden)]
    Paid(PaidFactory),
}
#[derive(Clone)]
pub struct PaidFactory(PacketMetadataArc<Holder>);
impl OwnedXhttpCarrierFactory {
    pub fn metadata_layout_for<F>(_: &F) -> Result<PacketMetadataLayout, PacketMemoryError> {
        PacketMetadataLayout::arc::<F>()?.checked_add(PacketMetadataArc::<Holder>::layout()?)
    }
    pub fn from_prepaid<F>(
        actual: F,
        credit: PacketMetadataCredit,
    ) -> Result<Self, (PacketMemoryError, F, PacketMetadataCredit)>
    where
        F: Fn() -> OwnedXhttpFactoryCall + Send + Sync + 'static,
    {
        let expected = match Self::metadata_layout_for(&actual) {
            Ok(layout) => layout,
            Err(source) => return Err((source, actual, credit)),
        };
        if credit.layout().allocations() < expected.allocations()
            || credit.layout().bytes() < expected.bytes()
        {
            return Err((PacketMemoryError::Capacity, actual, credit));
        }
        // All concrete closure + Holder + existing attachment layouts are
        // already paid. No wrapper closure or second callback is allocated.
        let holder = Holder {
            actual: Arc::new(actual),
        };
        let holder = PacketMetadataArc::from_prepaid(holder, credit)
            .unwrap_or_else(|_| panic!("exact factory allocation promised"));
        Ok(Self::Paid(PaidFactory(holder)))
    }
}
impl From<XhttpCarrierFactory> for OwnedXhttpCarrierFactory {
    fn from(actual: XhttpCarrierFactory) -> Self {
        Self::Shared(actual)
    }
}
impl OwnedXhttpCarrierFactory {
    pub(in crate::split_http) fn call(&self) -> OwnedXhttpFactoryCall {
        match self {
            Self::Shared(actual) => OwnedXhttpFactoryCall::shared(actual()),
            Self::Paid(actual) => (actual.0.actual)(),
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/split_http/owned_factory.rs"]
mod tests;
