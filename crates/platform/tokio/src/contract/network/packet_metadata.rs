//! Destruction order for the existing real opener and weak pending observer.
//! No raw Arc/Weak of the prepaid Source opener leaves these concrete handles.
use super::*;
/// An actual weak observer's short strong borrow. Keeping the raw Arc alone
/// would detach its still-live future from the prepaid header attachment.
pub(in crate::contract) struct UpstreamOpenerLease {
    pub(super) opener: ManuallyDrop<Arc<dyn UpstreamOpener>>,
    pub(super) packet_metadata: Option<zero_tun::packet_memory::PacketMetadataAttachment>,
}
impl std::ops::Deref for UpstreamOpenerLease {
    type Target = dyn UpstreamOpener;
    fn deref(&self) -> &Self::Target {
        &**self.opener
    }
}
impl Drop for UpstreamOpenerLease {
    fn drop(&mut self) {
        let metadata = self.packet_metadata.take();
        // SAFETY: unique one-time destruction; metadata remains a local until
        // the actual strong pointer and its allocation have been released.
        let opener = unsafe { ManuallyDrop::take(&mut self.opener) };
        drop(opener);
        drop(metadata);
    }
}

impl Drop for UpstreamTransport {
    fn drop(&mut self) {
        let metadata = self.packet_metadata.take();
        // SAFETY: these fields are taken once in Drop, after all ordinary
        // methods/clones finished. Locals also free both pointers on unwind,
        // BEFORE the earlier-declared attachment can refund their allocation.
        let binding = unsafe { ManuallyDrop::take(&mut self.binding) };
        let opener = unsafe { ManuallyDrop::take(&mut self.opener) };
        drop(opener);
        drop(binding);
        drop(metadata);
    }
}
impl Drop for PendingCarrierObserver {
    fn drop(&mut self) {
        let metadata = self.packet_metadata.take();
        // SAFETY: matching one-time destruction, as in the strong handle.
        let binding = unsafe { ManuallyDrop::take(&mut self.binding) };
        let opener = unsafe { ManuallyDrop::take(&mut self.opener) };
        drop(opener);
        drop(binding);
        drop(metadata);
    }
}
impl UpstreamTransport {
    #[cfg(feature = "native-packet-origin")]
    pub(crate) fn packet_driver_opener_layout<T>(
        binding: &UpstreamBinding,
    ) -> Result<
        zero_tun::packet_memory::PacketMetadataLayout,
        zero_tun::packet_memory::PacketMemoryError,
    > {
        Self::opener_metadata_layout::<T>(binding)
    }
    /// Pure storage of this existing opener shape. This issues no caller,
    /// native/Scope permission or alternate execution authority.
    pub fn opener_metadata_layout<T>(
        binding: &UpstreamBinding,
    ) -> Result<
        zero_tun::packet_memory::PacketMetadataLayout,
        zero_tun::packet_memory::PacketMemoryError,
    > {
        use zero_tun::packet_memory::{PacketMetadataAttachment, PacketMetadataLayout};
        PacketMetadataLayout::arc::<T>()?
            .checked_add(PacketMetadataLayout::arc::<UpstreamBinding>()?)?
            .checked_add(PacketMetadataLayout::array::<RoleInstanceId>(
                binding.prefix.capacity(),
            )?)?
            .checked_add(PacketMetadataLayout::array::<u8>(
                binding.dial.interface.as_ref().map_or(0, String::capacity),
            )?)?
            .checked_add(PacketMetadataAttachment::layout()?)
    }
    #[cfg(feature = "native-packet-origin")]
    pub(crate) fn from_prepaid_packet_driver_opener<T: UpstreamOpener + 'static>(
        binding: UpstreamBinding,
        opener: T,
        credit: zero_tun::packet_memory::PacketMetadataCredit,
    ) -> Result<
        Self,
        (
            zero_tun::packet_memory::PacketMemoryError,
            UpstreamBinding,
            T,
            zero_tun::packet_memory::PacketMetadataCredit,
        ),
    > {
        let (transport, lease) = Self::from_prepaid_opener(binding, opener, credit)?;
        drop(lease);
        Ok(transport)
    }
    /// Same original raw allocations and closed attachment survive every
    /// strong/weak observer; foreign standalone `new` keeps its old ownership.
    pub fn from_prepaid_opener<T: UpstreamOpener + 'static>(
        binding: UpstreamBinding,
        opener: T,
        credit: zero_tun::packet_memory::PacketMetadataCredit,
    ) -> Result<
        (Self, PrepaidUpstreamOpenerLease<T>),
        (
            zero_tun::packet_memory::PacketMemoryError,
            UpstreamBinding,
            T,
            zero_tun::packet_memory::PacketMetadataCredit,
        ),
    > {
        let expected = match Self::opener_metadata_layout::<T>(&binding) {
            Ok(layout) => layout,
            Err(source) => return Err((source, binding, opener, credit)),
        };
        let actual = credit.layout();
        if expected.allocations() > actual.allocations() || expected.bytes() > actual.bytes() {
            return Err((
                zero_tun::packet_memory::PacketMemoryError::Capacity,
                binding,
                opener,
                credit,
            ));
        }
        let metadata = match zero_tun::packet_memory::PacketMetadataAttachment::from_prepaid(credit)
        {
            Ok(metadata) => metadata,
            Err((source, credit)) => return Err((source, binding, opener, credit)),
        };
        let actual = Arc::new(opener);
        let lease = PrepaidUpstreamOpenerLease {
            actual: ManuallyDrop::new(actual.clone()),
            metadata: Some(metadata.clone()),
        };
        Ok((
            Self {
                binding: ManuallyDrop::new(Arc::new(binding)),
                opener: ManuallyDrop::new(actual),
                packet_metadata: Some(metadata),
            },
            lease,
        ))
    }
}

/// Short typed initialization borrow of the same allocation; no raw Arc/Weak
/// escapes. The lease carries its original attachment until its Arc is gone.
pub struct PrepaidUpstreamOpenerLease<T> {
    actual: ManuallyDrop<Arc<T>>,
    metadata: Option<zero_tun::packet_memory::PacketMetadataAttachment>,
}
impl<T> PrepaidUpstreamOpenerLease<T> {
    pub fn actual(&self) -> &T {
        &self.actual
    }
}
impl<T> Drop for PrepaidUpstreamOpenerLease<T> {
    fn drop(&mut self) {
        let metadata = self.metadata.take();
        // SAFETY: one-time take; the attachment outlives the actual strong drop.
        let actual = unsafe { ManuallyDrop::take(&mut self.actual) };
        drop(actual);
        drop(metadata);
    }
}
