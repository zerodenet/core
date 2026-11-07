//! Concrete prepaid layout attached to every handle of one private allocation
//! family. The owning concrete handle must free its strong/weak pointers BEFORE
//! this attachment. No original pointer, Root setter or IO permission is issued.
use super::*;
pub struct PacketMetadataAttachment(Option<Arc<PacketMetadataCredit>>);
impl PacketMetadataAttachment {
    pub fn layout() -> Result<PacketMetadataLayout, PacketMemoryError> {
        PacketMetadataLayout::arc::<PacketMetadataCredit>()
    }
    pub fn from_prepaid(
        credit: PacketMetadataCredit,
    ) -> Result<Self, (PacketMemoryError, PacketMetadataCredit)> {
        let expected = match Self::layout() {
            Ok(expected) => expected,
            Err(source) => return Err((source, credit)),
        };
        let actual = credit.layout();
        if expected.allocations() > actual.allocations() || expected.bytes() > actual.bytes() {
            return Err((PacketMemoryError::Capacity, credit));
        }
        Ok(Self(Some(Arc::new(credit))))
    }
}
impl Clone for PacketMetadataAttachment {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl Drop for PacketMetadataAttachment {
    fn drop(&mut self) {
        if let Some(credit) = self.0.take() {
            // No Weak of this private attachment exists. Concurrent last
            // custodians have exactly one into_inner winner; the attachment
            // allocation is freed before its owned credit is dropped.
            drop(Arc::into_inner(credit));
        }
    }
}
