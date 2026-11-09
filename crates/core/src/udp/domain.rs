//! Actual address backing only. No Root constructor, policy, IO or Scope grant.
use super::*;
use core::fmt;
use zero_traits::packet_metadata::{PacketMemoryError, PacketMetadataCredit, PacketMetadataLayout};

#[derive(Default)]
pub(super) struct DomainMemory(Option<PacketMetadataCredit>);
impl fmt::Debug for DomainMemory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DomainMemory")
    }
}

#[cfg(test)]
#[path = "domain/tests.rs"]
mod tests;
// Custody is not part of the dispatch's existing semantic equality.
impl PartialEq for DomainMemory {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for DomainMemory {}

fn address_layout(address: &Address) -> Result<PacketMetadataLayout, PacketMemoryError> {
    match address {
        Address::Domain(name) => PacketMetadataLayout::array::<u8>(name.capacity()),
        _ => Ok(PacketMetadataLayout::new(0, 0)),
    }
}
fn optional_layout(address: Option<&Address>) -> Result<PacketMetadataLayout, PacketMemoryError> {
    address
        .map(address_layout)
        .transpose()
        .map(|v| v.unwrap_or(PacketMetadataLayout::new(0, 0)))
}
impl InboundUdpDispatch {
    /// Exact future String capacities before the existing sniffed-domain
    /// transformation allocates its name or clones the original destination.
    pub fn sniffed_domain_metadata_layout(
        &self,
        name_bytes: usize,
        route_only: bool,
    ) -> Result<PacketMetadataLayout, PacketMemoryError> {
        let name = PacketMetadataLayout::array::<u8>(name_bytes)?;
        let original = if route_only {
            address_layout(&self.target)?
                .checked_add(optional_layout(self.sniffed_original_target.as_ref())?)?
        } else {
            // String::clone copies initialized bytes, not unused source capacity.
            match &self.target {
                Address::Domain(value) => PacketMetadataLayout::array::<u8>(value.len())?,
                _ => PacketMetadataLayout::new(0, 0),
            }
        };
        name.checked_add(original)?
            .checked_add(if route_only {
                PacketMetadataLayout::new(0, 0)
            } else {
                optional_layout(self.route_target.as_ref())?
            })?
            .checked_add(optional_layout(self.transparent_original_target.as_ref())?)
    }
    /// Attach a previously reserved exact backing credit after constructing
    /// only those same Strings. Error returns all original values unchanged.
    pub fn with_domain_metadata(
        mut self,
        credit: PacketMetadataCredit,
    ) -> Result<Self, (PacketMemoryError, Self, PacketMetadataCredit)> {
        let expected = (|| {
            address_layout(&self.target)?
                .checked_add(optional_layout(self.route_target.as_ref())?)?
                .checked_add(optional_layout(self.sniffed_original_target.as_ref())?)?
                .checked_add(optional_layout(self.transparent_original_target.as_ref())?)
        })();
        let expected = match expected {
            Ok(expected) => expected,
            Err(source) => return Err((source, self, credit)),
        };
        if self
            .domain_memory
            .0
            .as_ref()
            .is_some_and(|old| !old.same_counter(&credit))
        {
            return Err((PacketMemoryError::Stopped, self, credit));
        }
        if credit.layout().allocations() < expected.allocations()
            || credit.layout().bytes() < expected.bytes()
        {
            return Err((PacketMemoryError::Capacity, self, credit));
        }
        // The replacement already pays every surviving String on that same
        // counter before the previous backing credit is returned.
        self.domain_memory.0 = Some(credit);
        Ok(self)
    }
    /// Passive exact counter identity/layout, never allocation or IO authority.
    pub fn domain_metadata(&self) -> Option<&PacketMetadataCredit> {
        self.domain_memory.0.as_ref()
    }
}
