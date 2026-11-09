//! One Source's concrete UDP address copies. No IO or task rights are carried.
use super::*;
use zero_platform_tokio::contract::component::PacketDriverMetadataView;
use zero_traits::packet_metadata::{PacketMetadataCredit, PacketMetadataLayout};

pub(crate) struct UdpDomainMemory {
    pub(crate) source: PacketDriverMetadataView,
    credit: PacketMetadataCredit,
}
impl std::fmt::Debug for UdpDomainMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UdpDomainMemory")
    }
}
pub(crate) fn address_layout(address: &Address) -> Result<PacketMetadataLayout, EngineError> {
    match address {
        Address::Domain(name) => PacketMetadataLayout::array::<u8>(name.len()).map_err(failure),
        _ => Ok(PacketMetadataLayout::new(0, 0)),
    }
}
pub(crate) fn failure(source: impl std::error::Error + Send + Sync + 'static) -> EngineError {
    std::io::Error::other(source).into()
}
impl UdpDomainMemory {
    pub(super) fn prepare(
        dispatch: &InboundUdpDispatch,
        source: Option<PacketDriverMetadataView>,
    ) -> Result<Option<Self>, EngineError> {
        let Some(actual) = dispatch.domain_metadata() else {
            return Ok(None);
        };
        let source = source.ok_or_else(|| {
            failure(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Source domain has no actual memory owner",
            ))
        })?;
        let identity = source
            .reserve(PacketMetadataLayout::new(0, 0))
            .map_err(failure)?;
        if !identity.same_counter(actual) {
            return Err(failure(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Source domain counter changed before the pipe",
            )));
        }
        drop(identity);
        let mut layout = address_layout(dispatch.target())?;
        for address in [
            dispatch.route_target(),
            dispatch.sniffed_original_target(),
            dispatch.transparent_original_target(),
        ]
        .into_iter()
        .flatten()
        {
            layout = layout
                .checked_add(address_layout(address)?)
                .map_err(failure)?;
        }
        let credit = source.reserve(layout).map_err(failure)?;
        Ok(Some(Self { source, credit }))
    }
    pub(crate) fn pay_clone(&mut self, address: &Address) -> Result<(), EngineError> {
        let extra = self
            .source
            .reserve(address_layout(address)?)
            .map_err(failure)?;
        let empty = self.credit.absorb(extra).map_err(|(source, returned)| {
            drop(returned);
            failure(source)
        })?;
        drop(empty);
        Ok(())
    }
    pub(crate) fn pay_string(&mut self, length: usize) -> Result<(), EngineError> {
        let extra = self
            .source
            .reserve(PacketMetadataLayout::array::<u8>(length).map_err(failure)?)
            .map_err(failure)?;
        let empty = self.credit.absorb(extra).map_err(|(source, returned)| {
            drop(returned);
            failure(source)
        })?;
        drop(empty);
        Ok(())
    }
}
