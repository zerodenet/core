//! One paid missing-Root observation per exact sealed original, not IO history.
use super::*;
use std::sync::OnceLock;
use zero_platform_tokio::contract::{Impact, RetainedError, Stage};
use zero_traits::packet_metadata::PacketMetadataArc;
pub(in crate::component::native_dns::provider) struct ProtocolUse {
    pub original: ProtocolOriginal,
    failure: PacketMetadataArc<WaitFailure>,
    retained: RetainedError,
}
impl ProtocolUse {
    pub(super) fn prepare(
        original: ProtocolOriginal,
        memory: &PacketDriverMetadataView,
    ) -> Result<Self, ChannelError> {
        let (failure, retained) = prepare_failure(memory)?;
        Ok(Self {
            original,
            failure,
            retained,
        })
    }
    pub(super) fn receive_failure(
        &self,
        source: ChannelError,
    ) -> (RetainedError, Option<ChannelError>) {
        let repeated = self.failure.0.set(source).err();
        (self.retained.clone(), repeated)
    }
}
struct WaitFailure(OnceLock<ChannelError>);
impl std::fmt::Debug for WaitFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalProtocolWaitFailure")
            .field("observed", &self.0.get().is_some())
            .finish()
    }
}
impl std::fmt::Display for WaitFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("same original Root disappeared without settlement")
    }
}
impl std::error::Error for WaitFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.get().map(|source| source as _)
    }
}

fn prepare_failure(
    memory: &PacketDriverMetadataView,
) -> Result<(PacketMetadataArc<WaitFailure>, RetainedError), ChannelError> {
    let capacity = |source| {
        crate::component::instance::invalid(
            Reason::Capacity,
            "DNS protocol observation storage exhausted",
        )
        .with_source(source)
    };
    let credit = memory
        .reserve(PacketMetadataArc::<WaitFailure>::layout().map_err(capacity)?)
        .map_err(capacity)?;
    let failure = PacketMetadataArc::from_prepaid(WaitFailure(OnceLock::new()), credit)
        .map_err(|(source, _, _)| capacity(source))?;
    let credit = memory
        .reserve(RetainedError::metadata_layout().map_err(capacity)?)
        .map_err(capacity)?;
    // This exact API can fail only when its original phase disappeared
    // before publishing settlement. Actual protocol/native/driver failures
    // remain in the original Root's own canonical reports and receipts.
    let error = ChannelError::new(Stage::Close, Reason::Revoked, Impact::Component)
        .with_metadata_source(failure.clone().into_error());
    let retained =
        RetainedError::from_prepaid(error, credit).map_err(|(source, _, _)| capacity(source))?;

    Ok((failure, retained))
}

#[cfg(test)]
mod tests;
