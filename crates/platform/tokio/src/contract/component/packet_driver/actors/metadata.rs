//! Prepay the real existing reconnect actor before its original IO/task gate.
use super::*;
pub(super) struct Prepared {
    pub(super) memory: zero_tun::packet_memory::PacketMemoryView,
    actor: Option<zero_traits::packet_metadata::PacketMetadataCredit>,
    failures: Option<zero_traits::packet_metadata::PacketMetadataCredit>,
}
impl Prepared {
    pub(super) fn reserve(calls: &PacketDriverCalls) -> Result<Self, ChannelError> {
        let memory = calls.metadata_memory()?;
        let actor = memory
            .reserve_driver_metadata(ReconnectRef::layout().map_err(memory::memory_error)?)
            .map_err(memory::memory_error)?;
        let failures = memory
            .reserve_driver_metadata(ActorFailuresRef::layout().map_err(memory::memory_error)?)
            .map_err(memory::memory_error)?;
        Ok(Self {
            memory,
            actor: Some(actor),
            failures: Some(failures),
        })
    }
    pub(super) fn install(
        &mut self,
        calls: &PacketDriverCalls,
    ) -> Result<ReconnectRef, ChannelError> {
        let failures = ActorFailuresRef::from_prepaid(
            faults::ActorFailures::default(),
            self.failures.take().expect("unique original actor promise"),
        )
        .map_err(|(source, _, _)| memory::memory_error(source))?;
        let value = Reconnect {
            witness: OnceLock::new(),
            failures,
            observing_failure: Mutex::new(()),
            failure_delivery: OnceLock::new(),
            accepting: true.into(),
            forwarded: calls.source_failures.prepare(),
        };
        ReconnectRef::from_prepaid(
            value,
            self.actor.take().expect("unique original actor promise"),
        )
        .map_err(|(source, _, _)| memory::memory_error(source))
    }
}
