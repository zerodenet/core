//! Generation-bound first-hop health without retaining config or registries.
use crate::runtime::classify_channel_establishment_failure;
use std::sync::Arc;
use zero_engine::{Engine, EngineRuntimeSnapshot, OutboundHealthObservation, PassiveRelayOutcome};
use zero_platform_tokio::contract::{
    stream::{BoxStream, Connected},
    ChannelError,
};

pub(super) struct FirstHopHealth {
    observation: OutboundHealthObservation,
}
impl FirstHopHealth {
    pub(super) fn new(engine: &Engine, snapshot: &Arc<EngineRuntimeSnapshot>, tag: &str) -> Self {
        Self {
            observation: engine.bind_outbound_health(snapshot, tag),
        }
    }
    pub(super) fn observe(&self, attempt: &Result<Connected<BoxStream>, ChannelError>) {
        match attempt {
            Ok(_) => self.observation.record_success(),
            Err(error)
                if classify_channel_establishment_failure(error)
                    == PassiveRelayOutcome::Failure =>
            {
                self.observation.record_failure();
            }
            Err(_) => {}
        }
    }
}

#[cfg(test)]
#[path = "../../tests/relay_health.rs"]
mod tests;
