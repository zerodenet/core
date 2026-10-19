//! Snapshot-bound first-hop observations without retaining a component registry.
use crate::runtime::classify_channel_establishment_failure;
use std::sync::Arc;
use zero_engine::{Engine, EngineRuntimeSnapshot, PassiveRelayOutcome};
use zero_platform_tokio::contract::{
    stream::{BoxStream, Connected},
    ChannelError,
};

pub(super) struct FirstHopHealth {
    pub(super) engine: Engine,
    pub(super) snapshot: Arc<EngineRuntimeSnapshot>,
    pub(super) tag: String,
}
impl FirstHopHealth {
    pub(super) fn observe(&self, attempt: &Result<Connected<BoxStream>, ChannelError>) {
        match attempt {
            Ok(_) => self
                .engine
                .record_outbound_success_in_snapshot(&self.snapshot, &self.tag),
            Err(error)
                if classify_channel_establishment_failure(error)
                    == PassiveRelayOutcome::Failure =>
            {
                self.engine
                    .record_outbound_failure_in_snapshot(&self.snapshot, &self.tag);
            }
            Err(_) => {}
        }
    }
}
