use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use zero_config::RuntimeConfig;
use zero_router::RuleSet;

use crate::EnginePlan;

/// One immutable generation of the configuration-derived engine state.
///
/// Keeping these values in one `Arc` prevents consumers from combining a plan
/// from one reload generation with configuration or routing data from another.
#[derive(Debug)]
pub struct EngineRuntimeSnapshot {
    pub(super) config_revision: Arc<AtomicU64>,
    pub(super) config: Arc<RuntimeConfig>,
    pub(super) plan: Arc<EnginePlan>,
    pub(super) router: Arc<RuleSet>,
}

impl EngineRuntimeSnapshot {
    pub fn config_revision(&self) -> u64 {
        self.config_revision.load(Ordering::Acquire)
    }

    pub fn config(&self) -> &Arc<RuntimeConfig> {
        &self.config
    }

    pub fn plan(&self) -> &Arc<EnginePlan> {
        &self.plan
    }
}
