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
    pub(super) config: Arc<RuntimeConfig>,
    pub(super) plan: Arc<EnginePlan>,
    pub(super) router: Arc<RuleSet>,
}

impl EngineRuntimeSnapshot {
    pub fn config(&self) -> &Arc<RuntimeConfig> {
        &self.config
    }

    pub fn plan(&self) -> &Arc<EnginePlan> {
        &self.plan
    }
}
