use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use zero_config::RuntimeConfig;
use zero_router::RuleSet;

use crate::EnginePlan;

/// A lightweight publication fact. Retained ingress services keep no historical
/// snapshot or native resource alive merely to enforce the candidate gate.
#[derive(Debug, Clone)]
pub struct ConfigurationPublication(Arc<AtomicBool>);
impl ConfigurationPublication {
    pub fn is_committed(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
    pub(super) fn pending() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }
    pub(super) fn committed() -> Self {
        Self(Arc::new(AtomicBool::new(true)))
    }
    pub(super) fn publish(&self) {
        self.0.store(true, Ordering::Release);
    }
}

/// One immutable generation of the configuration-derived engine state.
///
/// Keeping these values in one `Arc` prevents consumers from combining a plan
/// from one reload generation with configuration or routing data from another.
/// Mutable group selections and probe results also belong to this generation;
/// their target IDs must never be interpreted by a different snapshot's plan.
#[derive(Debug)]
pub struct EngineRuntimeSnapshot {
    pub(super) config_revision: Arc<AtomicU64>,
    pub(super) publication: ConfigurationPublication,
    pub(super) config: Arc<RuntimeConfig>,
    pub(super) endpoint_intents: Arc<super::endpoint::EndpointIntents>,
    pub(super) plan: Arc<EnginePlan>,
    pub(super) dns_dispatch: Option<zero_router::DomainDispatcher<String>>,
    pub(super) router: Arc<RuleSet>,
    pub(super) bypass: Arc<RuleSet>,
    pub(crate) outbound_group_state: Arc<crate::groups::OutboundGroupStateStore>,
}

impl EngineRuntimeSnapshot {
    pub fn configuration_publication(&self) -> ConfigurationPublication {
        self.publication.clone()
    }

    /// DNS dispatch shares the actual prepared matcher resources of this snapshot.
    pub fn dns_dispatch(&self) -> Option<zero_router::DomainDispatcher<String>> {
        self.dns_dispatch.clone()
    }

    pub fn config_revision(&self) -> u64 {
        self.config_revision.load(Ordering::Acquire)
    }

    pub fn config(&self) -> &Arc<RuntimeConfig> {
        &self.config
    }

    pub fn plan(&self) -> &Arc<EnginePlan> {
        &self.plan
    }

    /// Immutable Direct policy identity for a concrete outbound. `None`
    /// identifies the implicit route action, independent of a tag named direct.
    pub fn direct_dial_policy(&self, tag: Option<&str>) -> Option<(zero_traits::DialPolicy, u64)> {
        let Some(tag) = tag else {
            return Some((zero_traits::DialPolicy::default(), 0));
        };
        self.tagged_direct_dial_policy(tag)
            .map(|(policy, generation)| (policy.clone(), generation))
    }

    /// Compare a prepared identity against this live snapshot without cloning
    /// policy strings. Values alone cannot revive a retired policy generation.
    pub fn direct_dial_policy_matches(
        &self,
        tag: Option<&str>,
        policy: &zero_traits::DialPolicy,
        generation: u64,
    ) -> bool {
        match tag {
            None => generation == 0 && *policy == zero_traits::DialPolicy::default(),
            Some(tag) => self
                .tagged_direct_dial_policy(tag)
                .is_some_and(|(current, revision)| current == policy && revision == generation),
        }
    }

    fn tagged_direct_dial_policy(&self, tag: &str) -> Option<(&zero_traits::DialPolicy, u64)> {
        let outbound = self.plan.target(self.plan.target_id(tag)?)?.as_outbound()?;
        (outbound.runtime_kind() == zero_config::OutboundRuntimeKind::Direct)
            .then(|| (outbound.dial_policy(), outbound.dial_generation()))
    }
}

impl super::Engine {
    pub fn direct_dial_policy(&self, tag: Option<&str>) -> Option<(zero_traits::DialPolicy, u64)> {
        self.runtime_snapshot().direct_dial_policy(tag)
    }
}
