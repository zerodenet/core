//! Persistent DNS transports depend on the entire actual selected native path.
use super::ProtocolInventory;
use crate::{
    component::{catalog::ComponentPlan, instance::ComponentSlot},
    protocol_registry::OutboundAdapterContext,
};
use std::{collections::BTreeSet, sync::Arc};
use zero_engine::{Engine, EngineRuntimeSnapshot, ResolvedOutbound};

#[derive(PartialEq, Eq)]
enum Shape {
    Single,
    Relay,
    Fallback,
}
struct Owners {
    shape: Shape,
    slots: Vec<Arc<ComponentSlot>>,
}
impl Owners {
    fn same(&self, other: &Self) -> bool {
        self.shape == other.shape
            && self.slots.len() == other.slots.len()
            && self
                .slots
                .iter()
                .zip(&other.slots)
                .all(|(old, new)| Arc::ptr_eq(old, new))
    }
}
impl ProtocolInventory {
    pub(crate) fn affected_dns_detours(
        &self,
        engine: &Engine,
        previous: &EngineRuntimeSnapshot,
        candidate: &EngineRuntimeSnapshot,
        prepared: &ComponentPlan,
    ) -> BTreeSet<String> {
        let targets: BTreeSet<_> = previous
            .config()
            .runtime
            .dns
            .iter()
            .chain(candidate.config().runtime.dns.iter())
            .flat_map(|config| config.servers.values())
            .filter_map(|server| server.detour().map(str::to_owned))
            .collect();
        let current = self.current_components(previous.config());
        targets
            .into_iter()
            .filter(|target| {
                let old = current
                    .as_ref()
                    .and_then(|plan| self.dns_owners(engine, previous, plan, target));
                let new = self.dns_owners(engine, candidate, prepared, target);
                match (old, new) {
                    (Some(old), Some(new)) => !old.same(&new),
                    // Legacy paths have no complete native owner identity. Retire
                    // conservatively instead of treating equal target names or
                    // numeric configuration generations as proof of reuse.
                    _ => true,
                }
            })
            .collect()
    }
    fn dns_owners(
        &self,
        engine: &Engine,
        snapshot: &EngineRuntimeSnapshot,
        components: &ComponentPlan,
        target: &str,
    ) -> Option<Owners> {
        // Dynamic selection may already differ from the pool's captured path.
        // Resolving it twice cannot recover that identity (and load-balance
        // resolution itself advances selection state). Such pools retire
        // conservatively on apply until carriers expose their actual binding.
        if has_dynamic_target(snapshot.config(), target) {
            return None;
        }
        let target = snapshot.plan().target_id(target)?;
        let (resolved, _plan) = engine.resolve_target_id_in_snapshot(snapshot, target)?;
        let (shape, leaves) = match &resolved {
            ResolvedOutbound::Single(leaf) => (Shape::Single, std::slice::from_ref(leaf)),
            ResolvedOutbound::Relay { chain } => (Shape::Relay, chain.as_slice()),
            ResolvedOutbound::Fallback { candidates } => (Shape::Fallback, candidates.as_slice()),
        };
        let slots = leaves
            .iter()
            .map(|leaf| {
                self.native_tcp_owner(
                    OutboundAdapterContext::new(snapshot.config()),
                    leaf,
                    components,
                )
            })
            .collect::<Option<Vec<_>>>()?;
        (!slots.is_empty()).then_some(Owners { shape, slots })
    }
}

fn has_dynamic_target(config: &zero_config::RuntimeConfig, target: &str) -> bool {
    use zero_config::OutboundGroupKind;
    let mut pending = vec![target];
    let mut visited = BTreeSet::new();
    while let Some(tag) = pending.pop() {
        if !visited.insert(tag) {
            continue;
        }
        let Some(group) = config.outbound_groups.iter().find(|group| group.tag == tag) else {
            continue;
        };
        match &group.group {
            OutboundGroupKind::Selector { .. }
            | OutboundGroupKind::UrlTest { .. }
            | OutboundGroupKind::LoadBalance { .. } => return true,
            group => pending.extend(group.members().iter().map(String::as_str)),
        }
    }
    false
}

#[cfg(all(test, feature = "socks5"))]
#[path = "tests/dns_dependencies.rs"]
mod tests;
