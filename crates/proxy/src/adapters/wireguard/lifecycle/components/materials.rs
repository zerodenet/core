//! Cold data handoff, not another owner table or source of IO permissions.
use super::super::recipe::outer::{OuterMaterials, OuterPathRecipe};
use super::*;
fn insert(cache: &mut OuterMaterials, outer: &OuterPathRecipe) {
    cache.insert(outer.materials().0.to_owned(), outer.retained_material());
}
impl WireguardAdapter {
    fn candidate_outer_materials(&self, config: &zero_config::RuntimeConfig) -> OuterMaterials {
        let mut materials = OuterMaterials::new();
        let Some(candidate) = PendingEndpoints::staged(&self.pending_endpoints, config) else {
            return materials;
        };
        for pending in candidate.values() {
            if let Some(outer) = pending
                .actual_graph()
                .and_then(|graph| graph.retained_outer())
            {
                insert(&mut materials, &outer);
            }
        }
        let roots = candidate
            .standalone
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .map(|candidate| candidate.root.clone())
            .collect::<Vec<_>>();
        for root in roots {
            if let Some(outer) = &root.outer {
                insert(&mut materials, outer);
            }
        }
        materials
    }
    pub(super) fn retained_outer_materials(
        &self,
        config: &zero_config::RuntimeConfig,
    ) -> Result<OuterMaterials, EngineError> {
        let mut materials = OuterMaterials::new();
        // Pure material references leave all existing owner locks before they
        // can be dropped, validated, or used by the cold protocol projection.
        let links = self
            .linked_endpoints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for link in links {
            if let Some(outer) = link
                .owned_resources()
                .and_then(|resources| resources.retained_outer())
            {
                insert(&mut materials, &outer);
            }
        }
        let roots = self
            .profiles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter_map(|profile| profile.graph.as_ref().and_then(std::sync::Weak::upgrade))
            .collect::<Vec<_>>();
        for root in roots {
            if let Some(outer) = &root.outer {
                insert(&mut materials, outer);
            }
        }
        materials.extend(self.candidate_outer_materials(config));
        Ok(materials)
    }
    pub(crate) fn release_component_resources_for_refresh(
        &self,
        config: &zero_config::RuntimeConfig,
    ) -> Result<OuterMaterials, EngineError> {
        let material = self.candidate_outer_materials(config);
        // Only after the existing complete original cleanup gate succeeds may
        // the caller retain this inert material through the same cold refresh.
        self.release_component_resources(config)?;
        Ok(material)
    }
}
