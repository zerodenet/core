//! Host projection of exact Source consumer edges; no factory or IO replacement.
use super::*;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{
    component::{ResourceOwner, RoleKind},
    Reason,
};
impl ComponentRecipe {
    pub fn with_inbound_dependencies(
        mut self,
        dependencies: &[ComponentInstanceId],
    ) -> Result<Self, BuildError> {
        if self.spec.owner != ResourceOwner::Component
            || self.spec.roles != [RoleKind::Inbound]
            || !self.spec.dependencies.is_empty()
        {
            return Err(super::super::instance::invalid(
                Reason::StalePlan,
                "dependency projection requires the original Source recipe",
            ));
        }
        let ids: std::collections::BTreeSet<_> =
            dependencies.iter().map(|value| value.component).collect();
        if ids.len() != dependencies.len() {
            return Err(super::super::instance::invalid(
                Reason::StalePlan,
                "duplicate actual Source callee",
            ));
        }
        if dependencies.is_empty() {
            return Ok(self);
        }
        let mut spec = (*self.spec).clone();
        spec.dependencies = dependencies.iter().map(|value| value.component).collect();
        let mut identity = spec.static_identity.to_vec();
        identity.extend_from_slice(b"\0actual-source-dependencies-v1\0");
        for dependency in dependencies {
            identity.extend_from_slice(&dependency.component.0.to_be_bytes());
            identity.extend_from_slice(&dependency.generation.0.to_be_bytes());
        }
        spec.static_identity = identity.into();
        self.spec = Arc::new(spec);
        Ok(self)
    }
}
