//! Actual source consumer edges supplied by its owning cold adapter.
use super::*;
pub type SourceDependencyDeclarations = BTreeMap<String, ([u8; 32], BTreeSet<ComponentId>)>;
impl ResourcePreparation<'_> {
    pub fn set_source_dependencies(
        &mut self,
        sources: SourceDependencyDeclarations,
    ) -> Result<(), BuildError> {
        self.ensure_valid()?;
        if self.source_dependencies.is_some() {
            return Err(invalid(
                Reason::StalePlan,
                "source dependencies already installed",
            ));
        }
        let candidate = self.candidate().ok_or_else(|| {
            invalid(
                Reason::StalePlan,
                "source dependencies have no actual candidate",
            )
        })?;
        if sources
            .keys()
            .any(|tag| !candidate.inbounds.iter().any(|input| &input.tag == tag))
            || sources.values().any(|(_, ids)| {
                ids.iter()
                    .any(|id| !self.named_components.values().any(|known| known == id))
            })
        {
            return Err(invalid(
                Reason::StalePlan,
                "source dependencies are outside this actual cold pass",
            ));
        }
        self.source_dependencies = Some(sources);
        Ok(())
    }
    pub(in crate::component) fn take_source_dependencies(
        &mut self,
    ) -> SourceDependencyDeclarations {
        self.source_dependencies.take().unwrap_or_default()
    }
}
