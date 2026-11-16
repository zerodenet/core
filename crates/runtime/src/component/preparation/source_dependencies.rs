//! Actual source consumer edges supplied by its owning cold adapter.
use super::*;
pub type SourceDependencyDeclarations = BTreeMap<String, ([u8; 32], BTreeSet<ComponentId>)>;
impl ResourcePreparation<'_> {
    /// Owning adapters add actual named provider edges in this same cold pass.
    /// Only identities reserved by the host and the full Source digest enter it.
    pub fn add_source_dependency(
        &mut self,
        tag: &str,
        identity: [u8; 32],
        component: ComponentId,
    ) -> Result<(), BuildError> {
        self.ensure_valid()?;
        if !self
            .candidate()
            .is_some_and(|config| config.inbounds.iter().any(|input| input.tag == tag))
            || !self
                .named_components
                .values()
                .any(|actual| *actual == component)
                && !self
                    .resources
                    .values()
                    .any(|actual| actual.component == component)
        {
            self.invalidate();
            return Err(invalid(
                Reason::StalePlan,
                "Source dependency is outside its actual cold pass",
            ));
        }
        let row = self
            .source_dependencies
            .get_or_insert_with(Default::default)
            .entry(tag.into())
            .or_insert_with(|| (identity, BTreeSet::new()));
        if row.0 != identity {
            self.invalidate();
            return Err(invalid(
                Reason::StalePlan,
                "same Source has different cold identity",
            ));
        }
        row.1.insert(component);
        Ok(())
    }
    pub fn set_source_dependencies(
        &mut self,
        sources: SourceDependencyDeclarations,
    ) -> Result<(), BuildError> {
        self.ensure_valid()?;
        if self.source_dependencies_installed {
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
                ids.iter().any(|id| {
                    !self.named_components.values().any(|known| known == id)
                        && !self
                            .resources
                            .values()
                            .any(|resource| resource.component == *id)
                })
            })
        {
            return Err(invalid(
                Reason::StalePlan,
                "source dependencies are outside this actual cold pass",
            ));
        }
        for (tag, (identity, ids)) in sources {
            let row = self
                .source_dependencies
                .get_or_insert_with(Default::default)
                .entry(tag)
                .or_insert_with(|| (identity, BTreeSet::new()));
            if row.0 != identity {
                self.invalidate();
                return Err(invalid(
                    Reason::StalePlan,
                    "same Source has different cold identity",
                ));
            }
            row.1.extend(ids);
        }
        self.source_dependencies_installed = true;
        Ok(())
    }
    pub(in crate::component) fn take_source_dependencies_or(
        &mut self,
        previous: &SourceDependencyDeclarations,
    ) -> SourceDependencyDeclarations {
        self.source_dependencies
            .take()
            .unwrap_or_else(|| previous.clone())
    }
    pub(in crate::component) fn take_source_dependencies(
        &mut self,
    ) -> SourceDependencyDeclarations {
        self.source_dependencies.take().unwrap_or_default()
    }
}
