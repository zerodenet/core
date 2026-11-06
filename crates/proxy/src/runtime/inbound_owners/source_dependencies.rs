//! The installed Source's exact consumer instances, not configuration guesses.
use super::*;
impl InboundOwnerRegistry {
    pub(in crate::runtime) fn changed_source_dependency_tags(
        &self,
        config: &zero_config::RuntimeConfig,
        plan: &crate::component::catalog::ComponentPlan,
    ) -> Result<std::collections::BTreeSet<String>, EngineError> {
        #[cfg(feature = "raw-ip-runtime")]
        {
            let mut changed = std::collections::BTreeSet::new();
            for input in &config.inbounds {
                if self.source_dependencies_changed(input, plan)? { changed.insert(input.tag.clone()); }
            }
            Ok(changed)
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        { let _ = (config, plan); Ok(Default::default()) }
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(in crate::runtime) fn source_dependencies_changed(
        &self,
        input: &zero_config::InboundConfig,
        plan: &crate::component::catalog::ComponentPlan,
    ) -> Result<bool, EngineError> {
        let Some(owner) = self.owner(&input.tag) else {
            return Ok(false);
        };
        let next = plan
            .source_dependency_instances(&input.tag, &super::source_input_identity(input)?)
            .map_err(std::io::Error::other)?;
        Ok(!owner
            .source_dependencies
            .get()
            .map_or(next.is_empty(), |actual| actual.as_ref() == next.as_slice()))
    }
}
