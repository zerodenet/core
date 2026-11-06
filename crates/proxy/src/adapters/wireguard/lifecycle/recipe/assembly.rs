//! Install one pure graph in its original canonical preparation pass.
use super::*;
impl LinkedEndpointRecipe {
    /// Split one pure candidate into the canonical graph owner and its short
    /// BuildNew invocation. No query budget or network service enters Options.
    pub(super) fn into_component(
        self,
        preparation: &mut crate::component::ResourcePreparation<'_>,
    ) -> Result<(zero_core::component::ComponentId, LinkedInvocation), EngineError> {
        let fresh = self.reuse.is_none();
        let (options, invocation) = self.into_parts()?;
        let id = preparation
            .share(
                SHARED_NAMESPACE,
                Arc::new(LinkedStackFactory),
                options.clone(),
            )
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
        if options.resource.get_or_init(|| id) != &id {
            return Err(invalid(
                "same graph was assigned a different canonical resource",
            ));
        }
        if fresh {
            preparation
                .require_fresh_resource(id)
                .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
        }
        Ok((id, invocation))
    }
    pub(in crate::adapters::wireguard::lifecycle) fn into_retained_component(
        self,
        preparation: &mut crate::component::ResourcePreparation<'_>,
        candidate: &super::super::pending_map::PendingEntries,
    ) -> Result<zero_core::component::ComponentId, EngineError> {
        self.check_staged()?;
        let tag = self.tag.clone();
        let ready = self.pending.ready().is_some();
        let binding = self.binding.clone();
        let fresh = self.reuse.is_none();
        let (options, invocation) = self.into_parts()?;
        let id = preparation
            .share(
                SHARED_NAMESPACE,
                Arc::new(LinkedStackFactory),
                options.clone(),
            )
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
        if options.resource.get_or_init(|| id) != &id {
            return Err(invalid(
                "same graph was assigned a different canonical resource",
            ));
        }
        if fresh {
            preparation
                .require_fresh_resource(id)
                .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
        }
        candidate
            .invocations
            .try_lock()
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))?
            .entry(tag)
            .or_insert_with(|| {
                if ready {
                    retained::RetainedInvocation::Installed { binding }
                } else {
                    retained::RetainedInvocation::Prepared(Box::new(invocation))
                }
            });
        Ok(id)
    }
    pub(super) fn into_parts(
        mut self,
    ) -> Result<(Arc<LinkedStackOptions>, LinkedInvocation), EngineError> {
        let options = match &self.reuse {
            Some(owner) => owner.upgrade().ok_or_else(|| {
                invalid("same canonical linked owner was released during cold preparation")
            })?,
            None => {
                let task_limit = self.task_limit()?;
                let stacks = std::mem::take(&mut self.stacks);
                let options = Arc::new(LinkedStackOptions::new(&self, stacks, task_limit)?);
                self.pending.bind_graph(&options)?;
                options
            }
        };
        let peers = options.peer_count();
        Ok((
            options,
            LinkedInvocation {
                recipe: self,
                peers,
            },
        ))
    }
}
