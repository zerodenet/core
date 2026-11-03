use super::*;
pub(super) struct Authority {
    pub(super) declaration: ResourceDeclaration,
    pub(super) scope: TaskScopeIdentity,
    pub(super) host_cancel: Arc<CancellationToken>,
}
impl Authority {
    pub(super) fn check(
        &self,
        owner: &Shared,
        query: &TaskJoinWitness,
    ) -> Result<(), ChannelError> {
        if self.host_cancel.is_cancelled()
            || owner.cancel.is_cancelled()
            || !self.scope.resource_open()
            || !self.scope.registration_open()
            || query.is_joined()
            || !query.scope_identity().same_scope(&self.scope)
        {
            return Err(error(
                Reason::Revoked,
                "metadata acquisition requires its actual active query scope",
            ));
        }
        Ok(())
    }
}
impl MetadataPoolRegistration {
    /// Host registration binds the sole real owner before native allocation.
    pub fn bind_declared(
        &self,
        declaration: &ResourceDeclaration,
        tasks: &TaskRegistrar,
        host_cancel: &Arc<CancellationToken>,
    ) -> Result<(), ChannelError> {
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| error(Reason::Revoked, "metadata owner dropped"))?;
        let (dial, limits, max_origins) = match declaration {
            ResourceDeclaration::ControlledMetadataDatagram {
                endpoint,
                dial,
                limits,
                max_origins,
            } if endpoint.port() != 0 => (dial, limits, max_origins),
            ResourceDeclaration::ControlledMetadataCarrier {
                endpoint,
                dial,
                ports,
                limits,
                max_origins,
            } if endpoint.port != 0 && ports.len() <= u16::MAX as usize && !ports.contains(&0) => {
                (dial, limits, max_origins)
            }
            _ => {
                return Err(error(
                    Reason::InvalidConfig,
                    "invalid ancillary pool declaration",
                ))
            }
        };
        if *limits != owner.limits || *max_origins != owner.limit {
            return Err(error(
                Reason::InvalidConfig,
                "metadata declaration differs from actual owner",
            ));
        }
        dial.validate().map_err(|source| {
            error(Reason::InvalidConfig, "invalid metadata dial policy").with_source(source)
        })?;
        let scope = tasks.scope_identity();
        if !scope.resource_open() || host_cancel.is_cancelled() || owner.cancel.is_cancelled() {
            return Err(error(Reason::Revoked, "metadata host scope is closed"));
        }
        let mut state = owner.state.lock().expect("metadata pool poisoned");
        if state.live || state.stopped || state.next != 0 {
            return Err(error(
                Reason::Revoked,
                "metadata registration already started",
            ));
        }
        if let Some(bound) = &state.bound {
            if &bound.declaration != declaration
                || !bound.scope.matches(tasks)
                || !Arc::ptr_eq(&bound.host_cancel, host_cancel)
            {
                return Err(error(Reason::StalePlan, "metadata binding changed"));
            }
        } else {
            state.bound = Some(Arc::new(Authority {
                declaration: declaration.clone(),
                scope,
                host_cancel: host_cancel.clone(),
            }));
        }
        Ok(())
    }
    pub fn matches_binding(
        &self,
        declaration: &ResourceDeclaration,
        tasks: &TaskRegistrar,
        host_cancel: &Arc<CancellationToken>,
    ) -> bool {
        self.owner
            .upgrade()
            .and_then(|owner| owner.state.lock().ok()?.bound.clone())
            .is_some_and(|bound| {
                &bound.declaration == declaration
                    && bound.scope.matches(tasks)
                    && Arc::ptr_eq(&bound.host_cancel, host_cancel)
            })
    }
}
