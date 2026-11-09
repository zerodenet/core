use super::*;
pub(super) struct Authority {
    pub(super) declaration: ResourceDeclaration,
    pub(super) scope: TaskScopeIdentity,
    pub(super) host_cancel: Arc<CancellationToken>,
    pub(super) metadata: Option<crate::contract::component::PacketDriverMetadataView>,
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
        self.bind_declared_inner(declaration, tasks, host_cancel, None, None)
    }
    pub fn bind_declared_with_totals(
        &self,
        declaration: &ResourceDeclaration,
        tasks: &TaskRegistrar,
        host_cancel: &Arc<CancellationToken>,
        totals: &Arc<crate::controlled_carrier::CarrierOriginalTotals>,
    ) -> Result<(), ChannelError> {
        self.bind_declared_inner(declaration, tasks, host_cancel, Some(totals), None)
    }
    pub fn bind_declared_with_accounting(
        &self,
        declaration: &ResourceDeclaration,
        tasks: &TaskRegistrar,
        host_cancel: &Arc<CancellationToken>,
        totals: &Arc<crate::controlled_carrier::CarrierOriginalTotals>,
        metadata: &crate::contract::component::PacketDriverMetadataView,
    ) -> Result<(), ChannelError> {
        self.bind_declared_inner(
            declaration,
            tasks,
            host_cancel,
            Some(totals),
            Some(metadata),
        )
    }
    fn bind_declared_inner(
        &self,
        declaration: &ResourceDeclaration,
        tasks: &TaskRegistrar,
        host_cancel: &Arc<CancellationToken>,
        totals: Option<&Arc<crate::controlled_carrier::CarrierOriginalTotals>>,
        metadata: Option<&crate::contract::component::PacketDriverMetadataView>,
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
        if totals.is_some_and(|totals| {
            owner
                .totals
                .get()
                .is_some_and(|actual| !Arc::ptr_eq(actual, totals))
        }) {
            return Err(error(
                Reason::StalePlan,
                "metadata owner cannot change kernel total roots",
            ));
        }
        if let Some(bound) = &state.bound {
            if &bound.declaration != declaration
                || !bound.scope.matches(tasks)
                || !Arc::ptr_eq(&bound.host_cancel, host_cancel)
                || match (&bound.metadata, metadata) {
                    (Some(actual), Some(expected)) => !actual.same_root(expected),
                    (None, None) => false,
                    _ => true,
                }
            {
                return Err(error(Reason::StalePlan, "metadata binding changed"));
            }
        } else {
            state.bound = Some(Arc::new(Authority {
                declaration: declaration.clone(),
                scope,
                host_cancel: host_cancel.clone(),
                metadata: metadata.cloned(),
            }));
        }
        if let Some(totals) = totals {
            let _ = owner.totals.set(totals.clone());
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
