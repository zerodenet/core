use crate::component::{
    catalog::ComponentPlan,
    instance::{retained, SharedError},
};
use std::sync::Arc;
use zero_config::RuntimeConfig;
impl super::ProtocolInventory {
    pub(crate) fn begin_packet_source_retirement<'a>(
        &'a self,
        candidate: &Arc<ComponentPlan>,
        pause: Arc<crate::component::catalog::ReplacementUsersPause>,
    ) -> Result<
        Option<crate::component::catalog::PacketSourceRetirement<'a>>,
        zero_platform_tokio::contract::ChannelError,
    > {
        self.host
            .components()
            .begin_packet_source_retirement(candidate, pause)
    }

    pub(crate) fn try_pause_replacement_users(
        &self,
        candidate: &Arc<ComponentPlan>,
    ) -> Result<
        crate::component::catalog::ReplacementUsersPause,
        zero_platform_tokio::contract::ChannelError,
    > {
        self.host
            .components()
            .try_pause_replacement_users(candidate)
    }
    /// Inline across the whole runtime and passive nested operations. This
    /// elects a join consumer; it never spawns another owner/task registry.
    pub(crate) async fn drive_component_supervision<T>(
        &self,
        work: impl std::future::Future<Output = T>,
    ) -> T {
        self.host.components().drive_supervised(work).await
    }
    pub(crate) async fn component_failure(&self) -> zero_engine::EngineError {
        zero_engine::EngineError::Io(std::io::Error::other(
            self.host.components().next_failure().await,
        ))
    }
    pub(crate) async fn scan_components(
        &self,
        config: &Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
    ) -> Result<(), zero_engine::EngineError> {
        self.host
            .components()
            .scan_candidate(config, deadline)
            .await
            .map_err(|error| zero_engine::EngineError::Io(std::io::Error::other(error)))
    }
    pub(crate) async fn maintain_service_components<T, F, Fut>(
        &self,
        config: Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
        work: F,
    ) -> Result<T, zero_platform_tokio::contract::ChannelError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        self.host
            .components()
            .maintain_service(config, deadline, work)
            .await
    }
    /// Read only: historical snapshots cannot prepare or publish a replacement.
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn source_dependency_instances(
        &self,
        config: &Arc<RuntimeConfig>,
        input: &zero_config::InboundConfig,
    ) -> Result<Vec<zero_core::component::ComponentInstanceId>, zero_engine::EngineError> {
        // The same catalog cache supplies this exact cold union; no independent
        // registry, old-plan generation or second source material decode.
        let plan = match self.host.components().prepared_candidate(config) {
            Some(prepared) => prepared,
            None => self.prepare_device_components(config.clone()),
        }
        .map_err(|error| std::io::Error::other(retained(error)))?;
        plan.source_dependency_instances(
            &input.tag,
            &crate::runtime::inbound_owners::source_input_identity(input)?,
        )
        .map_err(|error| std::io::Error::other(error).into())
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn source_requested_ring_dependencies(
        &self,
        config: &Arc<RuntimeConfig>,
        dependencies: &[zero_core::component::ComponentInstanceId],
    ) -> Result<bool, zero_engine::EngineError> {
        let plan = self
            .host
            .components()
            .prepared_candidate(config)
            .ok_or_else(|| {
                std::io::Error::other("actual source device class has no same cold plan")
            })?
            .map_err(|source| std::io::Error::other(retained(source)))?;
        plan.has_requested_ring_dependencies(dependencies)
            .map_err(|source| std::io::Error::other(source).into())
    }
    pub(crate) fn current_components(
        &self,
        config: &Arc<RuntimeConfig>,
    ) -> Option<Arc<ComponentPlan>> {
        self.host.components().current(config)
    }
    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn allocate_peer_association(
        &self,
    ) -> Result<zero_core::component::AssociationId, zero_platform_tokio::contract::ChannelError>
    {
        self.host.components().allocate_association()
    }
    pub(crate) fn allocate_resource_identity(
        &self,
    ) -> Result<zero_core::component::ComponentInstanceId, zero_engine::EngineError> {
        let component = self
            .host
            .components()
            .allocate_id()
            .map_err(|error| zero_engine::EngineError::Io(std::io::Error::other(error)))?;
        Ok(zero_core::component::ComponentInstanceId {
            component,
            generation: zero_core::component::ResourceGeneration(1),
        })
    }

    pub(crate) fn ingress_identity(
        &self,
        tag: String,
    ) -> Result<crate::component::request::IngressIdentity, SharedError> {
        use zero_core::component::{InboundId, ResourceGeneration, RoleId, RoleInstanceId};
        let component = self.host.components().allocate_id().map_err(Arc::new)?;
        Ok(crate::component::request::IngressIdentity {
            tag,
            role: RoleInstanceId {
                component,
                role: RoleId::Inbound(InboundId(component.0)),
                generation: ResourceGeneration(1),
            },
        })
    }
    pub(crate) fn stop_component_admission(&self) {
        self.host.components().stop_business();
    }
    /// Exact unstarted candidate cleanup, retaining the same current slot Arcs.
    /// Used only before this accepted phase has started or revoked resources.
    pub(crate) async fn discard_cold_components(
        &self,
        config: &Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
    ) -> Result<(), zero_engine::EngineError> {
        self.host
            .components()
            .discard_device_candidate(config, deadline)
            .await
            .map_err(|error| std::io::Error::other(error).into())
    }
    pub(crate) async fn start_components(
        &self,
        config: Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
    ) -> Result<(), zero_engine::EngineError> {
        #[cfg(feature = "raw-ip-runtime")]
        self.finish_previous_device_publication(deadline).await?;
        self.host
            .components()
            .start(config, deadline)
            .await
            .map_err(|error| zero_engine::EngineError::Io(std::io::Error::other(error)))
    }
    pub(crate) fn commit_components<T>(
        &self,
        config: &Arc<RuntimeConfig>,
        commit: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, String> {
        #[cfg(feature = "raw-ip-runtime")]
        {
            self.commit_device_publication(config, commit)
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        {
            self.host.components().commit(config, commit)
        }
    }
    pub(crate) async fn shutdown_components(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), zero_engine::EngineError> {
        self.host.cancel_probes();
        self.drive_component_supervision(self.host.drain_probes(deadline))
            .await
            .map_err(|source| zero_engine::EngineError::Io(std::io::Error::other(source)))?;
        #[cfg(feature = "raw-ip-runtime")]
        let publication = self.stop_device_publication(None, deadline).await?;
        let closed = self
            .host
            .components()
            .shutdown(deadline)
            .await
            .map_err(|error| zero_engine::EngineError::Io(std::io::Error::other(error)));
        closed?;
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(publication) = publication {
            self.clear_device_publication(&publication);
        }
        Ok(())
    }
    pub(crate) fn prepare_components(
        &self,
        config: Arc<RuntimeConfig>,
    ) -> Result<Arc<ComponentPlan>, SharedError> {
        self.host.components().prepare(config)
    }
    pub(crate) fn prepared_components(
        &self,
        config: &Arc<RuntimeConfig>,
    ) -> Option<Result<Arc<ComponentPlan>, SharedError>> {
        self.host.components().prepared_candidate(config)
    }
    /// Accepted transactions alone extend the cold union with device recipes.
    /// Ordinary TcpRuntimeServices readers continue to reuse prepare_components.
    pub(crate) fn prepare_device_components(
        &self,
        config: Arc<RuntimeConfig>,
    ) -> Result<Arc<ComponentPlan>, SharedError> {
        #[cfg(feature = "raw-ip-runtime")]
        {
            if !self.may_prepare_device_publication() {
                return Err(Arc::new(
                    zero_platform_tokio::contract::ChannelError::new(
                        zero_platform_tokio::contract::Stage::Prepare,
                        zero_platform_tokio::contract::Reason::StalePlan,
                        zero_platform_tokio::contract::Impact::Component,
                    )
                    .with_source(std::io::Error::other(
                        "previous actual device publication remains owned",
                    )),
                ));
            }
            self.host
                .components()
                .prepare_with_device_resources(config.clone(), |resources| {
                    let deferred = self.devices.prepare_component_resources(
                        &config,
                        resources,
                        &self.registry,
                    )?;
                    let sources = self
                        .devices
                        .source_component_dependencies(&config, resources)?;
                    resources.set_source_dependencies(sources)?;
                    Ok(deferred)
                })
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        {
            self.prepare_components(config)
        }
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn prepare_restarting_device_components(
        &self,
        config: Arc<RuntimeConfig>,
        recovering: bool,
    ) -> Result<Arc<ComponentPlan>, SharedError> {
        self.host.components().prepare_restarting_device_resources(
            config.clone(),
            recovering,
            |resources| {
                let deferred =
                    self.devices
                        .prepare_component_resources(&config, resources, &self.registry)?;
                let sources = self
                    .devices
                    .source_component_dependencies(&config, resources)?;
                resources.set_source_dependencies(sources)?;
                if recovering {
                    let aliases = self.devices.device_alias_overrides(&config, resources)?;
                    resources.set_device_alias_overrides(aliases)?;
                }
                Ok(deferred)
            },
        )
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) async fn prepare_resource_start<'a>(
        &'a self,
        config: Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
    ) -> Result<
        zero_runtime::component::catalog::PreparedComponentStart<'a>,
        zero_engine::EngineError,
    > {
        self.finish_previous_device_publication(deadline).await?;
        self.host
            .components()
            .prepare_resource_start(config, deadline)
            .await
            .map_err(|source| std::io::Error::other(source).into())
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) async fn refresh_prepared_device_components(
        &self,
        prepared: &mut zero_runtime::component::catalog::PreparedComponentStart<'_>,
        deadline: tokio::time::Instant,
    ) -> Result<Arc<ComponentPlan>, zero_engine::EngineError> {
        let config = prepared.config().clone();
        // These are the first pass's actual never-built pending recipes, not
        // the committed device publication. Release only their real wrappers.
        self.devices.release_component_resources(&config)?;
        prepared
            .refresh_device_resources(deadline, |resources| {
                let deferred =
                    self.devices
                        .prepare_component_resources(&config, resources, &self.registry)?;
                let sources = self
                    .devices
                    .source_component_dependencies(&config, resources)?;
                resources.set_source_dependencies(sources)?;
                Ok(deferred)
            })
            .await
            .map_err(|source| std::io::Error::other(source).into())
    }
    pub(crate) fn prepare_recovered_device_components(
        &self,
        config: Arc<RuntimeConfig>,
    ) -> Result<Arc<ComponentPlan>, SharedError> {
        #[cfg(feature = "raw-ip-runtime")]
        {
            self.host
                .components()
                .prepare_recovered_device_resources(config.clone(), |resources| {
                    let deferred = self.devices.prepare_component_resources(
                        &config,
                        resources,
                        &self.registry,
                    )?;
                    let sources = self
                        .devices
                        .source_component_dependencies(&config, resources)?;
                    resources.set_source_dependencies(sources)?;
                    let aliases = self.devices.device_alias_overrides(&config, resources)?;
                    resources.set_device_alias_overrides(aliases)?;
                    Ok(deferred)
                })
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        {
            self.prepare_components(config)
        }
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) async fn start_device_resources(
        &self,
        config: &Arc<RuntimeConfig>,
        services: &crate::protocol_registry::TcpRuntimeServices,
        deadline: tokio::time::Instant,
        cancel: zero_platform_tokio::contract::CancellationToken,
    ) -> Result<(), zero_engine::EngineError> {
        if !std::ptr::eq(services.snapshot().config().as_ref(), config.as_ref()) {
            return Err(zero_engine::EngineError::InvalidPlan {
                message: "device resolution source differs from the actual candidate snapshot"
                    .into(),
            });
        }
        let upstream = services.upstream();
        self.devices
            .resolve_component_resources(config, &upstream, services.engine(), deadline, cancel)
            .await?;
        self.host
            .components()
            .start_device_components(config.clone(), deadline)
            .await
            .map_err(|error| zero_engine::EngineError::Io(std::io::Error::other(error)))
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) async fn discard_device_components(
        &self,
        config: &Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
    ) -> Result<(), zero_engine::EngineError> {
        let publication = self.stop_device_publication(Some(config), deadline).await?;
        self.host
            .components()
            .discard_device_candidate(config, deadline)
            .await
            .map_err(|error| zero_engine::EngineError::Io(std::io::Error::other(error)))?;
        self.devices.cleanup_discarded_resources(config).await?;
        if let Some(publication) = publication {
            self.clear_device_publication(&publication);
        }
        Ok(())
    }
    pub(crate) fn validate_runtime_candidate(
        &self,
        config: &RuntimeConfig,
    ) -> Result<(), zero_engine::EngineError> {
        self.host
            .validate_probe_candidate(config)
            .map_err(|error| zero_engine::EngineError::Io(std::io::Error::other(error)))?;
        self.registry.validate_runtime_candidate(config)
    }
    pub(crate) fn validate_components(
        &self,
        config: Arc<RuntimeConfig>,
    ) -> Result<(), zero_engine::EngineError> {
        self.validate_runtime_candidate(&config)?;
        // Initial accepted assembly validates the same cold device union as
        // startup. Ordinary service readers never create a second pass.
        self.prepare_device_components(config)
            .map(|_| ())
            .map_err(|error| zero_engine::EngineError::Io(std::io::Error::other(retained(error))))
    }
}
