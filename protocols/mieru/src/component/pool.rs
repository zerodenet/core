use crate::client::{pool::managed::ManagedState, ClientPool};
use std::{num::NonZeroUsize, sync::Arc};
use tokio::time::Instant;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceDeclaration, ResourceOwner,
    },
    lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle},
    scope::TaskRegistrar,
    CancellationToken, ChannelError, CloseReason, Impact, Reason, Stage,
};

#[cfg(test)]
use std::sync::Weak;
mod options;
use crate::client::pool::profile::CarrierPlan;
pub use options::{PoolCarrierSpec, PoolOptions, PoolProfile};

pub struct PoolFactory;
impl ComponentFactory for PoolFactory {
    type Options = PoolOptions;
    fn describe(&self, options: &PoolOptions) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Endpoint("mieru.client_pool".into()),
            roles: vec![],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            resources: if options.carriers.is_empty() {
                vec![ResourceDeclaration::BackgroundCarrier]
            } else {
                options.carriers.iter().map(|carrier| if options.dynamic { ResourceDeclaration::ControlledCarrierPool {endpoint:carrier.endpoint.clone(),dial:carrier.binding.dial.clone(),datagram:carrier.udp,max_sockets:options.limit} } else if carrier.udp { ResourceDeclaration::ControlledDatagramCarrier {
                    endpoint: carrier.endpoint.clone(), binding: carrier.binding.clone(), max_sockets: options.limit,
                } } else { ResourceDeclaration::ControlledOutboundStream {
                    endpoint: carrier.endpoint.clone(), binding: carrier.binding.clone(),
                    demand: zero_platform_tokio::contract::component::ControlledStreamDemandClass::BackgroundControl,
                    max_sockets: options.limit,
                } }).collect()
            },
            task_limit: Some(options.limit),
        }
    }
    fn build(
        &self,
        options: Arc<PoolOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let upstreams = context.background_carrier.clone().into_iter().collect();
        self.build_with_carriers(options, context, upstreams)
    }
}
impl PoolFactory {
    /// Cold assembly injects the actual provider for each described carrier in
    /// declaration order. No sockets, workers or second pool are created here.
    pub fn build_with_carriers(
        &self,
        options: Arc<PoolOptions>,
        context: BuildContext,
        upstreams: Vec<zero_platform_tokio::contract::network::UpstreamTransport>,
    ) -> Result<ComponentInstance, BuildError> {
        if !context.dependencies.is_empty()
            || context.roles != zero_core::component::ComponentRoleIds::default()
            || upstreams.len() != options.carriers.len().max(1)
            || options
                .carriers
                .iter()
                .zip(&upstreams)
                .any(|(plan, upstream)| &plan.binding != upstream.binding())
        {
            return Err(error(
                context.instance(),
                Stage::Prepare,
                Reason::InvalidConfig,
            ));
        }
        let pool = options
            .pool
            .upgrade()
            .ok_or_else(|| error(context.instance(), Stage::Prepare, Reason::Revoked))?;
        let cancel = context.cancel.child_token();
        let carriers = upstreams
            .into_iter()
            .map(|upstream| OwnedCarrier {
                upstream,
                owner: options.controlled.then(|| {
                    zero_platform_tokio::controlled_carrier::CarrierOwner::new(
                        context.instance(),
                        options.limit,
                        &cancel,
                    )
                }),
            })
            .collect();
        Ok(ComponentInstance {
            roles: ComponentRoles::default(),
            lifecycle: erase_lifecycle(Owner {
                instance: context.instance(),
                pool,
                limit: options.limit,
                tasks: context.tasks,
                cancel,
                carriers,
                identity: options.identity.clone(),
                profiles: options.profiles.clone(),
                plans: options.carriers.clone(),
                state: None,
                attempted: false,
                resources: context.resources,
                declared: !options.carriers.is_empty(),
                pending_close: CloseReport::default(),
                dynamic: options.dynamic,
            }),
        })
    }
}
struct OwnedCarrier {
    upstream: zero_platform_tokio::contract::network::UpstreamTransport,
    owner: Option<zero_platform_tokio::controlled_carrier::CarrierOwner>,
}
struct Owner {
    instance: ComponentInstanceId,
    pool: Arc<ClientPool>,
    limit: NonZeroUsize,
    tasks: TaskRegistrar,
    cancel: CancellationToken,
    carriers: Vec<OwnedCarrier>,
    identity: Arc<[u8]>,
    profiles: Option<Arc<[PoolProfile]>>,
    plans: Arc<[CarrierPlan]>,
    state: Option<Arc<ManagedState>>,
    attempted: bool,
    resources: Arc<dyn zero_platform_tokio::contract::component::ResourceAccess>,
    declared: bool,
    pending_close: CloseReport,
    dynamic: bool,
}
fn error(instance: ComponentInstanceId, stage: Stage, reason: Reason) -> ChannelError {
    let mut error = ChannelError::new(stage, reason, Impact::Component);
    error.component = Some(instance.component);
    error
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.revoke(CloseReason::Shutdown);
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        if self.state.as_ref().is_some_and(|state| state.ready()) {
            return Ok(());
        }
        if self.attempted || self.cancel.is_cancelled() {
            return Err(error(self.instance, Stage::Prepare, Reason::Revoked));
        }
        if Instant::now() >= deadline {
            return Err(error(self.instance, Stage::Prepare, Reason::Timeout));
        }
        self.attempted = true;
        if self.declared {
            for (ordinal, carrier) in self.carriers.iter().enumerate() {
                self.resources.register_controlled_carrier(
                    ordinal,
                    carrier
                        .owner
                        .as_ref()
                        .expect("controlled declaration")
                        .registration(),
                    &self.tasks,
                )?;
            }
        }
        self.state = Some(
            self.pool
                .claim_managed(
                    self.instance,
                    self.limit,
                    self.tasks.clone(),
                    self.cancel.clone(),
                    crate::client::pool::managed::ManagedBindings {
                        carriers: self
                            .carriers
                            .iter()
                            .map(|carrier| crate::client::pool::managed::ManagedCarrier {
                                upstream: carrier.upstream.clone(),
                                grants: carrier.owner.as_ref().map(|owner| owner.grants()),
                            })
                            .collect(),
                        identity: self.identity.clone(),
                        plans: self.plans.clone(),
                        profiles: self.profiles.clone(),
                        dynamic: self.dynamic,
                    },
                )
                .map_err(|source| {
                    error(self.instance, Stage::Prepare, Reason::StalePlan).with_source(source)
                })?,
        );
        for carrier in &mut self.carriers {
            if let Some(owner) = &mut carrier.owner {
                owner.start(deadline).await?;
            }
        }
        self.state.as_ref().unwrap().activate()?;
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        if let Some(state) = &self.state {
            state.stop();
        }
        self.cancel.cancel();
        for carrier in &self.carriers {
            if let Some(owner) = &carrier.owner {
                owner.revoke();
            }
        }
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.close(deadline).await
    }
}
mod close;

#[cfg(test)]
#[path = "../../tests/component/pool.rs"]
mod tests;
