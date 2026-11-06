use crate::transport::{ShadowsocksManagedDatagramFlowResume, ShadowsocksUdpSocketFlow};
use std::{
    collections::HashMap,
    io,
    num::NonZeroUsize,
    sync::{Arc, Mutex, Weak},
};
use tokio::time::Instant;
use zero_core::{
    component::{ComponentId, ComponentInstanceId},
    request::Destination,
};
use zero_platform_tokio::{
    contract::{
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
            ResourceDeclaration, ResourceOwner,
        },
        lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle},
        network::{CarrierContext, UpstreamBinding, UpstreamTransport},
        scope::{TaskJoinWitness, TaskRegistrar},
        CancellationToken, ChannelError, CloseReason, Impact, Reason, Stage,
    },
    controlled_carrier::{CarrierAdoption, CarrierGrants, CarrierOwner},
};
mod registry;
use registry::Ownership;
pub(crate) use registry::{PoolRegistry, PoolRoot, PoolSelection};
mod options;
pub use options::UdpPoolOptions;
pub struct UdpPoolFactory;
struct Owner {
    _root: Arc<PoolRoot>,
    state: Arc<Managed>,
    carrier: CarrierOwner,
    resources: Arc<dyn zero_platform_tokio::contract::component::ResourceAccess>,
    attempted: bool,
}
impl ComponentFactory for UdpPoolFactory {
    type Options = UdpPoolOptions;
    fn describe(&self, options: &UdpPoolOptions) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Endpoint("shadowsocks.udp.cache".into()),
            roles: vec![],
            dependencies: options.process.map(|(id, _)| id).into_iter().collect(),
            static_identity: options.identity.clone(),
            resources: vec![ResourceDeclaration::ControlledCarrierPool {
                endpoint: options.endpoint.clone(),
                dial: options.binding.dial.clone(),
                datagram: true,
                max_sockets: NonZeroUsize::new(1).unwrap(),
            }],
            task_limit: NonZeroUsize::new(1),
        }
    }
    fn build(
        &self,
        options: Arc<UdpPoolOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let upstream = context
            .background_carrier
            .clone()
            .filter(|upstream| upstream.binding() == &options.binding)
            .ok_or_else(|| error(context.instance(), Stage::Prepare, Reason::InvalidConfig))?;
        if context.dependencies.len() != usize::from(options.process.is_some())
            || options
                .process
                .is_some_and(|(id, _)| context.dependency(id).is_none())
        {
            return Err(error(context.instance(), Stage::Prepare, Reason::StalePlan));
        }
        let root = options
            .root
            .upgrade()
            .ok_or_else(|| error(context.instance(), Stage::Prepare, Reason::Revoked))?;
        let cancel = context.cancel.child_token();
        let carrier = CarrierOwner::new(context.instance(), NonZeroUsize::new(1).unwrap(), &cancel);
        let state = Arc::new(Managed {
            instance: context.instance(),
            tasks: context.tasks.clone(),
            cancel,
            grants: carrier.grants(),
            upstream,
            options: options.clone(),
            process: options
                .process
                .map(|(id, startup)| (context.dependency(id).unwrap(), startup)),
            current: Mutex::new(None),
            opening: tokio::sync::Mutex::new(()),
            pending_budget: options.pending.actual().map_err(|source| {
                error(context.instance(), Stage::Prepare, Reason::Revoked).with_source(source)
            })?,
            logical: Mutex::new(Vec::new()),
            changed: Default::default(),
            ready: Default::default(),
        });
        root.claim(state.clone())?;
        Ok(ComponentInstance {
            roles: ComponentRoles::default(),
            lifecycle: erase_lifecycle(Owner {
                _root: root,
                state,
                carrier,
                resources: context.resources,
                attempted: false,
            }),
        })
    }
}
pub(crate) struct Managed {
    instance: ComponentInstanceId,
    tasks: TaskRegistrar,
    cancel: CancellationToken,
    grants: CarrierGrants,
    upstream: UpstreamTransport,
    options: Arc<UdpPoolOptions>,
    process: Option<(ComponentInstanceId, std::time::Duration)>,
    current: Mutex<Option<Arc<Record>>>,
    opening: tokio::sync::Mutex<()>,
    logical: Mutex<Vec<Arc<super::logical::Logical>>>,
    changed: tokio::sync::Notify,
    ready: std::sync::atomic::AtomicBool,
    pending_budget: Arc<crate::transport::udp_socket::responses::budget::PendingBudget>,
}
struct Record {
    adoption: CarrierAdoption,
    flow: Mutex<Option<ShadowsocksUdpSocketFlow>>,
    driver: Mutex<Option<TaskJoinWitness>>,
}
impl Record {
    fn stop(&self) {
        self.adoption.request_retirement();
        let flow = self.flow.lock().unwrap().clone();
        if let Some(flow) = flow {
            flow.retire_native();
        }
    }
    fn confirmed(&self) -> bool {
        self.adoption
            .closed()
            .is_some_and(|closed| closed.matches(&self.adoption))
            && self
                .adoption
                .users_closed()
                .is_some_and(|closed| closed.matches(&self.adoption))
            && self.driver.lock().unwrap().as_ref().is_none_or(|driver| {
                driver.try_joined().is_some_and(|joined| {
                    driver.matches_joined(&joined) && joined.registration_released()
                })
            })
    }
}
pub(super) fn error(instance: ComponentInstanceId, stage: Stage, reason: Reason) -> ChannelError {
    let mut error = ChannelError::new(stage, reason, Impact::Component);
    error.component = Some(instance.component);
    error
}
mod lifecycle;
mod open;

mod users;
