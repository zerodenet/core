use super::Options;
use std::sync::{Arc, Mutex, Weak};
use tokio::time::Instant;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{
    admission::{SessionAdmission, SessionOwner},
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner, RoleKind,
    },
    context::{with_deadline, ConnectFact, OpenContext},
    lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle},
    stream::{
        erase_stream, BoxStream, ConnectRequest, Connected, PeerConfirmation, StreamOutbound,
    },
    ChannelError, CloseReason, DeliveryState, Reason, Stage,
};
mod datagram;
mod lifecycle;
mod stream;
pub struct Factory;
struct Ready {
    leaf: Mutex<Option<crate::transport::VlessOutboundLeaf>>,
}
#[derive(Clone)]
struct Outbound {
    ready: Weak<Ready>,
    options: Weak<Options>,
    admission: Arc<SessionAdmission>,
    pool: ComponentInstanceId,
    control_stream: Option<Weak<dyn zero_platform_tokio::contract::stream::DynStreamOutbound>>,
}
struct Owner {
    ready: Arc<Ready>,
    options: Arc<Options>,
    admission: Arc<SessionAdmission>,
    inner: SessionOwner,
    pool: ComponentInstanceId,
    // The original owner retains the same ordinary callable role. Cached MUX
    // handles retain only a Weak and cannot pin its private resources.
    _control_stream: Option<Arc<dyn zero_platform_tokio::contract::stream::DynStreamOutbound>>,
    ordinary: Option<Box<dyn zero_platform_tokio::contract::lifecycle::DynResourceLifecycle>>,
    failures: zero_platform_tokio::contract::lifecycle::ResourceCleanupFailures,
    masks: Option<zero_transport::finalmask::Profile>,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        let ordinary = options.ordinary.as_ref().map(|plan| plan.describe());
        let mut dependencies = vec![options.pool_owner];
        if let Some(spec) = &ordinary {
            for dependency in &spec.dependencies {
                if !dependencies.contains(dependency) {
                    dependencies.push(*dependency);
                }
            }
        }
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: (options.stream || ordinary.is_some())
                .then_some(RoleKind::Stream)
                .into_iter()
                .chain(
                    (options.datagram || options.stream && ordinary.is_some())
                        .then_some(RoleKind::Datagram),
                )
                .collect(),
            dependencies,
            static_identity: options.identity.clone(),
            resources: ordinary
                .as_ref()
                .map(|spec| spec.resources.clone())
                .unwrap_or_default(),
            task_limit: ordinary.as_ref().and_then(|spec| spec.task_limit),
        }
    }

    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        self.build_roles(options, context, None, false)
    }
}
impl Factory {
    pub fn build_hosted(
        &self,
        options: Arc<Options>,
        context: BuildContext,
        resolver: Option<Arc<dyn zero_transport::tls::ech::EchConfigResolver>>,
    ) -> Result<ComponentInstance, BuildError> {
        self.build_roles(options, context, resolver, true)
    }
    fn build_roles(
        &self,
        options: Arc<Options>,
        context: BuildContext,
        resolver: Option<Arc<dyn zero_transport::tls::ech::EchConfigResolver>>,
        hosted: bool,
    ) -> Result<ComponentInstance, BuildError> {
        if options.prepared.leaf.preconnect_access().is_some()
            && options.prepared.pool.stream_dependency().is_none()
        {
            let mut error = ChannelError::new(
                Stage::Prepare,
                Reason::InvalidConfig,
                zero_platform_tokio::contract::Impact::Component,
            );
            error.component = Some(context.component);
            return Err(error);
        }
        let masks = options
            .prepared
            .material
            .as_ref()
            .map(|material| {
                super::super::masks::prepare(material, context.resources.as_ref(), hosted, None)
            })
            .transpose()?;
        let (inner, admission) = SessionOwner::new(context.component, &context.cancel);
        let ready = Arc::new(Ready {
            leaf: Mutex::new(None),
        });
        let pool = context
            .dependency(options.pool_owner)
            .filter(|pool| pool.component != context.component)
            .filter(|_| {
                let expected = self.describe(&options).dependencies;
                context.dependencies.len() == expected.len()
                    && expected.iter().all(|id| context.dependency(*id).is_some())
            })
            .ok_or_else(|| {
                admission.error(
                    None,
                    Stage::Prepare,
                    Reason::StalePlan,
                    DeliveryState::NotSubmitted,
                )
            })?;
        let mut ordinary = options
            .ordinary
            .as_ref()
            .map(|plan| {
                let spec = plan.describe();
                let actual = BuildContext {
                    component: context.component,
                    generation: context.generation,
                    dependencies: context
                        .dependencies
                        .iter()
                        .copied()
                        .filter(|id| spec.dependencies.contains(&id.component))
                        .collect::<Vec<_>>()
                        .into(),
                    roles: context.roles,
                    resources: context.resources.clone(),
                    tasks: context.tasks.clone(),
                    cancel: context.cancel.clone(),
                    background_carrier: context.background_carrier.clone(),
                };
                match plan {
                    super::options::OrdinaryPlan::Ordinary(options) if hosted => {
                        super::super::Factory.build_hosted(
                            options.clone(),
                            actual,
                            resolver,
                            masks.clone(),
                        )
                    }
                    super::options::OrdinaryPlan::Ordinary(options) => match resolver {
                        Some(resolver) => super::super::Factory.build_with_ech_resolver(
                            options.clone(),
                            actual,
                            resolver,
                        ),
                        None => super::super::Factory.build(options.clone(), actual),
                    },
                    super::options::OrdinaryPlan::Grpc(options) if hosted => {
                        super::super::grpc::StreamFactory.build_hosted(options.clone(), actual)
                    }
                    super::options::OrdinaryPlan::Grpc(options) => {
                        super::super::grpc::StreamFactory.build(options.clone(), actual)
                    }
                }
            })
            .transpose()?;
        let control_stream = ordinary
            .as_mut()
            .map(|instance| {
                instance.roles.stream.take().ok_or_else(|| {
                    admission.error(
                        None,
                        Stage::Prepare,
                        Reason::InvalidConfig,
                        DeliveryState::NotSubmitted,
                    )
                })
            })
            .transpose()?;
        let ordinary_stream = (!options.stream).then(|| control_stream.clone()).flatten();
        let ordinary_role = ordinary
            .as_mut()
            .filter(|_| options.stream && !options.datagram)
            .map(|instance| {
                instance.roles.datagram.take().ok_or_else(|| {
                    admission.error(
                        None,
                        Stage::Prepare,
                        Reason::InvalidConfig,
                        DeliveryState::NotSubmitted,
                    )
                })
            })
            .transpose()?;
        let outbound = Outbound {
            options: Arc::downgrade(&options),
            ready: Arc::downgrade(&ready),
            admission: admission.clone(),
            pool,
            control_stream: control_stream.as_ref().map(Arc::downgrade),
        };
        Ok(ComponentInstance {
            roles: ComponentRoles {
                stream: ordinary_stream
                    .or_else(|| options.stream.then(|| erase_stream(outbound.clone()))),
                datagram: ordinary_role.or_else(|| {
                    options
                        .datagram
                        .then(|| zero_platform_tokio::contract::datagram::erase_datagram(outbound))
                }),
                ..Default::default()
            },
            lifecycle: erase_lifecycle(Owner {
                options,
                ready,
                admission,
                inner,
                pool,
                _control_stream: control_stream,
                ordinary: ordinary.map(|instance| instance.lifecycle),
                failures: Default::default(),
                masks,
            }),
        })
    }
}
