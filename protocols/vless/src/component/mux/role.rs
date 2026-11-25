//! VLESS MUX stream/datagram role backed by [`PooledRoleFactory`] and [`LifecycleChain`].

use super::Options;
use std::{
    future::Future,
    sync::{Arc, Weak},
};
use tokio::time::Instant;
use zero_core::{
    component::{ComponentId, ComponentInstanceId},
    request::{DatagramTargets, Destination},
};
use zero_platform_tokio::{
    contract::{
        admission::SessionAdmission,
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
            ResourceOwner, RoleKind,
        },
        context::OpenContext,
        datagram::DatagramRequest,
        lifecycle::{BuildError, CloseReport, ResourceLifecycle},
        stream::{
            erase_stream, BoxStream, ConnectRequest, Connected, DynStreamOutbound, StreamOutbound,
        },
        ChannelError, CloseReason, DeliveryState, Impact, Reason, Stage,
    },
    controlled_carrier::loan::StreamLoan,
};
use zero_traits::DialPolicy;
use zero_transport::{
    pooled_driver::{datagram::PooledDatagram, PooledStream},
    runner::{
        HandshakeError, LifecycleChain, PooledBoundLeaf, PooledDatagramProtocol, PooledRoleFactory,
        PooledRoleOptions, PooledRoleProtocol,
    },
    RuntimeError,
};

pub struct Factory;

impl PooledRoleOptions for Options {
    fn pool_owner(&self) -> ComponentId {
        self.pool_owner
    }

    fn dial(&self) -> &DialPolicy {
        &self.prepared.pool.binding().dial
    }

    fn static_identity(&self) -> Arc<[u8]> {
        self.identity.clone()
    }

    fn supports_datagram(&self) -> bool {
        self.datagram
    }
}

impl PooledBoundLeaf for crate::transport::VlessOutboundLeaf {
    fn pool_instance(&self) -> Option<ComponentInstanceId> {
        self.native_mux_instance().ok()
    }

    fn open_stream<'a>(
        &'a self,
        destination: Destination,
        context: &'a OpenContext,
    ) -> impl Future<Output = Result<PooledStream, RuntimeError>> + Send + 'a {
        self.open_native_mux(destination, context)
    }
}

#[derive(Clone)]
struct VlessMuxRoleProtocol {
    masks: Option<zero_transport::finalmask::Profile>,
}

impl PooledRoleProtocol for VlessMuxRoleProtocol {
    type Options = Options;
    type BoundLeaf = crate::transport::VlessOutboundLeaf;

    fn bind_leaf(
        &self,
        options: &Self::Options,
        pool: ComponentInstanceId,
    ) -> Result<Self::BoundLeaf, RuntimeError> {
        let mut bound = options.prepared.leaf.bind_native_mux(pool)?;
        if let Some(masks) = &self.masks {
            bound = bound.with_native_tcp_masks(masks.clone());
        }
        Ok(bound)
    }
}

#[derive(Clone, Copy)]
struct VlessMuxDatagramProtocol;

impl PooledDatagramProtocol<VlessMuxRoleProtocol> for VlessMuxDatagramProtocol {
    type PacketIo = crate::mux_pool::native::DatagramIo;

    fn validate_leaf_request(
        &self,
        leaf: &crate::transport::VlessOutboundLeaf,
        request: &DatagramRequest,
    ) -> Result<(), HandshakeError> {
        if leaf.native_udp443_blocked()
            && matches!(&request.targets, DatagramTargets::Fixed(target) if target.port == 443)
        {
            return Err(HandshakeError::unsupported());
        }
        Ok(())
    }

    fn open_datagram<'a>(
        &'a self,
        leaf: &'a crate::transport::VlessOutboundLeaf,
        request: &'a DatagramRequest,
        context: &'a OpenContext,
    ) -> impl Future<Output = Result<PooledDatagram<Self::PacketIo>, RuntimeError>> + Send + 'a
    {
        leaf.open_native_mux_udp(request.association, context)
    }
}

struct ControlRetainer(Option<Arc<dyn DynStreamOutbound>>);

impl ResourceLifecycle for ControlRetainer {
    async fn start(&mut self, _: Instant) -> Result<(), BuildError> {
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, BuildError> {
        self.0.take();
        Ok(CloseReport::default())
    }
}

struct ReverseControlStream {
    mux: Arc<dyn DynStreamOutbound>,
    control: Weak<dyn DynStreamOutbound>,
    admission: Arc<SessionAdmission>,
}

impl ReverseControlStream {
    fn resolve_control(
        &self,
        context: &OpenContext,
    ) -> Result<Arc<dyn DynStreamOutbound>, ChannelError> {
        let session = self.admission.check(context)?;
        self.control.upgrade().ok_or_else(|| {
            self.admission.error(
                session,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            )
        })
    }
}

impl StreamOutbound for ReverseControlStream {
    type Stream = BoxStream;

    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        if crate::component::request::dialed_ingress(&context) {
            return self
                .resolve_control(&context)?
                .connect(request, context)
                .await;
        }
        self.mux.connect(request, context).await
    }

    async fn connect_carrier(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<StreamLoan>, ChannelError> {
        if crate::component::request::dialed_ingress(&context) {
            return self
                .resolve_control(&context)?
                .connect_carrier(request, context)
                .await;
        }
        self.mux.connect_carrier(request, context).await
    }
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
            let mut error =
                ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component);
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
        let expected = self.describe(&options).dependencies;
        let Some(pool) = context
            .dependency(options.pool_owner)
            .filter(|pool| pool.component != context.component)
            .filter(|_| {
                context.dependencies.len() == expected.len()
                    && expected.iter().all(|id| context.dependency(*id).is_some())
            })
        else {
            let mut error = ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Component);
            error.component = Some(context.component);
            error.delivery = DeliveryState::NotSubmitted;
            return Err(error);
        };
        let mux_context = BuildContext {
            component: context.component,
            generation: context.generation,
            dependencies: Arc::from([pool]),
            roles: context.roles,
            resources: context.resources.clone(),
            tasks: context.tasks.clone(),
            cancel: context.cancel.clone(),
            background_carrier: context.background_carrier.clone(),
        };
        let (mut mux_instance, admission) = PooledRoleFactory::with_datagram(
            VlessMuxRoleProtocol {
                masks: masks.clone(),
            },
            VlessMuxDatagramProtocol,
        )
        .build_with_admission(options.clone(), mux_context)?;

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
                        super::super::Factory.build_hosted(options.clone(), actual, resolver, masks)
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

        let raw_mux_stream = mux_instance
            .roles
            .stream
            .take()
            .expect("pooled stream role");
        let mux_stream = if options.stream {
            Some(match &control_stream {
                Some(control) => erase_stream(ReverseControlStream {
                    mux: raw_mux_stream,
                    control: Arc::downgrade(control),
                    admission,
                }),
                None => raw_mux_stream,
            })
        } else {
            None
        };

        let lifecycle = LifecycleChain::new()
            .push_opt(ordinary.map(|instance| instance.lifecycle))
            .push_typed_opt(control_stream.map(|cs| ControlRetainer(Some(cs))))
            .push(mux_instance.lifecycle)
            .into_boxed();

        Ok(ComponentInstance {
            roles: ComponentRoles {
                stream: ordinary_stream.or(mux_stream),
                datagram: ordinary_role.or(mux_instance.roles.datagram),
                ..Default::default()
            },
            lifecycle,
        })
    }
}
